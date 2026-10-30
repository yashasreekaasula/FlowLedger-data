"""Real PySpark transformations for batch and Structured Streaming."""
import json,sqlite3,hashlib,shutil,time,os
from pathlib import Path
from pyspark.sql import SparkSession,functions as F,types as T
from .warehouse import load_reference,commit,connect,log_failure

SCHEMA=T.StructType([
 T.StructField('event_id',T.StringType()),T.StructField('order_id',T.StringType()),
 T.StructField('event_time',T.StringType()),T.StructField('payment',T.StructType([
 T.StructField('amount_cents',T.LongType()),T.StructField('currency',T.StringType()),T.StructField('status',T.StringType())])),
 T.StructField('_corrupt_record',T.StringType())])
ORDER_SCHEMA='order_id string, merchant_id string, order_date string, amount_cents long, currency string'

def session():
    os.environ.setdefault('SPARK_LOCAL_IP','127.0.0.1')
    s=(SparkSession.builder.master(os.getenv('SPARK_MASTER','local[2]')).appName('FlowLedger')
      .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','4')
      .config('spark.ui.enabled','false').getOrCreate())
    s.sparkContext.setLogLevel('WARN');return s

def archive(path,root):
    path=Path(path); key=hashlib.sha256(path.read_bytes()).hexdigest()
    dest=Path(root)/'bronze'/f'{key}{path.suffix}'
    dest.parent.mkdir(parents=True,exist_ok=True)
    if not dest.exists():shutil.copy2(path,dest)
    return key

def transform(lines,orders):
    df=lines.select(F.col('value').alias('raw_json'),F.input_file_name().alias('source_file'))
    df=df.withColumn('p',F.from_json('raw_json',SCHEMA))
    df=df.select('raw_json','source_file',F.trim('p.event_id').alias('event_id'),F.trim('p.order_id').alias('order_id'),
        F.to_timestamp('p.event_time').alias('ts'),F.col('p.payment.amount_cents').alias('amount_cents'),
        F.upper(F.trim('p.payment.currency')).alias('currency'),F.upper(F.trim('p.payment.status')).alias('status'),
        F.col('p._corrupt_record').alias('corrupt'))
    ref=orders.select(F.col('order_id').alias('known_order')).dropDuplicates()
    df=df.join(F.broadcast(ref),df.order_id==ref.known_order,'left')
    reason=(F.when(F.col('corrupt').isNotNull(),'malformed_json')
      .when(F.col('event_id').isNull()|(F.col('event_id')==''),'missing_event_id')
      .when(F.col('order_id').isNull()|(F.col('order_id')==''),'missing_order_id')
      .when(F.col('ts').isNull(),'invalid_timestamp')
      .when(F.col('amount_cents').isNull()|(F.col('amount_cents')<=0),'invalid_amount')
      .when(F.col('currency').isNull()|(F.col('currency')!='INR'),'unsupported_currency')
      .when(F.col('status').isNull()|(~F.col('status').isin('SUCCESS','FAILED')),'invalid_status')
      .when(F.col('known_order').isNull(),'unknown_order'))
    df=df.withColumn('reason',reason).withColumn('event_time',F.date_format('ts',"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
    cols=['event_id','order_id','event_time','amount_cents','currency','status']
    df=df.withColumn('fingerprint',F.sha2(F.to_json(F.struct(*cols)),256))
    return df.select(*cols,'fingerprint','raw_json','source_file','reason')

def references(spark,root):
    src=Path(root)/'sources'
    with sqlite3.connect(src/'merchant_source.sqlite') as db:
        merchants=[dict(zip(['merchant_id','merchant_name','region'],r)) for r in db.execute('SELECT merchant_id,merchant_name,region FROM merchants')]
    # Explicit schema + FAILFAST prevents silently accepting broken CSV values.
    orders=spark.read.option('header',True).option('mode','FAILFAST').schema(ORDER_SCHEMA).csv(str(src/'orders.csv')).cache()
    invalid=orders.filter(F.col('order_id').isNull()|(F.trim('order_id')=='')|F.col('merchant_id').isNull()|
       F.col('amount_cents').isNull()|(F.col('amount_cents')<=0)|F.col('currency').isNull()|(F.col('currency')!='INR')|
       F.to_date('order_date').isNull())
    if invalid.limit(1).count():raise ValueError('Invalid order source; reference load blocked')
    if orders.groupBy('order_id').count().filter('count>1').limit(1).count():raise ValueError('Duplicate order keys')
    if orders.filter(~F.col('merchant_id').isin([m['merchant_id'] for m in merchants])).limit(1).count():raise ValueError('Unknown merchant')
    load_reference(root,merchants,(r.asDict() for r in orders.toLocalIterator()))
    return orders

def batch(root,max_bad_ratio=.2):
    root=Path(root);s=session();start=time.monotonic()
    try:
        orders=references(s,root)
        files=sorted((root/'sources'/'payments').glob('*.json'))
        if not files:raise ValueError('No payment files')
        hashes=[archive(f,root) for f in files]
        hashes += [archive(root/'sources'/'orders.csv',root),archive(root/'sources'/'merchant_source.sqlite',root)]
        key='batch:'+hashlib.sha256('|'.join(sorted(hashes)).encode()).hexdigest()
        clean=transform(s.read.text([str(f) for f in files]),orders)
        (root/'evidence').mkdir(exist_ok=True)
        (root/'evidence'/'spark_plan.txt').write_text(clean._jdf.queryExecution().executedPlan().toString())
        result=commit(root,(r.asDict() for r in clean.toLocalIterator()),key,'batch',max_bad_ratio,started=start)
        print(json.dumps(result,indent=2));return result
    except Exception as e:log_failure(root,e,'batch');raise
    finally:s.stop()

def stream(root,once=False,max_bad_ratio=.2):
    root=Path(root);s=session();inbox=root/'stream_inbox';inbox.mkdir(parents=True,exist_ok=True)
    try:
        with connect(root) as db:ids=[(r[0],) for r in db.execute('SELECT order_id FROM fact_order')]
        if not ids:raise ValueError('Run batch before streaming to initialize reference tables')
        orders=s.createDataFrame(ids,'order_id string')
        clean=transform(s.readStream.option('maxFilesPerTrigger',1).option('pathGlobFilter','*.json').text(str(inbox)),orders)
        def write_microbatch(df,batch_id):
            # Stable stream namespace plus batch ID, checkpoint and warehouse must stay together.
            # Archive exact discovered source files before attempting the sink transaction.
            for row in df.select('source_file').distinct().collect():
                from urllib.parse import urlparse,unquote
                archive(Path(unquote(urlparse(row[0]).path)),root)
            result=commit(root,(r.asDict() for r in df.toLocalIterator()),f'stream-v1:{batch_id}','stream',max_bad_ratio)
            print(json.dumps(result),flush=True)
        writer=clean.writeStream.foreachBatch(write_microbatch).option('checkpointLocation',str(root/'checkpoint'))
        writer=writer.trigger(availableNow=True) if once else writer.trigger(processingTime='5 seconds')
        query=writer.start();query.awaitTermination()
        (root/'evidence').mkdir(exist_ok=True)
        (root/'evidence'/'stream_progress.json').write_text(json.dumps(query.recentProgress,indent=2))
    except Exception as e:log_failure(root,e,'stream');raise
    finally:s.stop()
