"""Deterministic synthetic source systems. No real financial/customer data."""
import csv,json,sqlite3,random,os,time,uuid
from pathlib import Path
from datetime import datetime,timezone,timedelta

def atomic_jsonl(path, rows):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_name('.'+path.name+'.tmp')
    tmp.write_text(''.join(json.dumps(r,sort_keys=True)+'\n' for r in rows))
    os.replace(tmp,path)  # Spark sees only fully written .json files.

def generate(root, count=1000, day='2026-10-01'):
    if count<20: raise ValueError('Use at least 20 orders')
    root=Path(root); src=root/'sources';src.mkdir(parents=True,exist_ok=True)
    if (src/'orders.csv').exists():
        raise FileExistsError('Sources already exist: choose another --root; do not overwrite replay inputs')
    rng=random.Random(38)
    with sqlite3.connect(src/'merchant_source.sqlite') as db:
        db.execute('CREATE TABLE merchants(merchant_id TEXT PRIMARY KEY,merchant_name TEXT,region TEXT)')
        db.executemany('INSERT INTO merchants VALUES(?,?,?)',[(f'M{i}',f'Demo Merchant {i}',r) for i,r in enumerate(['South','North','West','East'],1)])
    orders=[];events=[]; base=datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
    for i in range(count):
        amount=rng.randint(100,10000)*100
        order={'order_id':f'O{i:07d}','merchant_id':f'M{i%4+1}','order_date':day,'amount_cents':amount,'currency':'INR'}
        orders.append(order)
        if i%10==0: continue # genuinely unpaid orders
        events.append({'event_id':f'E{i:07d}','order_id':order['order_id'],
            'event_time':(base+timedelta(seconds=i)).isoformat(),
            'payment':{'amount_cents':amount+(100 if i%17==0 else 0),'currency':'INR','status':'FAILED' if i%13==0 else 'SUCCESS'}})
    with (src/'orders.csv').open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=list(orders[0]));writer.writeheader();writer.writerows(orders)
    # Separate files have duplicate business events, bad references, values and malformed JSON.
    bad=[{'event_id':'BAD_NEG','order_id':orders[0]['order_id'],'event_time':base.isoformat(),'payment':{'amount_cents':-10,'currency':'INR','status':'SUCCESS'}},
         {'event_id':'BAD_REF','order_id':'MISSING','event_time':base.isoformat(),'payment':{'amount_cents':100,'currency':'INR','status':'SUCCESS'}},
         {'event_id':'BAD_TIME','order_id':orders[0]['order_id'],'event_time':'not-a-date','payment':{'amount_cents':100,'currency':'INR','status':'SUCCESS'}}]
    atomic_jsonl(src/'payments'/'batch-001.json',events+events[:10]+bad)
    with (src/'payments'/'batch-001.json').open('a') as f:f.write('{broken json\n')
    (root/'stream_inbox').mkdir(exist_ok=True)
    expected={'synthetic':True,'orders':count,'unique_valid_payments':len(events),'invalid_rows':4,'duplicate_rows':min(10,len(events))}
    (src/'expected.json').write_text(json.dumps(expected,indent=2))
    return expected

def emit(root, count=5, interval=1.0):
    root=Path(root)
    # Emit payments only for existing orders that were intentionally unpaid.
    with (root/'sources'/'orders.csv').open() as f:
        orders=[r for r in csv.DictReader(f) if int(r['order_id'][1:])%10==0][:count]
    for order in orders:
        event={'event_id':'LIVE_'+uuid.uuid4().hex,'order_id':order['order_id'],
          'event_time':datetime.now(timezone.utc).isoformat(),
          'payment':{'amount_cents':int(order['amount_cents']),'currency':'INR','status':'SUCCESS'}}
        atomic_jsonl(root/'stream_inbox'/f'{event["event_id"]}.json',[event])
        print('Published',event['event_id'],flush=True);time.sleep(interval)
