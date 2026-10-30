"""Small local serving warehouse; transactional commits and durable event dedup."""
import sqlite3,json,hashlib,time,uuid
from contextlib import contextmanager
from pathlib import Path
from datetime import datetime,timezone

SQL=Path(__file__).resolve().parents[1]/'sql'
def now():return datetime.now(timezone.utc).isoformat()
def digest(value):return hashlib.sha256(value.encode()).hexdigest()
@contextmanager
def connect(root):
    root=Path(root);root.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(root/'warehouse.sqlite',timeout=60)
    db.row_factory=sqlite3.Row
    db.execute('PRAGMA foreign_keys=ON');db.execute('PRAGMA journal_mode=WAL')
    db.executescript((SQL/'schema.sql').read_text())
    try:
        with db:
            yield db
    finally:
        db.close()

def load_reference(root, merchants, orders):
    with connect(root) as db:
        db.execute('BEGIN IMMEDIATE')
        for m in merchants:
            old=db.execute('SELECT * FROM dim_merchant WHERE merchant_id=?',(m['merchant_id'],)).fetchone()
            values=(m['merchant_id'],m['merchant_name'],m['region'])
            if old and tuple(old)!=values:raise ValueError('Reference mutation: use an explicit dimension migration')
            db.execute('INSERT OR IGNORE INTO dim_merchant VALUES(?,?,?)',values)
        for o in orders:
            values=(o['order_id'],o['merchant_id'],o['order_date'],o['amount_cents'],o['currency'])
            old=db.execute('SELECT * FROM fact_order WHERE order_id=?',(o['order_id'],)).fetchone()
            if old and tuple(old)!=values:raise ValueError('Order mutation detected; immutable order contract')
            db.execute('INSERT OR IGNORE INTO fact_order VALUES(?,?,?,?,?)',values)

def commit(root, rows, run_key, mode, max_bad_ratio=.20, fail_after=None, started=None):
    """Entire sink batch + audit row is atomic. Exceptions roll back all writes.
    fail_after injects a crash for tests; it must never be set in normal jobs.
    toLocalIterator upstream bounds memory, but this sink remains single-node.
    """
    start=started if started is not None else time.monotonic()
    with connect(root) as db:
        db.execute('BEGIN IMMEDIATE')
        old=db.execute('SELECT * FROM pipeline_run WHERE run_key=?',(run_key,)).fetchone()
        if old:return dict(old)|{'replayed':True}
        counts={'input_rows':0,'inserted_rows':0,'rejected_rows':0,'duplicate_rows':0}
        for row in rows:
            r=dict(row); counts['input_rows']+=1; reason=r.get('reason')
            # Defensive FK validation also protects callers outside Spark.
            if not reason and not db.execute('SELECT 1 FROM fact_order WHERE order_id=?',(r['order_id'],)).fetchone():reason='unknown_order'
            prior=db.execute('SELECT fingerprint FROM fact_payment WHERE event_id=?',(r.get('event_id'),)).fetchone()
            if not reason and prior:
                if prior[0]==r['fingerprint']:
                    counts['duplicate_rows']+=1;continue
                reason='conflicting_event_id'
            if reason:
                counts['rejected_rows']+=1
                db.execute('INSERT OR IGNORE INTO quarantine VALUES(?,?,?,?,?,?)',
                  (digest(r['raw_json']+'|'+reason),r.get('event_id'),reason,r['raw_json'],r['source_file'],now()))
            else:
                db.execute('INSERT INTO fact_payment VALUES(?,?,?,?,?,?,?,?,?)',
                  (r['event_id'],r['order_id'],r['event_time'],r['amount_cents'],r['currency'],r['status'],r['fingerprint'],r['source_file'],now()))
                counts['inserted_rows']+=1
            if fail_after is not None and counts['input_rows']>=fail_after:raise RuntimeError('Injected failure before commit')
        if counts['rejected_rows']/max(counts['input_rows'],1)>max_bad_ratio:
            raise ValueError('Quality gate failed: reject ratio exceeds threshold; transaction rolled back')
        result={'run_key':run_key,'mode':mode,'completed_at':now(),**counts,'duration_seconds':round(time.monotonic()-start,4)}
        db.execute('INSERT INTO pipeline_run VALUES(:run_key,:mode,:completed_at,:input_rows,:inserted_rows,:rejected_rows,:duplicate_rows,:duration_seconds)',result)
    return result

def log_failure(root,error,mode):
    folder=Path(root)/'failures';folder.mkdir(parents=True,exist_ok=True)
    (folder/f'{uuid.uuid4().hex}.json').write_text(json.dumps({'time':now(),'mode':mode,'error_type':type(error).__name__,'error':str(error)},indent=2))
