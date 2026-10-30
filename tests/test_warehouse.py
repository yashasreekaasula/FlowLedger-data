import json
import pytest
from flowledger.warehouse import connect,load_reference,commit

@pytest.fixture
def root(tmp_path):
    load_reference(tmp_path,[dict(merchant_id='M1',merchant_name='Test',region='South')],
       [dict(order_id='O1',merchant_id='M1',order_date='2026-10-01',amount_cents=10000,currency='INR')])
    return tmp_path

def event(key='E1',**changes):
    r=dict(event_id=key,order_id='O1',event_time='2026-10-01T00:00:00.000Z',amount_cents=10000,
      currency='INR',status='SUCCESS',fingerprint='hash-'+key,source_file='test.json',raw_json=json.dumps({'id':key}),reason=None)
    r.update(changes);return r

def count(root,table):
    with connect(root) as db:return db.execute('SELECT COUNT(*) FROM '+table).fetchone()[0]

def test_same_run_replay_does_not_double_count(root):
    commit(root,[event()],'run1','batch')
    assert commit(root,[event()],'run1','batch')['replayed']
    assert count(root,'fact_payment')==1
    assert count(root,'pipeline_run')==1

def test_duplicate_across_batches(root):
    commit(root,[event()],'run1','stream')
    r=commit(root,[event()],'run2','stream')
    assert r['duplicate_rows']==1 and count(root,'fact_payment')==1

def test_conflicting_id_is_quarantined(root):
    commit(root,[event()],'run1','stream')
    r=commit(root,[event(fingerprint='changed',amount_cents=5000)],'run2','stream',1)
    assert r['rejected_rows']==1
    with connect(root) as db:
        assert db.execute('SELECT amount_cents FROM fact_payment').fetchone()[0]==10000
        assert db.execute('SELECT reason FROM quarantine').fetchone()[0]=='conflicting_event_id'

def test_crash_rolls_back_then_retry_succeeds(root):
    with pytest.raises(RuntimeError):commit(root,[event(),event('E2')],'run1','batch',fail_after=1)
    assert count(root,'fact_payment')==count(root,'pipeline_run')==0
    assert commit(root,[event(),event('E2')],'run1','batch')['inserted_rows']==2

def test_quality_gate_rolls_back_good_and_bad_rows(root):
    with pytest.raises(ValueError):commit(root,[event(),event('E2',reason='invalid_amount')],'run1','batch',.2)
    assert count(root,'fact_payment')==count(root,'quarantine')==count(root,'pipeline_run')==0

def test_unknown_reference_is_rejected(root):
    assert commit(root,[event(order_id='UNKNOWN')],'run1','batch',1)['rejected_rows']==1
    assert count(root,'fact_payment')==0

def test_late_event_is_retained(root):
    commit(root,[event(event_time='2025-01-01T00:00:00.000Z')],'late','stream')
    with connect(root) as db:assert db.execute('SELECT event_date FROM daily_revenue').fetchone()[0]=='2025-01-01'

def test_reconciliation_amount_and_status(root):
    commit(root,[event(status='FAILED')],'failed','batch')
    with connect(root) as db:assert db.execute('SELECT reconciliation_status FROM order_reconciliation').fetchone()[0]=='UNPAID'
    commit(root,[event('E2',amount_cents=9000)],'partial','batch')
    with connect(root) as db:
        row=db.execute('SELECT reconciliation_status,difference_cents FROM order_reconciliation').fetchone()
        assert tuple(row)==('MISMATCH',-1000)

def test_order_mutation_is_blocked(root):
    with pytest.raises(ValueError):load_reference(root,[],[dict(order_id='O1',merchant_id='M1',order_date='2026-10-01',amount_cents=1,currency='INR')])

def test_reference_failure_is_atomic(root):
    with pytest.raises(Exception):load_reference(root,[dict(merchant_id='M2',merchant_name='Other',region='West')],
        [dict(order_id='O2',merchant_id='MISSING',order_date='2026-10-01',amount_cents=1,currency='INR')])
    assert count(root,'dim_merchant')==1
