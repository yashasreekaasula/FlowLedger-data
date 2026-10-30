import json
import pytest
from flowledger.pipeline import session,transform

@pytest.fixture(scope='module')
def spark():
    s=session();yield s;s.stop()

def test_spark_nested_parse_validation_and_normalization(spark):
    valid=dict(event_id=' E1 ',order_id='O1',event_time='2026-10-01T02:00:00+02:00',payment=dict(amount_cents=500,currency=' inr ',status=' success '))
    samples=[json.dumps(valid),'{bad',json.dumps(valid|{'event_id':'E2','event_time':'bad'}),
      json.dumps(valid|{'event_id':'E3','order_id':'UNKNOWN'}),
      json.dumps(valid|{'event_id':'E4','payment':{'amount_cents':-5,'currency':'INR','status':'SUCCESS'}}),
      json.dumps(valid|{'event_id':'E5','payment':{'amount_cents':500,'currency':'USD','status':'SUCCESS'}}),
      json.dumps(valid|{'event_id':'E6','payment':{'amount_cents':500,'currency':'INR','status':'PENDING'}})]
    rows=transform(spark.createDataFrame([(x,) for x in samples],'value string'),spark.createDataFrame([('O1',)],'order_id string')).collect()
    reasons={r.reason for r in rows}
    assert reasons=={None,'malformed_json','invalid_timestamp','unknown_order','invalid_amount','unsupported_currency','invalid_status'}
    good=next(r for r in rows if r.reason is None)
    assert good.event_id=='E1' and good.event_time=='2026-10-01T00:00:00.000Z' and good.currency=='INR'
