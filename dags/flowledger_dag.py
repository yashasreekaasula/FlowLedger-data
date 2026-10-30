"""Daily reconciliation workflow; event stream is an independent long-running process."""
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
with DAG('flowledger_reconciliation',start_date=pendulum.datetime(2026,1,1,tz='UTC'),
         schedule='@daily',catchup=False,max_active_runs=1,
         default_args={'retries':2,'retry_delay':timedelta(seconds=30),'execution_timeout':timedelta(minutes=15)},
         tags=['portfolio','pyspark','quality']) as dag:
    # PROJECT_ROOT injected by Compose. Inputs staged explicitly before triggering.
    base='cd "$PROJECT_ROOT" && python -m flowledger --root "${FLOWLEDGER_DATA_ROOT:-/opt/flowledger/data}" '
    ingest=BashOperator(task_id='batch_transform_and_load',bash_command=base+'batch')
    validate=BashOperator(task_id='validate_warehouse',bash_command=base+'check')
    publish=BashOperator(task_id='publish_metrics',bash_command=base+'report')
    ingest >> validate >> publish
