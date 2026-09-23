from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator

PROJECT = '/opt/airflow/project'


def failure_callback(context):
    ti = context['task_instance']
    dag_run = context['dag_run']
    print('=' * 60)
    print('TASK FAILED')
    print(f'  dag_id:        {ti.dag_id}')
    print(f'  task_id:       {ti.task_id}')
    print(f'  run_id:        {context["run_id"]}')
    print(f'  logical_date:  {context["logical_date"]}')
    print(f'  try_number:    {ti.try_number}')
    print(f'  max_tries:     {ti.max_tries}')
    print(f'  params:        {dag_run.conf or {}}')
    print('=' * 60)


DEFAULT_ARGS = {
    'owner': 'dss150p',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=15),
    'on_failure_callback': failure_callback,
}


def _bash(cmd: str) -> str:
    return f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli {cmd}'


def _pick_load_branch(**context):
    """Return the task_id of the load task to run, based on run_mode param."""
    mode = context['params'].get('run_mode', 'full')
    return 'load_partition' if mode == 'partition' else 'load_full'


with DAG(
    dag_id='dss150p_sales_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule='0 2 * * *',
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        'run_mode': Param('full', enum=['full', 'partition'],
                          description='full = run whole pipeline; partition = load one year/month'),
        'year':  Param(2026, type='integer', description='Used when run_mode=partition'),
        'month': Param(1, type='integer', minimum=1, maximum=12,
                       description='Used when run_mode=partition'),
    },
    tags=['DSS150P'],
) as dag:

    begin_run = BashOperator(
        task_id='begin_run',
        bash_command=_bash('begin-run'),
        execution_timeout=timedelta(minutes=2),
    )

    extract = BashOperator(
        task_id='extract',
        bash_command=_bash('extract'),
        execution_timeout=timedelta(minutes=10),
    )

    transform = BashOperator(
        task_id='transform',
        bash_command=_bash('transform'),
        execution_timeout=timedelta(minutes=15),
    )

    choose_load = BranchPythonOperator(
        task_id='choose_load_branch',
        python_callable=_pick_load_branch,
    )

    load_full = BashOperator(
        task_id='load_full',
        bash_command=_bash('load'),
        execution_timeout=timedelta(minutes=15),
    )

    load_partition = BashOperator(
        task_id='load_partition',
        bash_command=_bash(
            'load-partition --year {{ params.year }} --month {{ params.month }}'
        ),
        execution_timeout=timedelta(minutes=10),
    )

    validate = BashOperator(
        task_id='validate',
        bash_command=_bash('validate'),
        trigger_rule='none_failed_min_one_success',
        execution_timeout=timedelta(minutes=5),
    )

    end_run = BashOperator(
        task_id='end_run',
        bash_command=_bash('end-run'),
        trigger_rule='none_failed_min_one_success',
        execution_timeout=timedelta(minutes=2),
    )

    begin_run >> extract >> transform >> choose_load
    choose_load >> [load_full, load_partition] >> validate >> end_run