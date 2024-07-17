import datetime

from airflow import DAG
from airflow.contrib.kubernetes import secret
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from data_validation_report_processor import report_generation_pipeline

# Define Kubernetes secret
secret_file = secret.Secret(
    deploy_type="volume",
    deploy_target="/tmp/secrets/google",
    secret="gc-storage-rw-key",
    key="key.json",
)

# Set up date for DAG start
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
bucket_name = Variable.get("bucket")
env_name = Variable.get("env")

def create_dag(dag_id, stage, default_args):
    dag = DAG(
        f"{dag_id}_{stage}",
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_args,
        start_date=YESTERDAY,
    )
    with dag:
        report_generation_pipeline_task = PythonOperator(
            task_id=f"{dag_id}_{stage}_report_generation_and_upload",
            python_callable=report_generation_pipeline,
            op_kwargs={"stage": stage, "bucket": bucket_name},
        )

        report_generation_pipeline_task
    return dag

dag_args = {
    "email": ["soujyo.sen@thoughtworks.com"],
}

# Create DAGs for different stages
globals()["report_generation_pipeline_pre_transcription"] = create_dag(
    "report_generation_pipeline", "pre-transcription", default_args=dag_args
)
globals()["report_generation_pipeline_post_transcription"] = create_dag(
    "report_generation_pipeline", "post-transcription", default_args=dag_args
)
