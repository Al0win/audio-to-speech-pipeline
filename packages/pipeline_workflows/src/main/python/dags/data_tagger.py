import datetime
import json

from airflow import models
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from move_exp_data_dag_processor import count_utterances_file_chunks, copy_utterances

# Load variables
composer_namespace = Variable.get("composer_namespace")
bucket_name = Variable.get("bucket")
env_name = Variable.get("env")
default_args = {"email": ["gaurav.gupta@thoughtworks.com"]}
project = Variable.get("project")

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

# Define Kubernetes secret
secret_file = secret.Secret(
    deploy_type="volume",
    deploy_target="/tmp/secrets/google",
    secret="gc-storage-rw-key",
    key="key.json",
)

# DAG definition
dag_id = "data_tagger_pipeline"
dag = models.DAG(
    dag_id,
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_args,
    start_date=YESTERDAY,
)

with dag:
    # Task to run the data tagger
    kubernetes_list_bucket_pod = KubernetesPodOperator(
        task_id="data-tagger",
        name="data-tagger",
        cmds=[
            "python",
            "-m",
            "src.scripts.data_tagger",
            "cluster",
            bucket_name,
            "data/audiotospeech/config/datatagger/config.yaml",
        ],
        namespace=composer_namespace,
        startup_timeout_seconds=300,
        secrets=[secret_file],
        image=f"us.gcr.io/{project}/data_tagger:{env_name}_1.0.0",
        image_pull_policy="Always",
    )

    # Task to count utterances file chunks
    count_utterances_chunks_list = PythonOperator(
        task_id=f"{dag_id}_count_utterances_file_chunks",
        python_callable=count_utterances_file_chunks,
        op_kwargs={"source": dag_id},
    )

    kubernetes_list_bucket_pod >> count_utterances_chunks_list

    # Load utterances chunks list and batch count
    utterances_chunks_list = json.loads(Variable.get("utteranceschunkslist"))
    utterances_batch_count = int(Variable.get("utterancesconcurrentbatchcount"))

    # Create copy tasks for utterances chunks
    for index, utterances_chunk in enumerate(utterances_chunks_list["utteranceschunkslist"]):
        if index >= utterances_batch_count:
            break

        copy_utterance_files = PythonOperator(
            task_id=f"{dag_id}_copy_utterances_{index}",
            python_callable=copy_utterances,
            op_kwargs={"src_file_name": utterances_chunk},
        )
        
        count_utterances_chunks_list >> copy_utterance_files
