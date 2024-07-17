import datetime
import json
import math

from airflow import DAG
from azure.kubernetes import secret  # Adjusted import for Azure
from azure.kubernetes import KubernetesPodOperator  # Adjusted import for Azure
from airflow.models import Variable
from airflow.operators import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from helper_dag import fetch_require_audio_ids_for_stt, fetch_upload_db_data_dump

sourceinfo = json.loads(Variable.get("sourceinfo"))
storage_account_name = Variable.get("storage_account_name")  # Use Azure storage
env_name = Variable.get("env")
composer_namespace = Variable.get("composer_namespace")
resource_limits = json.loads(Variable.get("stt_resource_limits"))
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
LANGUAGE_CONSTANT = "{language}"
project = Variable.get("project")

# Define a secret from Azure
secret_file = secret.Secret(
    deploy_type="volume",
    deploy_target="/tmp/secrets/azure",  # Update to your Azure secret location
    secret="azure-storage-key",  # Update to your Azure storage secret name
    key="key.json",
)

def create_dag(dag_id, dag_number, default_args, args, batch_count):
    dag = DAG(
        f"{dag_id}_stt_" + args.get("stt") + '_' + args.get("language"),
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_args,
        start_date=YESTERDAY,
    )

    with dag:
        language = args.get("language")
        data_set = args.get("data_set")
        source_path = args.get("source_path")
        stt = args.get("stt")
        print(args)
        print(f"Language for source is {language}")

        next_dag_id = 'trigger_training'
        trigger_dependent_dag = TriggerDagRunOperator(
            task_id="trigger_dependent_dag_" + next_dag_id,
            trigger_dag_id=next_dag_id,
        )

        fetch_audio_ids = PythonOperator(
            task_id=dag_id + "_fetch_audio_ids",
            python_callable=fetch_require_audio_ids_for_stt,
            op_kwargs={
                "source": dag_id,
                "language": language.title(),
                "stt": stt,
                "data_set": data_set,
                "bucket_name": storage_account_name,
            },
            dag_number=dag_number,
        )

        fetch_data_snapshot = PythonOperator(
            task_id=dag_id + "_generate_data_snapshot",
            python_callable=fetch_upload_db_data_dump,
            op_kwargs={
                "source": dag_id,
                "language": language.title(),
                "bucket_name": storage_account_name,
            },
            dag_number=dag_number,
        )

        [fetch_audio_ids, fetch_data_snapshot]

        def batch_audio_ids(d, each_pod_batch_size):
            each_pod_batch_size = min(each_pod_batch_size, sum(d.values()))
            d = d.copy()
            temp = {}
            c = list(d.keys())
            e = list(d.values())

            i = 0
            while each_pod_batch_size > 0:
                diff = each_pod_batch_size - e[i]

                if diff > 0:
                    temp[c[i]] = e[i]
                    each_pod_batch_size = diff
                    del d[c[i]]
                else:
                    temp[c[i]] = each_pod_batch_size
                    if diff == 0:
                        del d[c[i]]
                    else:
                        d[c[i]] = abs(diff)
                    break
                c = list(d.keys())
                e = list(d.values())

            return d, list(temp.keys())

        parallelism = args.get("parallelism")

        audio_file_ids = json.loads(Variable.get("audioidsforstt"))[dag_id]
        if audio_file_ids:
            each_pod_batch_size = math.ceil(sum(audio_file_ids.values()) / parallelism)
        batches = []
        while audio_file_ids:
            audio_file_ids, batch = batch_audio_ids(audio_file_ids, each_pod_batch_size)
            batches.append(batch)

        for batch_audio_file_ids in batches:
            data_prep_task = KubernetesPodOperator(
                task_id=dag_id + "_data_stt_" + batch_audio_file_ids[0],
                name="data-prep-stt",
                cmds=[
                    "python",
                    "invocation_script.py",
                    "-b",
                    storage_account_name,  # Use Azure storage account
                    "-a",
                    "audio_transcription",
                    "-rc",
                    "data/audiotospeech/config/config.yaml",
                    "-ai",
                    ",".join(batch_audio_file_ids),
                    "-ds",
                    data_set,
                    "-as",
                    dag_id,
                    "-stt",
                    stt,
                    "-l",
                    language,
                    "-sp",
                    source_path,
                ],
                namespace=composer_namespace,
                startup_timeout_seconds=300,
                secrets=[secret_file],
                image=f"myregistry.azurecr.io/ekstep_data_pipelines:{env_name}_1.0.0",  # Use Azure Container Registry
                image_pull_policy="Always",
                resources=resource_limits,
            )

            [fetch_audio_ids, fetch_data_snapshot] >> data_prep_task >> trigger_dependent_dag

    return dag


for source in sourceinfo.keys():
    source_info = sourceinfo.get(source)

    batch_count = source_info.get("count", 5)
    parallelism = source_info.get("parallelism", batch_count)
    api = source_info.get("stt", 'google')
    source_path = source_info.get("source_path", 'dummy')
    language = source_info.get("language").lower()
    data_set = source_info.get("data_set", '').lower()
    dag_id = source

    dag_args = {
        "email": ["ekstep@thoughtworks.com"],
    }

    args = {"parallelism": parallelism, "stt": api, "language": language, "data_set": data_set,
            "source_path": source_path}

    dag_number = dag_id + str(batch_count)

    globals()[dag_id] = create_dag(dag_id, dag_number, dag_args, args, batch_count)
