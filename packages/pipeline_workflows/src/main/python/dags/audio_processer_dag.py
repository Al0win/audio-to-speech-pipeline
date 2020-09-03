import json
import datetime
import math
import time

from airflow import DAG
from airflow.models import Variable
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.operators.python_operator import PythonOperator
from helper_dag import get_audio_ids, get_files_from_landing_zone, move_raw_to_processed

snr_catalogue_source = json.loads(Variable.get("snrcatalogue"))
source_path_for_snr = Variable.get("sourcepathforsnr")
error_landing_path_snr = Variable.get("errorlandingpathsnr")
tobe_processed_path_snr = Variable.get("tobeprocessedpathsnr")

# processed_path = Variable.get("rawprocessedpath")

composer_namespace = Variable.get("composer_namespace")
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

secret_file = secret.Secret(
    deploy_type='volume',
    deploy_target='/tmp/secrets/google',
    secret='gc-storage-rw-key',
    key='key.json')


def create_dag(dag_id,
               dag_number,
               default_args,
               args,
               batch_count):
    dag = DAG(dag_id,
              schedule_interval=datetime.timedelta(days=1),
              default_args=default_args,
              start_date=YESTERDAY)

    with dag:

        audio_format = args.get('audio_format')
        print(args)

        copy_files_in_buckets = PythonOperator(
            task_id=dag_id + "_copy_landing_tobeprocessed",
            python_callable=get_files_from_landing_zone,
            op_kwargs={'source': dag_id, 'source_landing_path': source_path_for_snr, 'error_landing_path': error_landing_path_snr,
                       'tobe_processed_path': tobe_processed_path_snr, 'batch_count': batch_count, 'audio_format': audio_format},
            dag_number=dag_number)

        fetch_audio_ids = PythonOperator(
            task_id=dag_id + "_fetch_audio_ids",
            python_callable=get_audio_ids,
            op_kwargs={'source': dag_id,
                       'tobe_processed_path': tobe_processed_path_snr},
            dag_number=dag_number)

        copy_files_in_buckets >> fetch_audio_ids

        parallelism = args.get("parallelism")

        audio_file_ids = json.loads(Variable.get("audiofileids"))[dag_id]

        batches = []

        if len(audio_file_ids) > 0:
            chunk_size = math.ceil(len(audio_file_ids) / parallelism)
            batches = [audio_file_ids[i:i + chunk_size]
                       for i in range(0, len(audio_file_ids), chunk_size)]

        for batch_audio_file_ids in batches:
            data_prep_task = kubernetes_pod_operator.KubernetesPodOperator(
                task_id=dag_id + "_data_snr_",
                name='data-prep-snr',
                cmds=["python", "invocation_script.py", "-a", "audio_processing", "-rc", "data/audiotospeech/config/audio_processing/config.yaml",
                      "-ai", ','.join(batch_audio_file_ids), "-af", args.get('audio_format'), "-as", dag_id],
                namespace=composer_namespace,
                startup_timeout_seconds=300,
                secrets=[secret_file],
                image='us.gcr.io/ekstepspeechrecognition/ekstep_data_pipelines:1.0.0',
                image_pull_policy='Always')

            move_to_processed = PythonOperator(
                task_id=dag_id + "_move_raw_to_processed_" + batch_audio_file_ids[0],
                python_callable=move_raw_to_processed,
                op_kwargs={'source': dag_id, 'batch_audio_file_ids': batch_audio_file_ids,
                           'tobe_processed_path': tobe_processed_path_snr},
                dag_number=dag_number)

            fetch_audio_ids >> data_prep_task >> move_to_processed

    return dag


for source in snr_catalogue_source.keys():
    source_info = snr_catalogue_source.get(source)

    batch_count = source_info.get('count')
    parallelism = source_info.get('parallelism', batch_count)
    audio_format = source_info.get('format')

    dag_id = source

    dag_args = {
        'email': ['gaurav.gupta@thoughtworks.com'],
    }

    args = {
        'audio_format': audio_format,
        'parallelism': parallelism
    }

    # schedule = '@daily'

    dag_number = dag_id + str(batch_count)

    globals()[dag_id] = create_dag(dag_id,
                                   dag_number,
                                   dag_args,
                                   args,
                                   batch_count)
