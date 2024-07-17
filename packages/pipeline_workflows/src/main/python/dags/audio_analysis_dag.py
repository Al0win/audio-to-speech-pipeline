import datetime
import json
import math

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from azure.storage.blob import BlobServiceClient  # Adjusted import for Azure
from azure.kubernetes import KubernetesPodOperator  # Adjusted for Azure

from helper_dag import generate_splitted_batches_for_audio_analysis

audio_analysis_config = json.loads(Variable.get("audio_analysis_config"))
source_path = Variable.get("sourcepathforaudioanalysis")
destination_path = Variable.get("destinationpathforaudioanalysis")
storage_account_name = Variable.get("storage_account_name")
env_name = Variable.get("env")
kube_namespace = Variable.get("kube_namespace")
# Azure equivalent of GCP project is typically not used in the same way

resource_limits = json.loads(Variable.get("audio_analysis_resource_limits"))

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
LANGUAGE_CONSTANT = "{language}"

# Azure equivalent secret handling would go here, depending on your setup
# You may use Azure Key Vault or similar

def interpolate_language_paths(path, language):
    path_set = path.replace(LANGUAGE_CONSTANT, language)
    return path_set

def create_dag(dag_id, dag_number, default_args, args, max_records_threshold_per_pod):
    dag = DAG(
        dag_id,
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_args,
        start_date=YESTERDAY,
    )

    with dag:
        audio_format = args.get("audio_format")
        language = args.get("language")
        parallelism = args.get("parallelism")
        source = args.get("source")
        print(args)
        print(f"Language for source is {language}")
        source_path_set = interpolate_language_paths(source_path, language)
        destination_path_set = interpolate_language_paths(destination_path, language)

        next_dag_id = 'data_marker_pipeline'
        trigger_dependent_dag = TriggerDagRunOperator(
            task_id="trigger_dependent_dag_" + next_dag_id,
            trigger_dag_id=next_dag_id,
        )
        
        generate_batch_files = PythonOperator(
            task_id=dag_id + "_generate_batches",
            python_callable=generate_splitted_batches_for_audio_analysis,
            op_kwargs={
                "source": source,
                "source_path": source_path_set,
                "destination_path": destination_path_set,
                "max_records_threshold_per_pod": max_records_threshold_per_pod,
                "audio_format": audio_format,
                "storage_account_name": storage_account_name,  # Use Azure storage account
            },
            dag_number=dag_number,
        )

        generate_batch_files

        data_audio_analysis_task = KubernetesPodOperator(
            task_id=f"data-audio-analysis-{source}",
            name="data-audio-analysis",
            cmds=[
                "python",
                "invocation_script.py",
                "-b",
                storage_account_name,  # Adjust for Azure
                "-a",
                "audio_analysis",
                "-rc",
                f"data/audiotospeech/config/config.yaml",
                "-as",
                source,
                "-l",
                language,
            ],
            namespace=kube_namespace,
            startup_timeout_seconds=300,
            # Add Azure-specific secrets handling here if needed
            image=f"myregistry.azurecr.io/ekstep_data_pipelines:{env_name}_1.0.0",  # Use Azure Container Registry
            image_pull_policy="Always",
            resources=resource_limits,
        )

        batch_file_path_dict = json.loads(Variable.get("embedding_batch_file_list"))
        list_of_batch_files = batch_file_path_dict[source]
        if len(list_of_batch_files) > 0:
            total_phases = math.ceil(len(list_of_batch_files) / parallelism)
            task_dict = {}
            for phase in range(0, total_phases):
                for pod in range(1, parallelism + 1):
                    if len(list_of_batch_files) > 0:
                        batch_file = list_of_batch_files.pop()
                        task_dict[
                            f"create_embedding_task_{pod}_{phase}"] = KubernetesPodOperator(
                            task_id=source + "_create_embedding_" + str(pod) + '_' + str(phase),
                            name="create-embedding",
                            cmds=[
                                "python",
                                "invocation_script.py",
                                "-b",
                                storage_account_name,  # Adjust for Azure
                                "-a",
                                "audio_embedding",
                                "-rc",
                                f"data/audiotospeech/config/config.yaml",
                                "-fp",
                                batch_file,
                                "-as",
                                source,
                                "-l",
                                language,
                            ],
                            namespace=kube_namespace,
                            startup_timeout_seconds=300,
                            # Add Azure-specific secrets handling here if needed
                            image=f"myregistry.azurecr.io/ekstep_data_pipelines:{env_name}_1.0.0",  # Use Azure Container Registry
                            image_pull_policy="Always",
                            resources=resource_limits,
                        )
                        if phase == 0:
                            generate_batch_files >> task_dict[
                                f"create_embedding_task_{pod}_{phase}"] >> data_audio_analysis_task >> trigger_dependent_dag
                        else:
                            task_dict[f"create_embedding_task_{pod}_{phase - 1}"] >> task_dict[
                                f"create_embedding_task_{pod}_{phase}"] >> data_audio_analysis_task >> trigger_dependent_dag
        else:
            generate_batch_files >> data_audio_analysis_task >> trigger_dependent_dag

    return dag

for source in audio_analysis_config.keys():
    source_info = audio_analysis_config.get(source)

    max_records_threshold_per_pod = source_info.get("batch_size")
    parallelism = source_info.get("parallelism", 5)
    audio_format = source_info.get("format")
    language = source_info.get("language").lower()

    dag_id = source + '_' + language + '_' + 'audio_embedding_analysis'

    dag_args = {
        "email": ["your_email@example.com"],  # Update email as necessary
    }

    args = {
        "audio_format": audio_format,
        "parallelism": parallelism,
        "language": language,
        "source": source
    }

    dag_number = dag_id + str(max_records_threshold_per_pod)

    globals()[dag_id] = create_dag(dag_id, dag_number, dag_args, args, max_records_threshold_per_pod)
