# [START azure_kubernetespodoperator]
import datetime
from airflow import models
from airflow.models import Variable
from azure.kubernetes import KubernetesPodOperator  # Adjusted import for Azure
from airflow.kubernetes import secret  # Ensure to import the secret module

storage_account_name = Variable.get("storage_account_name")
env_name = Variable.get("env")
default_args = {"email": ["gaurav.gupta@thoughtworks.com"]}

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

# Define a secret from a file
secret_file = secret.Secret(
    deploy_type="volume",
    deploy_target="/tmp/secrets/azure",  # Update to your Azure secret location
    secret="azure-storage-key",  # Update to your Azure storage secret name
    key="key.json",
)

# If a Pod fails to launch, or has an error occur in the container, Airflow
# will show the task as failed, as well as contain all of the task logs
# required to debug.
with models.DAG(
    dag_id="data_prep_cataloguer_pipeline",
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_args,
    start_date=YESTERDAY,
) as dag:
    kubernetes_list_bucket_pod = KubernetesPodOperator(
        task_id="data-normalizer",
        name="data-normalizer",
        cmds=[
            "python",
            "invocation_script.py",
            "-b",
            storage_account_name,  # Use Azure storage account
            "-a",
            "audio_cataloguer",
            "-rc",
            "data/audiotospeech/config/config.yaml",
        ],
        namespace="your_kube_namespace",  # Specify your Kubernetes namespace
        startup_timeout_seconds=300,
        secrets=[secret_file],  # Include the secret file
        image=f"myregistry.azurecr.io/ekstep_data_pipelines:{env_name}_1.0.0",  # Use Azure Container Registry
        image_pull_policy="Always",
    )
