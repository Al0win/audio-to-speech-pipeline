import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from data_validation_report_processor import report_generation_pipeline

# Set up date for DAG start
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
bucket_name = Variable.get("bucket")
env_name = Variable.get("env")
key_vault_name = Variable.get("key_vault_name")  # Add the Key Vault name to your Airflow Variables

# Create a SecretClient to access Azure Key Vault
credential = DefaultAzureCredential()
key_vault_uri = f"https://{key_vault_name}.vault.azure.net/"
secret_client = SecretClient(vault_url=key_vault_uri, credential=credential)

# Retrieve the secret (for example, a service account key)
# Replace 'gc-storage-rw-key' with your secret name in Azure Key Vault
secret_name = "gc-storage-rw-key"
secret_value = secret_client.get_secret(secret_name).value

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
            op_kwargs={"stage": stage, "bucket": bucket_name, "secret": secret_value},
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
