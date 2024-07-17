import datetime
import json

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator


trigger_training_config = json.loads(Variable.get("trigger_training_config"))
command = trigger_training_config.get("command")
resource_group = trigger_training_config.get("resource_group")
vm_name = trigger_training_config.get("vm_name")
ssh_user = trigger_training_config.get("ssh_user")  # Ensure this is set in your trigger_training_config variable

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

# Use Azure CLI to SSH into the VM and execute the command
bash_cmd = f'az vm run-command invoke --resource-group {resource_group} --name {vm_name} --command-id RunShellScript --scripts "{command}" --parameters username={ssh_user}'

with DAG(dag_id="trigger_training", schedule_interval='@daily', start_date=yesterday, default_args=default_dag_args, catchup=False) as dag:
    start = DummyOperator(task_id='start')
    
    end = DummyOperator(task_id='end')
         
    bash_remote_azure_vm = BashOperator(task_id='bash_remote_azure_vm_task', bash_command=bash_cmd)
    
start >> bash_remote_azure_vm >> end
