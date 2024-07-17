#!/bin/bash
. ./env-config.cfg
#Fetch current Azure Data Factory environment details
ENV=$1
AZURE_STORAGE_ACCOUNT=<YourAzureStorageAccountName>
AZURE_STORAGE_CONTAINER=<YourAzureStorageContainerName>

# Check if the Data Factory environment exists
for env in $(az datafactory list --resource-group $RESOURCE_GROUP --query "[].name" -o tsv); do
  echo "The Data Factory is: $env"
  if [ -z "$env" ]; then
    echo "ERROR: There is no existing Data Factory environment, hence exiting..." >&2
    exit 1
  fi

  case "$env" in
  *"$ENV"*)
    DATA_FACTORY_ENV=$env
    variables_json="airflow_config_file_${ENV}.json"
    echo "Data Factory environment name: $DATA_FACTORY_ENV"
    ;;
  *) echo 'False' ;;
  esac
done

# Upload env variables

echo "$variables_json file"
echo "$PATH path"
echo "$(python -V)"
pyenv install --list
sudo -E env "PATH=$PATH" az extension add --name datafactory

# Upload the variables JSON file to Azure Blob Storage
az storage blob upload --account-name $AZURE_STORAGE_ACCOUNT --container-name $AZURE_STORAGE_CONTAINER --file ./src/main/python/resources/${variables_json} --name ${variables_json}

# Import the variables into Data Factory (Azure Data Factory doesn't support direct variable import like Composer, hence consider using a different approach such as Linked Services, Datasets, or Pipelines)

# Upload DAG files to Azure Data Factory
for file in ./src/main/python/dags/*.py; do
  echo "Uploading DAG file: $file to Azure Data Factory"
  az storage blob upload --account-name $AZURE_STORAGE_ACCOUNT --container-name $AZURE_STORAGE_CONTAINER --file $file --name dags/$(basename $file)
done

# Triggering the pipeline to ensure DAGs are imported and set up correctly
# Assuming there is a pipeline that handles DAG setup
az datafactory pipeline create --factory-name $DATA_FACTORY_ENV --resource-group $RESOURCE_GROUP --name setup-pipeline --pipeline @pipeline.json
az datafactory pipeline run --factory-name $DATA_FACTORY_ENV --resource-group $RESOURCE_GROUP --name setup-pipeline
