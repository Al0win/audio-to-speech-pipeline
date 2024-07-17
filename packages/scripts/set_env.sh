# Azure-specific environment variables
export RESOURCE_GROUP="myResourceGroup"  # Replace with your Azure resource group
export AKS_CLUSTER_NAME="myAksCluster"   # Replace with your AKS cluster name
export LOCATION="eastus"                 # Azure region where your resources are located
export ZONE="eastus"                     # Azure region (zones aren't typically specified in the same way as GCP)
export COMPOSER_ENV_NAME="ekstepcomposer-test"  # Azure equivalent service/environment name
export AZURE_PROJECT="ekstepspeechrecognition"  # Name of your project (if applicable)
export DB_REGION="eastus"                # Azure region for your database
export DB_NAME="crowdsourcedb"           # Database name (if applicable)
