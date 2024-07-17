#!/usr/bin/env bash

AZURE_REGION=eastus  # Set your Azure region
DB_NAME=crowdsourcedb
AZURE_AUTH=azure-auth-file.json  # Path to your Azure service principal credentials

# Install Azure CLI
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# Authenticate using service principal credentials
az login --service-principal -u $AZURE_CLIENT_ID -p $AZURE_CLIENT_SECRET --tenant $AZURE_TENANT_ID

# Set the Azure subscription
az account set --subscription $AZURE_SUBSCRIPTION_ID

# Create an Azure SQL Database
az sql db create --resource-group $AZURE_RESOURCE_GROUP --server $AZURE_SQL_SERVER --name $DB_NAME --service-objective S0

# Configure the Azure SQL Database firewall to allow access
az sql server firewall-rule create --resource-group $AZURE_RESOURCE_GROUP --server $AZURE_SQL_SERVER --name AllowYourIp --start-ip-address YOUR_IP --end-ip-address YOUR_IP

# Download and configure the Azure SQL proxy
wget https://github.com/GoogleCloudPlatform/cloudsql-proxy/releases/download/v1.28.0/cloud_sql_proxy.linux.amd64 -O cloud_sql_proxy
chmod +x cloud_sql_proxy
nohup ./cloud_sql_proxy -dir=./cloudsql -instances=${AZURE_SQL_SERVER}:${AZURE_REGION}:${DB_NAME}=tcp:5432 &
sleep 25s
cat nohup.out

# Export Azure credentials
export AZURE_APPLICATION_CREDENTIALS=${HOME}/$AZURE_AUTH

echo $AZURE_APPLICATION_CREDENTIALS
