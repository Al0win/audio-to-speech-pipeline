#!/usr/bin/env bash

# Azure-specific settings
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
az sql db create --resource-group $RESOURCE_GROUP --server $AZURE_SQL_SERVER --name $DB_NAME --service-objective S0

# Configure the Azure SQL Database firewall to allow access
az sql server firewall-rule create --resource-group $RESOURCE_GROUP --server $AZURE_SQL_SERVER --name AllowYourIp --start-ip-address $YOUR_IP --end-ip-address $YOUR_IP

# Export Azure credentials
export AZURE_APPLICATION_CREDENTIALS=${HOME}/$AZURE_AUTH

echo $AZURE_APPLICATION_CREDENTIALS

# Example command to connect to Azure SQL Database using sqlcmd (make sure to install sqlcmd tool)
# sqlcmd -S tcp:${AZURE_SQL_SERVER}.database.windows.net,1433 -d $DB_NAME -U $AZURE_SQL_ADMIN_USER -P $AZURE_SQL_ADMIN_PASSWORD -G
