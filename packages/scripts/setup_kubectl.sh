#!/bin/bash
source scripts/set_env.sh

# Get AKS credentials
az aks get-credentials --resource-group ${RESOURCE_GROUP} --name ${AKS_CLUSTER_NAME}

# Verify that kubectl is configured to use the AKS cluster
kubectl get nodes
