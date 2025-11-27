#!/bin/bash
# Script to fix airflow_local_settings.py in ConfigMap after Helm install
# This is needed because the default Helm chart has incompatible imports for Airflow 3.1.2

NAMESPACE=${1:-airflow}
CONFIGMAP_NAME="airflow-config"

echo "Waiting for ConfigMap $CONFIGMAP_NAME to be created in namespace $NAMESPACE..."

# Wait for ConfigMap to be created (max 2 minutes)
for i in {1..24}; do
  if kubectl get configmap $CONFIGMAP_NAME -n $NAMESPACE &>/dev/null; then
    echo "ConfigMap found! Patching airflow_local_settings.py..."
    kubectl patch configmap $CONFIGMAP_NAME -n $NAMESPACE --type merge -p '{"data":{"airflow_local_settings.py":"# Minimal local settings for Airflow 3.1.2 compatibility\npass\n"}}'
    echo "ConfigMap patched successfully!"
    exit 0
  fi
  echo "Waiting for ConfigMap... ($i/24)"
  sleep 5
done

echo "ERROR: ConfigMap not found after 2 minutes. Please check Helm installation."
exit 1

