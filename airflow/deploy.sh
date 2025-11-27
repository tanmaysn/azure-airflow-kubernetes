n=airflow
rg=wus-d-1-rg
cluster=wus-d-1-aks
STORAGE_ACCOUNT_NAME=testairflowsa
STORAGE_KEY=$(az storage account keys list -g $rg --account-name $STORAGE_ACCOUNT_NAME --query "[0].value" -o tsv)


kubectl create namespace airflow
echo "namespace created"


kubectl create secret generic storage-account-credentials \
     --from-literal=azurestorageaccountname=$STORAGE_ACCOUNT_NAME \
     --from-literal=azurestorageaccountkey=$STORAGE_KEY \
     --namespace $n
echo "storage account credentials created"

kubectl apply -f pv-logs.yaml -n $n
echo "pv created"

kubectl apply -f pvc-logs.yaml -n $n
echo "pvc created"

