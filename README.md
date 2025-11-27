# Azure Airflow Kubernetes Deployment

This project deploys Apache Airflow to Azure Kubernetes Service (AKS) with custom OCR capabilities and Azure Blob Storage integration for logs.

## Prerequisites

Before you begin, ensure you have the following installed and configured:

- **Azure CLI** - [Installation guide](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli)
- **Terraform** (>= 1.0) - [Installation guide](https://www.terraform.io/downloads)
- **kubectl** - [Installation guide](https://kubernetes.io/docs/tasks/tools/)
- **Docker** - [Installation guide](https://docs.docker.com/get-docker/)
- **Helm** (>= 3.0) - [Installation guide](https://helm.sh/docs/intro/install/)
- **Azure subscription** with appropriate permissions
- **Azure Container Registry (ACR)** - You'll need an existing ACR or create one

## Architecture Overview

- **Kubernetes Cluster**: AKS cluster with blob storage driver enabled
- **Container Registry**: Azure Container Registry for storing custom Airflow images
- **Storage**: Azure Blob Storage for Airflow logs with lifecycle management
- **Executor**: KubernetesExecutor for dynamic task execution
- **DAGs**: Synced from Git repository via git-sync
- **Database**: PostgreSQL with PgBouncer connection pooling

## Step-by-Step Deployment Guide

### Step 1: Configure Terraform Variables

1. Navigate to the terraform directory:
   ```bash
   cd terraform
   ```

2. Update `terraform.tfvars` with your values:
   ```hcl
   rg_name  = "your-resource-group-name"
   app_name = "your-app-name"
   location = "westus"  # or your preferred Azure region
   ```

3. Update `main.tf` with your Azure subscription details:
   - Update `tenant_id` in the provider block
   - Update `subscription_id` in the provider block
   - Update the ACR name and resource group in the `data.azurerm_container_registry` block

### Step 2: Provision Azure Infrastructure

1. Initialize Terraform:
   ```bash
   terraform init
   ```

2. Review the planned changes:
   ```bash
   terraform plan
   ```

3. Apply the Terraform configuration to create:
   - Resource Group
   - AKS Cluster
   - Storage Account and Container for logs
   - Role assignments for ACR access
   ```bash
   terraform apply
   ```

4. After successful deployment, note the following outputs:
   - AKS cluster name
   - Storage account name
   - Resource group name

### Step 3: Configure kubectl for AKS

1. Get AKS credentials:
   ```bash
   az aks get-credentials --resource-group <your-rg-name>-rg --name <your-rg-name>-<your-app-name>-aks
   ```

2. Verify connection:
   ```bash
   kubectl get nodes
   ```

### Step 4: Verify Azure Blob Storage CSI Driver

The Azure Blob Storage CSI driver is integrated directly into AKS. Since your Terraform configuration includes `blob_driver_enabled = true` in the storage profile, the driver should already be enabled on your cluster.

1. Verify the blob driver is enabled:
   ```bash
   az aks show --resource-group <your-rg-name>-rg --name <your-rg-name>-<your-app-name>-aks \
     --query "storageProfile.blobCsiDriver.enabled" -o tsv
   ```

   This should return `true`.

2. If the driver is not enabled (returns `false` or empty), enable it:
   ```bash
   az aks update \
     --resource-group <your-rg-name>-rg \
     --name <your-rg-name>-<your-app-name>-aks \
     --enable-blob-driver
   ```

3. Verify the driver pods are running:
   ```bash
   kubectl get pods -n kube-system | grep blob
   ```

   You should see pods like `blob-csi-driver-node-*` running.

**Note:** If you're using an existing AKS cluster that wasn't created with Terraform, you'll need to enable the blob driver using the `az aks update` command above.

### Step 5: Create Storage Account Credentials Secret

1. Get the storage account key:
   ```bash
   STORAGE_ACCOUNT_NAME="<your-app-name>airflowsa"
   RESOURCE_GROUP="<your-rg-name>-rg"
   STORAGE_KEY=$(az storage account keys list --resource-group $RESOURCE_GROUP --account-name $STORAGE_ACCOUNT_NAME --query "[0].value" -o tsv)
   ```

2. Create the Kubernetes secret for storage account credentials:
   ```bash
   kubectl create namespace airflow
   kubectl create secret generic storage-account-credentials \
     --from-literal=azurestorageaccountname=$STORAGE_ACCOUNT_NAME \
     --from-literal=azurestorageaccountkey=$STORAGE_KEY \
     --namespace airflow
   ```

### Step 6: Update Persistent Volume Configuration

1. Edit `airflow/pv-logs.yaml` and update the following values:
   - `resourceGroup`: Your resource group name
   - `storageAccount`: Your storage account name (from Step 2)
   - `containerName`: Should be `airflow-logs`

2. Apply the PersistentVolume:
   ```bash
   kubectl apply -f airflow/pv-logs.yaml
   ```

3. Apply the PersistentVolumeClaim:
   ```bash
   kubectl apply -f airflow/pvc-logs.yaml
   ```

4. Verify the PV and PVC are bound:
   ```bash
   kubectl get pv
   kubectl get pvc -n airflow
   ```

### Step 7: Build and Push Custom Airflow Docker Image

1. Navigate to the project root:
   ```bash
   cd ..
   ```

2. Log in to Azure Container Registry:
   ```bash
   az acr login --name <your-acr-name>
   ```

3. Build the Docker image:
   ```bash
   docker build -t <your-acr-name>.azurecr.io/airflow-custom:latest .
   ```

   Or with a specific version tag:
   ```bash
   docker build -t <your-acr-name>.azurecr.io/airflow-custom:3.1.2 .
   ```

4. Push the image to ACR:
   ```bash
   docker push <your-acr-name>.azurecr.io/airflow-custom:latest
   ```

   Or if using a version tag:
   ```bash
   docker push <your-acr-name>.azurecr.io/airflow-custom:3.1.2
   ```

### Step 8: Create Git SSH Secret for DAG Sync

1. Generate an SSH key pair (if you don't have one):
   ```bash
   ssh-keygen -t rsa -b 4096 -C "airflow-git-sync" -f airflow-git-key
   ```

2. Add the public key to your GitHub repository as a deploy key:
   - Go to your GitHub repository
   - Settings → Deploy keys → Add deploy key
   - Paste the contents of `airflow-git-key.pub`
   - Enable "Allow write access" if needed

3. Create the Kubernetes secret with the private key:
   ```bash
   kubectl create secret generic airflow-git-ssh-secret \
     --from-file=gitSshKey=airflow-git-key \
     --namespace airflow
   ```

4. (Optional) Clean up the local key files:
   ```bash
   rm airflow-git-key airflow-git-key.pub
   ```

### Step 9: Update Airflow Helm Values

1. Edit `airflow/values.yaml` and add/update the following:

   - **Image**: Add image configuration to use your custom image (add this section if not present):
     ```yaml
     images:
       airflow:
         repository: <your-acr-name>.azurecr.io/airflow-custom
         tag: latest  # or your version tag
         pullPolicy: Always
     ```

   - **Git Repository**: Update the `dags.gitSync.repo` if using a different repository:
     ```yaml
     dags:
       gitSync:
         repo: git@github.com:your-username/your-repo.git
     ```

   - **Storage Account**: Verify the storage account name matches your deployment

2. Review other configurations:
   - Node selector
   - Executor type (KubernetesExecutor)
   - Environment variables
   - PostgreSQL and PgBouncer settings

### Step 10: Add Apache Airflow Helm Repository

1. Add the official Airflow Helm chart repository:
   ```bash
   helm repo add apache-airflow https://airflow.apache.org
   helm repo update
   ```

### Step 11: Deploy Airflow with Helm

1. Install Airflow using Helm:
   ```bash
   helm install airflow apache-airflow/airflow \
     --namespace airflow \
     --create-namespace \
     --values airflow/values.yaml \
     --version 1.13.0  # Use the latest compatible version
   ```

2. Monitor the deployment:
   ```bash
   kubectl get pods -n airflow -w
   ```

3. Wait for all pods to be in `Running` state:
   ```bash
   kubectl get pods -n airflow
   ```

### Step 12: Access Airflow Web UI

**Note:** In Airflow 3.0+, the traditional webserver is replaced by the API server, which serves the web UI.

1. Port-forward to access the Airflow web UI:
   ```bash
   kubectl port-forward svc/airflow-api-server 8080:8080 -n airflow
   ```

2. Open your browser and navigate to:
   ```
   http://localhost:8080
   ```

3. Default credentials:
   - Username: `admin`
   - Password: `admin` (default for Airflow 3.0+)
   
   To get the Fernet key (if needed):
   ```bash
   kubectl get secret airflow-fernet-key -n airflow -o jsonpath="{.data.fernet-key}" | base64 -d
   ```

### Step 13: Trigger OCR DAGs via API

You can trigger the OCR DAGs using the Airflow REST API. First, ensure you have port-forwarding active:

```bash
kubectl port-forward svc/airflow-api-server 8080:8080 -n airflow
```

#### Trigger `document-ocr-v4` DAG

This DAG processes a single document. It expects `file_path` and `file_name` to be passed via XCom (typically set by an API trigger).

**Note:** Airflow 3.0+ uses REST API v2. The v1 API has been removed.

```bash
curl -X POST "http://localhost:8080/api/v2/dags/document-ocr-v4/dagRuns" \
  -H "Content-Type: application/json" \
  -u admin:admin \
  -d '{
    "dag_run_id": "manual_trigger_'$(date +%s)'",
    "conf": {
      "file_path": "/path/to/your/document.pdf",
      "file_name": "document.pdf"
    }
  }'
```

#### Trigger `document_ocr_folder_v12` DAG

This DAG processes all documents in an Azure Blob Storage folder. It requires `container` and `folder_path` in the configuration.

```bash
curl -X POST "http://localhost:8080/api/v2/dags/document_ocr_folder_v12/dagRuns" \
  -H "Content-Type: application/json" \
  -u admin:admin \
  -d '{
    "dag_run_id": "manual_trigger_'$(date +%s)'",
    "conf": {
      "container": "your-container-name",
      "folder_path": "incoming/batch1/",
      "allowed_extensions": [".pdf", ".png", ".jpg"]
    }
  }'
```

**Note:** 
- Replace `your-container-name` with your actual Azure Blob Storage container name
- Replace `incoming/batch1/` with the actual folder path in your container
- The `allowed_extensions` field is optional; if not provided, it defaults to `['.pdf', '.png', '.jpg', '.jpeg', '.tiff', '.bmp']`
- Ensure the `AZURE_BLOB_CONNECTION_STRING` Airflow Variable is set before running the folder DAG

#### Check DAG Run Status

To check the status of a DAG run:

```bash
# Get all runs for a DAG
curl -X GET "http://localhost:8080/api/v2/dags/document-ocr-v4/dagRuns" \
  -u admin:admin

# Get a specific DAG run (replace {dag_run_id} with actual run ID)
curl -X GET "http://localhost:8080/api/v2/dags/document-ocr-v4/dagRuns/{dag_run_id}" \
  -u admin:admin
```

#### Set Azure Blob Connection String Variable

Before running the folder DAG, set the Azure Blob Storage connection string:

```bash
# Create or update the variable
curl -X POST "http://localhost:8080/api/v2/variables" \
  -H "Content-Type: application/json" \
  -u admin:admin \
  -d '{
    "key": "AZURE_BLOB_CONNECTION_STRING",
    "value": "DefaultEndpointsProtocol=https;AccountName=YOUR_ACCOUNT_NAME;AccountKey=YOUR_ACCOUNT_KEY;EndpointSuffix=core.windows.net"
  }'
```

Or update it if it already exists:

```bash
curl -X PATCH "http://localhost:8080/api/v2/variables/AZURE_BLOB_CONNECTION_STRING" \
  -H "Content-Type: application/json" \
  -u admin:admin \
  -d '{
    "value": "DefaultEndpointsProtocol=https;AccountName=YOUR_ACCOUNT_NAME;AccountKey=YOUR_ACCOUNT_KEY;EndpointSuffix=core.windows.net"
  }'
```

**Note:** Replace `YOUR_ACCOUNT_NAME` and `YOUR_ACCOUNT_KEY` with your actual Azure Storage Account credentials. You can get the connection string from the Azure Portal or using Azure CLI:
```bash
az storage account show-connection-string --name <storage-account-name> --resource-group <resource-group-name>
```

### Step 14: Verify Deployment

1. Check all Airflow components:
   ```bash
   kubectl get all -n airflow
   ```

2. Check PersistentVolumeClaims:
   ```bash
   kubectl get pvc -n airflow
   ```

3. Check logs for any issues:
   ```bash
   kubectl logs -n airflow -l component=scheduler --tail=50
   kubectl logs -n airflow -l component=api-server --tail=50
   ```

4. Verify DAGs are synced:
   - Log into the Airflow UI
   - Check if your DAGs from the `dags/` directory appear in the UI

## Troubleshooting

### Pods Not Starting

1. Check pod status:
   ```bash
   kubectl describe pod <pod-name> -n airflow
   ```

2. Check pod logs:
   ```bash
   kubectl logs <pod-name> -n airflow
   ```

### PersistentVolume Not Binding

1. Verify the storage account credentials secret:
   ```bash
   kubectl get secret storage-account-credentials -n airflow
   ```

2. Check PV and PVC status:
   ```bash
   kubectl get pv
   kubectl describe pvc pvc-airflow-logs -n airflow
   ```

### Git Sync Issues

1. Check git-sync pod logs:
   ```bash
   kubectl logs -n airflow -l component=dags --tail=50
   ```

2. Verify SSH secret:
   ```bash
   kubectl get secret airflow-git-ssh-secret -n airflow
   ```

### Image Pull Errors

1. Verify ACR access:
   ```bash
   az acr repository list --name <your-acr-name>
   ```

2. Check AKS has ACR pull permissions (configured in Terraform)

### Access Airflow UI Externally

To expose Airflow UI via LoadBalancer:

1. Edit the API server service (which serves the web UI in Airflow 3.0+):
   ```bash
   kubectl edit svc airflow-api-server -n airflow
   ```

2. Change `type: ClusterIP` to `type: LoadBalancer`

3. Get the external IP:
   ```bash
   kubectl get svc airflow-api-server -n airflow
   ```

## Updating Airflow

### Update Custom Image

1. Make changes to `Dockerfile` or `requirements.txt`
2. Rebuild and push the image:
   ```bash
   docker build -t <your-acr-name>.azurecr.io/airflow-custom:latest .
   docker push <your-acr-name>.azurecr.io/airflow-custom:latest
   ```
3. Restart Airflow pods to pull the new image:
   ```bash
   kubectl rollout restart deployment -n airflow
   ```

### Update Helm Chart

1. Update Helm values in `airflow/values.yaml`
2. Upgrade the Helm release:
   ```bash
   helm upgrade airflow apache-airflow/airflow \
     --namespace airflow \
     --values airflow/values.yaml
   ```

## Cleanup

To remove all resources:

1. Uninstall Airflow:
   ```bash
   helm uninstall airflow -n airflow
   ```

2. Delete Kubernetes resources:
   ```bash
   kubectl delete pvc pvc-airflow-logs -n airflow
   kubectl delete pv pv-airflow-logs
   kubectl delete namespace airflow
   ```

3. Destroy Terraform infrastructure:
   ```bash
   cd terraform
   terraform destroy
   ```

## Additional Notes

- **Storage Account**: Logs are automatically pruned after 7 days via lifecycle policy
- **Git Sync**: DAGs are automatically synced from the configured Git repository
- **Custom Image**: Includes Tesseract OCR and Python dependencies for OCR tasks
- **PostgreSQL**: Uses the built-in PostgreSQL chart (not recommended for production)
- **PgBouncer**: Enabled for connection pooling to PostgreSQL

## References

- [Apache Airflow Helm Chart](https://airflow.apache.org/docs/helm-chart/stable/index.html)
- [Azure Blob Storage CSI Driver](https://github.com/kubernetes-sigs/blob-csi-driver)
- [Azure Kubernetes Service Documentation](https://docs.microsoft.com/en-us/azure/aks/)

