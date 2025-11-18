terraform {
  required_providers {
    azapi = {
      source  = "azure/azapi"
      version = "~>1.5"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~>3.116.0"
    }
  }
}

provider "azurerm" {
  tenant_id                  = "2a9f86a9-29e7-44bd-8863-849373d53db8" # Epiq
  subscription_id            = "57a7f8d5-cea7-483a-8dda-30af42054d91" # Epiq AI Prod
  skip_provider_registration = false
  features {}
}

resource "azurerm_resource_group" "rg" {
  name     = "${var.rg_name}-rg"
  location = var.location
}

# Azure Kubernetes Cluster

resource "azurerm_kubernetes_cluster" "main" {
  name                = "${var.rg_name}-${var.app_name}-aks"
  location            = var.location
  resource_group_name = azurerm_resource_group.rg.name
  dns_prefix          = "${var.app_name}-aks"

  default_node_pool {
    name       = "default"
    node_count = 1
    vm_size    = "Standard_DS2_v2"
  }

  identity {
    type = "SystemAssigned"
  }

  storage_profile {
    blob_driver_enabled = true
  }
}

# Azure Container Registry

data "azurerm_container_registry" "nonprodacr" {
  name                = "eaicrnonprod"
  resource_group_name = "eai-u-eus-aiservice-1"  
}

resource "azurerm_role_assignment" "main" {
  principal_id         = azurerm_kubernetes_cluster.main.kubelet_identity[0].object_id
  role_definition_name = "AcrPull"
  scope                = data.azurerm_container_registry.nonprodacr.id
}

# Blob storage for Airflow logs

resource "azurerm_storage_account" "airflow" {
  name                     = "${var.app_name}airflowsa"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_storage_container" "airflow_logs" {
  name                  = "airflow-logs"
  storage_account_name  = azurerm_storage_account.airflow.name
  container_access_type = "private"
}

resource "azurerm_storage_management_policy" "prune_logs" {
  storage_account_id = azurerm_storage_account.airflow.id

  rule {
    name    = "prune-logs"
    enabled = true
    filters {
      prefix_match = ["airflow-logs"]
      blob_types   = ["blockBlob"]
    }
    actions {
      base_blob {
        delete_after_days_since_modification_greater_than = 7
      }
    }
  }
}