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
  tenant_id                  = "9e04cb4f-2d66-44bc-9166-d2bc015aee9e" # Epiq
  subscription_id            = "b30a3be3-de2b-4fc8-9757-33a9971e184d" # PAYG01
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
    node_count = 2
    vm_size    = "Standard_D4s_v3"
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
  name                = "tanmaysn"
  resource_group_name = "tanmaysn-rg"  
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