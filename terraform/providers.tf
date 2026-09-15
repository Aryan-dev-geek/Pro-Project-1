terraform {
  required_version = ">= 1.0.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    databricks = {
      source  = "databrickslabs/databricks"
      version = "~> 1.0"
    }
  }
}

provider "azurerm" {
  features {}
}
