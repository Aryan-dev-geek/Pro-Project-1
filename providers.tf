terraform {
  required_version = ">= 1.0.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
  # Terraform automatically picks up AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, 
  # AZURE_TENANT_ID, and AZURE_SUBSCRIPTION_ID from the environment!
}
