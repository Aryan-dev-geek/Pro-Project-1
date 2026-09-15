resource "azurerm_resource_group" "rg" {
  name     = "rg-proproject1-dev-v2"
  location = "East US"
}

resource "azurerm_storage_account" "storage" {
  name                     = "proprojectstoragev2123" # Must be globally unique, lowercase letters and numbers only
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_databricks_workspace" "workspace" {
  name                        = "db-proproject1-dev-v2"
  resource_group_name         = azurerm_resource_group.rg.name
  location                    = azurerm_resource_group.rg.location
  sku                         = "standard"
  managed_resource_group_name = "rg-proproject1-db-managed-v2"
}
