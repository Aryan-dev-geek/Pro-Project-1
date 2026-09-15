resource "azurerm_resource_group" "rg" {
  name     = "rg-proproject1-dev-v3"
  location = "East US"
}

resource "azurerm_storage_account" "storage" {
  name                     = "proprojectstoragev3123"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_databricks_workspace" "workspace" {
  name                        = "db-proproject1-dev-v3"
  resource_group_name         = azurerm_resource_group.rg.name
  location                    = azurerm_resource_group.rg.location
  sku                         = "premium"
  managed_resource_group_name = "rg-proproject1-db-managed-v3"
}
