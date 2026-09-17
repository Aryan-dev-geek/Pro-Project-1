# 🚀 End-to-End Automated Crypto Data Pipeline & Dashboard

### ☁️ Azure • Databricks • PySpark • Delta Lake • Terraform • GitHub Actions

> **A cloud-native, production-oriented data engineering platform that ingests live cryptocurrency market data, transforms it through a Medallion Architecture, and delivers analytics through interactive Databricks Lakeview Dashboards.**

<br>

<p align="center">

**CoinGecko API** → **Bronze** → **Silver** → **Gold** → **Lakeview Dashboard**

</p>

---

## 📊 Project Overview

This project demonstrates a complete **end-to-end cloud data engineering workflow**, from API ingestion to executive-level analytics.

The platform automatically:

- 🌐 Ingests live cryptocurrency market data from the **CoinGecko API**
- 🥉 Stores raw data in the **Bronze** layer
- 🥈 Cleans and standardizes data in the **Silver** layer
- 🥇 Generates business-ready analytics in the **Gold** layer
- ⚙️ Orchestrates pipelines using **Databricks Asset Bundles**
- ☁️ Provisions Azure infrastructure using **Terraform**
- 🔄 Automates deployment through **GitHub Actions**
- 📈 Visualizes market intelligence using **Databricks Lakeview Dashboards**

---

# 🏗️ Architecture

```text
                         ┌──────────────────────┐
                         │   CoinGecko API 🌐   │
                         └──────────┬───────────┘
                                    │
                                    ▼
                    ┌─────────────────────────────┐
                    │       🥉 BRONZE             │
                    │      Raw Delta Data         │
                    │                             │
                    │  ingest_coingecko.py        │
                    └──────────────┬──────────────┘
                                   │
                                   ▼
                    ┌─────────────────────────────┐
                    │       🥈 SILVER             │
                    │    Cleaned & Standardized   │
                    │                             │
                    │  clean_crypto.py            │
                    └──────────────┬──────────────┘
                                   │
                                   ▼
                    ┌─────────────────────────────┐
                    │        🥇 GOLD              │
                    │   Business-Ready Analytics  │
                    │                             │
                    │  aggregate_crypto.py        │
                    └──────────────┬──────────────┘
                                   │
                                   ▼
                    ┌─────────────────────────────┐
                    │      📊 LAKEVIEW            │
                    │        DASHBOARD            │
                    │                             │
                    │ Market Trends • Gainers     │
                    │ Asset Performance • KPIs    │
                    └─────────────────────────────┘
```

---

# 🛠️ Technology Stack

| Layer | Technology |
|---|---|
| ☁️ **Cloud** | Microsoft Azure |
| 🗄️ **Storage** | Azure Data Lake Storage Gen2 |
| ⚡ **Processing** | Apache Spark / PySpark |
| 🧱 **Storage Format** | Delta Lake |
| 🏛️ **Governance** | Unity Catalog |
| 🔧 **Infrastructure** | Terraform |
| 🔄 **Orchestration** | Databricks Asset Bundles |
| 🚀 **CI/CD** | GitHub Actions |
| 📊 **Visualization** | Databricks Lakeview |
| 🌐 **Data Source** | CoinGecko Public API |
| 🗃️ **Analytics** | Databricks SQL |

---

# 🔄 Data Engineering Pipeline

## 1️⃣ Data Ingestion — Bronze

**File:** `src/bronze/ingest_coingecko.py`

The ingestion layer connects to the CoinGecko `/coins/markets` endpoint and retrieves live cryptocurrency market information.

### Responsibilities

- Connect to the public API
- Retrieve market data
- Preserve source information
- Create raw Delta data
- Provide the foundation for downstream processing

```text
CoinGecko API
      │
      ▼
Python Requests
      │
      ▼
Raw JSON / Records
      │
      ▼
Bronze Delta Table
```

---

## 2️⃣ Data Cleaning — Silver

**File:** `src/silver/clean_crypto.py`

The Silver layer converts raw API data into a reliable analytical dataset.

### Transformations

- 🧹 Handle null values
- 🔁 Remove duplicate records
- 🔢 Cast numeric columns
- 🕒 Standardize timestamps
- 🧱 Normalize the schema
- 💾 Write cleaned data to Delta

```text
Bronze
  │
  ├── Null Handling
  ├── Deduplication
  ├── Type Casting
  └── Timestamp Standardization
          │
          ▼
       Silver
```

---

## 3️⃣ Business Analytics — Gold

**File:** `src/gold/aggregate_crypto.py`

The Gold layer transforms cleaned data into analytics-ready datasets optimized for dashboards and business analysis.

### 📈 Gold Tables

| Table | Purpose |
|---|---|
| `gold_crypto_global_summary` | High-level cryptocurrency market metrics |
| `gold_crypto_asset_trends` | Historical asset-level performance |
| `gold_crypto_top_gainers` | Dynamically ranked top-performing assets |

### ⚡ Spark Window Functions

The top-gainer analysis uses Spark window functions to dynamically rank cryptocurrency assets based on market performance.

```text
Silver Data
     │
     ▼
Aggregation
     │
     ├───────────────┐
     │               │
     ▼               ▼
Global Summary   Asset Trends
     │
     ▼
Dynamic Ranking
     │
     ▼
Top Gainers
```

---

# ☁️ Azure Infrastructure

The entire cloud environment is provisioned using **Infrastructure as Code**.

### Terraform provisions:

```text
Azure Subscription
        │
        ▼
┌──────────────────────┐
│   Resource Group     │
└──────────┬───────────┘
           │
     ┌─────┴───────────────┐
     │                     │
     ▼                     ▼
 ADLS Gen2            Databricks
     │                     │
     ├── bronze            ├── Workspace
     ├── silver            └── SQL Warehouse
     └── gold
```

### Why Terraform?

- ♻️ Reproducible infrastructure
- 📦 Infrastructure version control
- ⚙️ Automated provisioning
- 🔄 Consistent environments
- 🚀 Faster deployments

---

# ⚙️ Databricks Asset Bundles

The project uses **Databricks Asset Bundles (DABs)** to package and deploy the Databricks workload.

### Pipeline Dependency

```text
🥉 Bronze
   │
   ▼
🥈 Silver
   │
   ▼
🥇 Gold
```

This ensures that downstream transformations execute only after their required upstream datasets are available.

### Bundle Components

```text
databricks.yml
      │
      ▼
resources/
      │
      └── crypto_pipeline_job.yml
                  │
                  ├── Bronze Job
                  ├── Silver Job
                  └── Gold Job
```

---

# 🔐 CI/CD Automation

GitHub Actions automates the deployment lifecycle.

```text
        Developer
            │
            ▼
       Git Push 🚀
            │
            ▼
     GitHub Actions
            │
      ┌─────┴─────┐
      ▼           ▼
 Terraform     Databricks
 Deployment    Bundle Deploy
      │           │
      └─────┬─────┘
            ▼
       Azure + Databricks
```

### 🔑 Secure Authentication

GitHub repository secrets are used for Azure authentication:

```text
APPID
PASSWORD
SUBSCRIPTION_ID
TENANT
```

These credentials are mapped to the appropriate Terraform `ARM_*` environment variables during deployment.

---

# 📊 Dashboard & Analytics

The Gold-layer datasets power interactive **Databricks Lakeview Dashboards**.

### Dashboard capabilities

📈 **Market Trends**

Track cryptocurrency market movement over time.

🏆 **Top Gainers**

Identify dynamically ranked assets based on market performance.

📊 **Asset Trends**

Explore historical performance across individual cryptocurrencies.

📋 **Market Summary**

View aggregated market-level KPIs and statistics.

---

# 📁 Project Structure

```text
Pro-Project-1/
│
├── .github/
│   └── workflows/
│       ├── terraform.yml
│       └── bundle-deploy.yml
│
├── src/
│   ├── bronze/
│   │   └── ingest_coingecko.py
│   │
│   ├── silver/
│   │   └── clean_crypto.py
│   │
│   └── gold/
│       └── aggregate_crypto.py
│
├── terraform/
│   ├── main.tf
│   └── providers.tf
│
├── resources/
│   ├── job.yml
│   └── crypto_pipeline_job.yml
│
├── databricks.yml
│
└── README.md
```

---

# 🚀 Getting Started

## Prerequisites

Make sure the following tools are installed:

- Terraform CLI
- Azure CLI
- Databricks CLI
- Git
- Access to an Azure subscription
- Access to an Azure Databricks workspace

### Azure Authentication

Authenticate locally using:

```bash
az login
```

Alternatively, configure the required `ARM_*` environment variables.

### Databricks Authentication

Configure the Databricks CLI for your target workspace using your preferred authentication method.

> ⚠️ **Important:** The PySpark scripts under `src/` are designed to run as Databricks Jobs. They expect a Databricks-managed Spark environment and are not intended to be executed directly with a standard local Python interpreter without additional Spark configuration.

---

# 1. Clone the Repository

```bash
git clone https://github.com/Aryan-dev-geek/Pro-Project-1.git
cd Pro-Project-1
```

---

# 2. Provision Azure Infrastructure

```bash
cd terraform

terraform init
terraform plan
terraform apply
```

Terraform provisions the Azure resources defined in the project configuration.

---

# 3. Deploy the Databricks Bundle

Return to the project root:

```bash
cd ..

databricks bundle deploy --target dev
```

---

# 4. Run the Pipeline

After deployment, execute the configured Databricks Job.

The pipeline executes:

```text
Bronze Ingestion
       ↓
Silver Transformation
       ↓
Gold Aggregation
       ↓
Dashboard Analytics
```

---

# 🧠 Engineering Concepts Demonstrated

This project showcases practical experience with:

- 🏛️ Medallion Architecture
- ☁️ Azure Cloud Infrastructure
- 🧱 Data Lakehouse Architecture
- ⚡ PySpark & Apache Spark
- 💾 Delta Lake
- 🔐 Unity Catalog
- 🔧 Terraform / Infrastructure as Code
- ⚙️ Databricks Asset Bundles
- 🔄 GitHub Actions CI/CD
- 🌐 REST API Data Ingestion
- 🪟 Spark Window Functions
- 📊 Databricks SQL
- 📈 Lakeview Dashboard Development

---

# 🎯 Project Outcome

The result is a **reproducible, automated cloud data platform** that transforms live cryptocurrency market data into actionable analytical datasets and interactive dashboards.

```text
        🌐 LIVE DATA
             │
             ▼
      📥 INGESTION
             │
             ▼
       🥉 BRONZE
             │
             ▼
       🥈 SILVER
             │
             ▼
        🥇 GOLD
             │
             ▼
       📊 ANALYTICS
             │
             ▼
      🚀 DASHBOARD
```

The architecture brings together **Azure, Databricks, PySpark, Delta Lake, Terraform, GitHub Actions, Unity Catalog, and Lakeview** into a single end-to-end data engineering solution.

---

## 👨‍💻 Built With

**Azure · Databricks · PySpark · Spark · Delta Lake · Terraform · GitHub Actions · Unity Catalog · Databricks SQL · Lakeview · CoinGecko API**

⭐ **If you find this project useful, consider giving the repository a star!**
