# Kai: The Senior Data Engineer Agent 🏗️

> **"Hello, I am Kai, the senior data engineer bot created by Master Vijay."**

## 📖 Overview

**Kai** is an enterprise-grade AI Agent built using the **Google Cloud Agent Development Kit (ADK)**. He acts as a "Data Steward" for your Google Cloud Platform environment, automating the lifecycle of data engineering tasks while enforcing strict governance, safety, and naming conventions.

Unlike standard chatbots, Kai is **state-aware** and follows rigorous **safety protocols** (the "Grim Reaper" module) to prevent accidental data loss during deletions or updates.

---

## 🚀 Key Features

### 1. 🔍 Discovery & Navigation
* **File Scout:** Instantly list and filter CSV files in your GCS Landing Zone.
* **Metadata Explorer:** View available Datasets and Tables in your project without leaving the chat.
* **Smart Analysis:** Reads only the first 2KB of any file to extract headers instantly, even for multi-gigabyte files.

### 2. 🧠 Intelligent Schema Design
* **Auto-Typing:** Automatically maps CSV columns to `STRING` (Raw Layer standard).
* **Convention Enforcement:** Converts all column names to `snake_case`.
* **Mandatory Audit Columns:** Automatically appends `batch_date` and `db_prcsd_dttm` to every table.
* **Optimization Advisor:**
    * Detects `DATE`/`TIMESTAMP` → Suggests **Partitioning** & Expiration Policies.
    * Detects High Cardinality IDs → Suggests **Clustering**.

### 3. 🛡️ Lifecycle Management (CRUD)
* **Safe Updates:** Handles schema changes by creating a temporary backup, recreating the table, and restoring data automatically.
* **Dataset Management:** Can create new datasets (US Multi-region) and delete existing ones safely.
* **Infrastructure-as-Code (IaC) Mode:**
    * Option to **Generate Artifacts** instead of executing changes.
    * Produces valid `JSON` Schema and `DDL` SQL files in GCS for Terraform pipelines.

### 4. 💀 The "Grim Reaper" Safety Protocols
Kai implements "Nuclear Safety" logic for destructive actions:
* **Strict Confirmation:** Requires users to type specific phrases (e.g., *"I confirm nuclear deletion..."*).
* **Mandatory GCS Fallback:** Even if a user declines a standard backup, Kai **forces** a CSV export to GCS before dropping any table.
* **Tombstones:** Before deleting a dataset, Kai saves its definition (JSON) to GCS as a permanent record.

---

## 🛠️ Technical Architecture

* **Brain:** Gemini 2.0 Flash (via Vertex AI)
* **Framework:** Google Cloud ADK (Python)
* **Storage:** Google Cloud Storage (Artifacts & Backups)
* **Compute:** BigQuery (Serverless SQL Execution)

### Folder Structure
```text
data-architect-agent/
├── .env                    # Secrets & Config
├── raw_architect/          # The Agent Package
│   ├── __init__.py         # Package Expo
│   ├── agent.py            # The Brain (Logic & Persona)
│   └── tools.py            # The Hands (Pure Python Functions)
└── README.md               # You are here
```

-----

## ⚙️ Setup & Installation

### Prerequisites

1.  **Google Cloud Project:** An active GCP project with Billing enabled.
2.  **Environment:** Python 3.10+ installed.
3.  **Permissions:** A Service Account (or your user account) with the following IAM roles:
      * `BigQuery Admin` (To create datasets/tables)
      * `Storage Object Admin` (To read/write GCS artifacts)
      * `Vertex AI User` (To invoke the Gemini model)

### 1\. Installation

Run the following commands in your terminal to set up the environment:

```bash
# 1. Create a project directory
mkdir data-architect-agent
cd data-architect-agent

# 2. Create and activate a Python virtual environment
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 3. Install required libraries
# We install the Agent Development Kit and Google Cloud client libraries
pip install google-adk google-cloud-bigquery google-cloud-storage pandas
```

### 2\. Configuration

Create a `.env` file in the root directory (`data-architect-agent/.env`) to store your configuration. This keeps your secrets safe.

```env
# Google Cloud Configuration
GOOGLE_CLOUD_PROJECT="your-project-id-here"
GOOGLE_APPLICATION_CREDENTIALS="path/to/your/service-account-key.json" # Optional if using gcloud auth

# Agent Configuration
GCS_BUCKET_NAME="data_architect"  # The bucket you created manually
LANDING_FOLDER="landing/"         # The folder for raw CSVs
```

### 3\. Running the Agent

Once configured, you can launch the **Kai Agent** using the ADK web interface. This starts a local server for testing.

```bash
# Ensure you are authenticated (if not using service account key)
gcloud auth application-default login

# Launch the interactive web chat
adk web raw_architect.agent
```

  * **Access the UI:** Open your browser and navigate to `http://localhost:8080`.
  * **Verify Identity:** Type "Who are you?" in the chat.
      * *Expected Response:* "Hello, I am Kai, the senior data engineer bot created by master Vijay."

-----

## 🎮 Usage Scenarios

### Scenario A: Onboarding a New File

> **User:** "Process `sales_data.csv`."
>
> **Kai:**
>
> 1.  Analyzes headers.
> 2.  Notices a `txn_date` column → Suggests Partitioning.
> 3.  Generates DDL with `snake_case` + Audit Columns.
> 4.  Asks for Approval.
> 5.  **Action:** Creates `raw_zone.sales_raw` in BigQuery.

### Scenario B: Infrastructure-as-Code (Terraform)

> **User:** "Process `inventory.csv` but don't run it. Generate artifacts."
>
> **Kai:**
>
> 1.  Designs the schema.
> 2.  **Action:** Writes two files to GCS:
>       * `gs://.../table_generation/DDL/inventory_raw.sql`
>       * `gs://.../table_generation/json/inventory_raw.json`

### Scenario C: Safe Table Deletion

> **User:** "Delete table `sales_raw`."
>
> **Kai:**
>
> 1.  Demands: *"Type 'I confirm deletion of sales\_raw'."*
> 2.  Asks: *"Want a BQ Snapshot?"* (User says No).
> 3.  **Action:** *"Initiating Mandatory GCS Export..."*
> 4.  Exports data to `gs://.../backup/bq_table/...`
> 5.  **Action:** Drops the table only after export succeeds.

### Scenario D: Dataset Nuclear Deletion

> **User:** "Delete dataset `legacy_data`."
>
> **Kai:**
>
> 1.  Checks contents. *"Warning: Contains 5 tables."*
> 2.  Demands: *"Type 'I confirm nuclear deletion of dataset legacy\_data'."*
> 3.  **Action:** Generates a "Tombstone" JSON of the dataset metadata to GCS.
> 4.  **Action:** Wipes the dataset and all contents.

-----

## 📂 GCS Artifact Paths

Kai organizes outputs automatically in your bucket:

| Path Prefix | Purpose |
| :--- | :--- |
| `landing/` | Input folder for raw CSVs. |
| `table_generation/DDL/` | SQL Create statements for tables. |
| `table_generation/json/` | JSON Schema definitions for tables. |
| `dataset_generation/json/` | JSON definitions for Datasets. |
| `backup/bq_table/` | Mandatory CSV exports before deletion. |

-----

## 👨‍💻 Maintainers

  * **Creator:** Master Vijay
  * **Agent Identity:** Kai (Senior Data Engineer)

<!-- end list -->

```
```