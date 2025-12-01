# Self-Service Fabric Pipeline

A Streamlit-based self-service data pipeline application integrated with Microsoft Fabric for end-to-end data processing workflows.

## Overview

This application enables users to build data pipelines through a web interface, providing capabilities for data ingestion, validation, cleaning, transformation, and loading into Microsoft Fabric Lakehouse. It supports multiple data sources and execution methods.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Streamlit UI Layer                      │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│  │  Login   │→ │  Source  │→ │ Validate │→ │  Clean   │→...   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘       │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    Application Core (app.py)                    │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              PipelineApp (State Manager)                │   │
│  │  - Session state management                             │   │
│  │  - Pipeline configuration                               │   │
│  │  - Lineage tracking                                     │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                      Business Logic Layer                       │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────┐ │
│  │  Data Sources    │  │  Data Operations │  │  Components  │ │
│  │  ├─ Local File   │  │  ├─ Validation   │  │  └─ Lineage  │ │
│  │  ├─ OneLake      │  │  ├─ Cleaning     │  │              │ │
│  │  └─ ADLS         │  │  └─ Transform    │  │              │ │
│  └──────────────────┘  └──────────────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    Integration Layer                            │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────┐ │
│  │  Fabric Auth     │  │  Fabric API      │  │  OneLake     │ │
│  │  - Device Flow   │  │  - Workspaces    │  │  - ADLS Gen2 │ │
│  │  - Token Mgmt    │  │  - Lakehouses    │  │  - Files     │ │
│  │                  │  │  - Notebooks     │  │  - Tables    │ │
│  └──────────────────┘  └──────────────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    Microsoft Fabric                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐    │
│  │  Workspace  │  │  Lakehouse  │  │  Fabric Notebooks   │    │
│  │             │  │  - Tables   │  │  - Data Validation  │    │
│  │             │  │  - Files    │  │  - Data Processing  │    │
│  └─────────────┘  └─────────────┘  └─────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

## Key Features

### Data Sources
- **Local File Upload**: CSV and Excel file support with automatic schema detection
- **OneLake Tables**: Direct connection to existing Fabric Lakehouse tables
- **ADLS Integration**: Azure Data Lake Storage connectivity

### Data Operations
- **Validation**: Rule-based validation using Great Expectations
  - Null checks, type validation, custom expectations
  - Error data segregation
- **Cleaning**: Configurable cleaning operations
  - Remove duplicates
  - Handle null values (remove/fill)
  - Column renaming and dropping
- **Transformation**: Custom transformation logic
- **Lineage Tracking**: Visual lineage graph of pipeline operations

### Execution Methods
- **Fabric Notebook**: Execute operations via Fabric notebooks
- **Local Execution**: Process data locally within the Streamlit app

## Prerequisites

- Python 3.8+
- Microsoft Azure subscription
- Microsoft Fabric workspace with Lakehouse
- Azure AD app registration with appropriate permissions:
  - `https://api.fabric.microsoft.com/.default`
  - `https://storage.azure.com/.default`

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd self_serving_fabric

# Install dependencies
pip install streamlit pandas requests azure-identity azure-storage-file-datalake \
    msal pyjwt great-expectations graphviz python-dotenv

# Or install from requirements.txt (if available)
# pip install -r requirements.txt
```

## Configuration

### Environment Variables

The application uses environment variables for secure configuration management.

1. Copy the example environment file:
```bash
cp .env.example .env
```

2. Edit `.env` and add your Microsoft Fabric credentials:
```bash
# Azure AD Configuration
FABRIC_TENANT_ID=your-tenant-id-here
FABRIC_CLIENT_ID=your-client-id-here
FABRIC_CLIENT_SECRET=your-client-secret-here

# Fabric Workspace and Lakehouse
FABRIC_WORKSPACE_ID=your-workspace-id-here
FABRIC_WORKSPACE_NAME=your-workspace-name
FABRIC_LAKEHOUSE_ID=your-lakehouse-id-here
FABRIC_LAKEHOUSE_NAME=your-lakehouse-name

# Development Settings (optional)
DEV_MODE_SKIP_LOGIN=False
```

3. Ensure `.env` is added to `.gitignore` to prevent committing sensitive credentials.

### Getting Your Credentials

1. **Azure AD App Registration**:
   - Go to Azure Portal > Azure Active Directory > App Registrations
   - Create a new registration or use existing one
   - Note the `Application (client) ID` and `Directory (tenant) ID`
   - Create a client secret under "Certificates & secrets"

2. **Microsoft Fabric Workspace & Lakehouse**:
   - Open Microsoft Fabric portal
   - Navigate to your workspace and note the Workspace ID from the URL
   - Open your Lakehouse and note the Lakehouse ID from the URL

3. **API Permissions** (required for the app registration):
   - `https://api.fabric.microsoft.com/.default`
   - `https://storage.azure.com/.default`

### Alternative: Azure Key Vault (Production)

For production deployments, use Azure Key Vault:

```python
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

credential = DefaultAzureCredential()
client = SecretClient(vault_url="https://your-vault.vault.azure.net/", credential=credential)

fabric_tenant_id = client.get_secret("fabric-tenant-id").value
```

## Usage

```bash
# Start the application
streamlit run app.py
```

### Workflow Steps

1. **Login**: Authenticate with app credentials and acquire Fabric tokens
2. **Select Data Source**: Choose from Local File, OneLake, or ADLS
3. **Data Validation**: Configure validation rules and split clean/error data
4. **Data Cleaning**: Apply cleaning operations (optional)
5. **Data Transformation**: Apply custom transformations (optional)
6. **Load Data**: Push processed data to Fabric Lakehouse
7. **Pipeline Success**: Review lineage and download configurations

## Project Structure

```
self_serving_fabric/
├── app.py                          # Main application entry point
├── utils.py                        # Utility functions for Fabric API
├── fabric_auth_helper.py           # Authentication and token management
├── pages/                          # Streamlit page modules
│   ├── login.py
│   ├── select_data_source.py
│   ├── data_validation.py
│   ├── data_cleaning.py
│   ├── data_transformation.py
│   ├── load_data.py
│   ├── pipeline_final.py
│   └── pipeline_success.py
├── data_sources/                   # Data source implementations
│   ├── abstract_data_source.py
│   ├── local_file_data_source.py
│   ├── onelake_data_source.py
│   └── adls_data_source.py
├── cleaning/                       # Cleaning operation classes
│   ├── base_cleaning.py
│   ├── remove_duplicate.py
│   ├── remove_null_data.py
│   ├── fill_null_data.py
│   ├── rename_columns.py
│   └── drop_columns.py
├── components/                     # Reusable UI components
│   └── lineage_component.py
└── template_notebook/              # Fabric notebook templates
    ├── ge_validation_template.py
    └── upload_file.py
```

## Technical Details

### State Management
The application uses an object-oriented approach with `PipelineApp` class managing all session state, synchronized with Streamlit's session state for persistence across reruns.

### Data Source Pattern
Data sources implement `AbstractDataSource` interface with methods:
- `get_source_options()`: Retrieve available options
- `set_active_selection()`: Process user selection
- `is_selection_valid()`: Validate current state
- `reset_source_state()`: Clear source-specific state

### Cleaning Operations
Cleaning operations extend `BaseCleaning` class with:
- Configuration management
- DataFrame transformation logic
- Lineage tracking integration

### API Integration
All Fabric API calls go through `call_fabric_api()` utility function with:
- Centralized authentication header injection
- Error handling and retry logic
- Response parsing and validation

## Security Notes

- All credentials are managed via environment variables (never hardcoded)
- The `.env` file is excluded from version control via `.gitignore`
- Production deployments should use Azure Key Vault for enhanced security
- Device Code Flow requires user interaction for authentication
- Tokens are stored in Streamlit session state (memory only, not persisted)
- Mock user credentials in `config.py` should be replaced with proper authentication in production

## License

[Add your license information here]