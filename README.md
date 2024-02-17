# Hubspot Data Integration

## Overview

This repository contains Python programs and modules designed for integrating and uploading data to HubSpot CRM from multiple data sources. The program fetches data from SQL Server and MySQL databases, performs data processing, and uploads the enriched data to HubSpot using its API.

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [License](#license)

## Features

- Fetches data from SQL Server and MySQL databases.
- Enriches data through data processing and joins.
- Creates a batch payload for uploading data to HubSpot.
- Handles API requests, retries, and error responses.
- Supports configurable refresh intervals.

## Requirements

- Python 3.x
- Required Python packages (install using `pip install -r requirements.txt`):
  - pandas
  - requests
  - mysql-connector-python
  - sqlalchemy

## Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/MinhQuan-Pham/internal_db_hubspot_pipeline.git
    ```

2. Install the required Python packages:

    ```bash
    pip install -r requirements.txt
    ```

## Usage

1. Set up your HubSpot API keys and configure the necessary parameters in `config.py`.

2. Run the main file:

    ```bash
    python data_uploader.py
    ```

3. The script will fetch data, process it, and upload it to HubSpot CRM.

## Configuration

Update the `config.py` file with your specific configurations:

```python
# config.py

# Replace values with your actual credentials
PROD_API_KEY = "your_production_api_key"
TEST_API_KEY = "your_test_api_key"
# Add any additional configuration parameters here

```

## License
This project is licensed under the MIT License.
