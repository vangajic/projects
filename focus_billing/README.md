# Focus Billing

This repository contains the Focus Billing project, which is designed to manage and analyze billing data. The project includes various scripts and notebooks to process, analyze, and visualize billing information. 

## Contents

- `dags/`: Directory containing airflow-ready dag file.
- `scripts/`: Jupyter Notebooks for data processing and analysis.
- `dashboard/`: PowerBI Desktop project file for interactive data analysis.
- `BeerCraft_Technical_Exercise.pdf`: Casefile containing project description and source (cost and price) files.
- `README.md`: Project documentation.

## How to Use

### Setting Up the Environment

1. **Airflow**
    - Follow the [official Airflow installation guide](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html) to set up Airflow.
    - Reuse dag provided in the `dags/` directory.

2. **Jupyter Notebook**
    - Install Jupyter Notebook by following the [official Jupyter installation guide](https://jupyter.org/install).
    - Launch Jupyter Notebook and open the notebooks in the `scripts/` directory for interactive processing and analysis.

3. **PowerBI**
    - Download and install PowerBI Desktop from the [official PowerBI website](https://powerbi.microsoft.com/desktop/).
    - Use the project file in the `dashboard/` directory to explore report.

### Resources
0. Links to source files can be found in `BeerCraft_Technical_Exercise.pdf`
1. Process the data using either:
    -  **Jupyter Notebook** using the scripts in the `scripts/` directory
    -  **Airflow** by running `azure_billing_dag.py`
2. Resulting data (FOCUS transformed billing file) will be stored in `focus_billing.csv` file
3. Download `focus_billing.csv` file to your local environment
4. Visualization project is available in `dashboard/` folder
    -  Open it in **PowerBI Desktop** and _feed_ it with previously downloaded data

For detailed instructions on required setup steps, refer to the respective documentation links provided above.
