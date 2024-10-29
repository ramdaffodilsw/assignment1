"""
DAG Script for Web Scraping with Airflow

This script defines an Airflow Directed Acyclic Graph (DAG) for a web scraping 
process, which is triggered daily. The DAG uses a PythonOperator to execute a 
Python function that calls the main web scraping function from an external module, 
`src.main`.

Modules and Packages:
    - `airflow`: Provides the core components for building and scheduling the DAG.
    - `PythonOperator`: Executes Python code in the DAG.
    - `datetime`, `timedelta`: Manage scheduling and timing of DAG execution.
    - `os`, `sys`: Configure the system path for imports.
    - `loguru`: Provides logging with rotation to track task progress and issues.

Usage:
    - Place this script in the Airflow DAGs folder.
    - Ensure the `src` module and `main` function are accessible at the 
      specified path.
    - Logs are generated daily in the `logs` directory with a timestamped filename.

Configuration:
    - `dag_id`: Unique identifier for the DAG.
    - `schedule_interval`: Specifies that the DAG will run daily.
    - `default_args`: Sets default arguments, including retry parameters.
    - `loguru` logger is used to track start, success, and error events in the 
      scraping process.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from loguru import logger

from src.main import main  # Import the main function from scrap_src

# Default arguments for the Airflow DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the Airflow DAG with schedule and task
with DAG(
    dag_id='scraping_dag_db',
    default_args=default_args,
    description='A simple scraping DAG to run a web scraping task daily',
    schedule_interval='@daily',
    start_date=datetime(2024, 10, 28),
    catchup=False,
) as dag:

    def run_scraping():
        """
        Executes the web scraping process by calling the main function
        from the 'scrap_src.main' module.
        
        Logs the start and end of the scraping task, including error handling
        to capture any issues encountered during the run.

        Raises:
            Exception: If an error occurs during the execution of the scraping process.
        """
        logger.info("Starting the scraping process...")
        try:
            # Call the main function to perform scraping
            main()
            logger.info("Scraping process completed successfully.")
        except Exception as e:
            logger.error("Error occurred during scraping.")
            logger.error(e)
            raise

    # Define the task to run the scraping function
    scraping_task = PythonOperator(
        task_id='run_scraping',
        python_callable=run_scraping,
    )

    ############## Set the task in the DAG ##############
    scraping_task
