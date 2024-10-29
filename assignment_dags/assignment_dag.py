"""
Airflow DAG for an ELT pipeline with separate Extract, Load, and Transform tasks.

This script defines an ETL process using Apache Airflow with three main tasks:
1. **Extract Task**: Reads data files from a specified directory and pushes them to XCom.
2. **Load Task**: Pulls data from XCom, performs a loading operation, and logs the load status.
3. **Transform Task**: Applies transformations to the loaded data and saves the output.

Each task is encapsulated in a Python function and executed in sequence using
the PythonOperator in Airflow.

Environment Variables:
- `RUN_ID`: Unique identifier for the run session.
- `PROCESS_ID`: Process identifier for tracking.
- `INPUT_DIR_PATH`: Path to the directory containing input data files.

DAG Structure:
The DAG is scheduled to run once daily and starts from a fixed date. Dependencies
are set to ensure `extract_task` runs first, followed by `load_task`, and then `transform_task`.

Modules:
- `src.extract`: Provides the `extract` function to read data.
- `src.load`: Contains the `load` function to handle data loading.
- `src.transform`: Defines the `transform` function for data transformation.
- `lib.utils`: Includes utility functions like `get_files_list_for_current_session`.

Logging:
Logging is performed using `loguru` to capture detailed task execution logs for
debugging and monitoring.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.load import load
from src.extract import extract
from src.transform import transform
from loguru import logger
import os
from lib.utils import get_files_list_for_current_session


# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'elt_pipeline',
    default_args=default_args,
    description='An ELT pipeline DAG with separate tasks for Extract, Load, and Transform',
    # scheduling is performed here.
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Environment variables (if required)
RUN_ID = os.getenv("RUN_ID")
PROCESS_ID = os.getenv("PROCESS_ID")
INPUT_DIR_PATH = r'D:\\Daffo\\Other\\assignment\\data_files\\input\\'

# Define each task function to run in Airflow
def extract_task(**kwargs):
    """
    Extract task function to retrieve files from the specified directory,
    process them, and push the resulting dataframe to XCom.

    :param kwargs: Additional keyword arguments for task instance context.
    """
    logger.info("Starting extract task...")
    files_list = get_files_list_for_current_session(INPUT_DIR_PATH, RUN_ID, PROCESS_ID)
    if files_list:
        for file_path in files_list:
            logger.info(f"Extracting file: {file_path}")
            dataframe = extract(INPUT_DIR_PATH + file_path)
            kwargs['ti'].xcom_push(key='dataframe', value=dataframe)
    else:
        raise ValueError("No files found in the directory.")

def load_task(**kwargs):
    """
    Load task function to pull the extracted dataframe from XCom,
    perform loading operation, and log the status.

    :param kwargs: Additional keyword arguments for task instance context.
    """    
    logger.info("Starting load task...")
    dataframe = kwargs['ti'].xcom_pull(task_ids='extract_task', key='dataframe')
    if dataframe is not None:
        response = load(dataframe)
        if response:
            logger.info("Load successful.")
        else:
            raise ValueError("Load failed.")
    else:
        raise ValueError("Dataframe is empty, load task cannot proceed.")

def transform_task(**kwargs):
    """
    Transform task function to perform transformations on the loaded data
    and log the output file path on successful completion.

    :param kwargs: Additional keyword arguments for task instance context.
    """    
    logger.info("Starting transform task...")
    output_file_path = transform()
    if output_file_path:
        logger.info(f"Transform task completed. Output saved to {output_file_path}")
    else:
        raise ValueError("Transform task failed to produce an output file.")

# Define the tasks in Airflow using PythonOperator
extract_op = PythonOperator(
    task_id='extract_task',
    python_callable=extract_task,
    provide_context=True,
    dag=dag,
)

load_op = PythonOperator(
    task_id='load_task',
    python_callable=load_task,
    provide_context=True,
    dag=dag,
)

transform_op = PythonOperator(
    task_id='transform_task',
    python_callable=transform_task,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_op >> load_op >> transform_op
