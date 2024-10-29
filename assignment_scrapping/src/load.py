import numpy as np
import pandas as pd
import os,sys
import time
import logging
from loguru import logger
# This code is for loading all module built, into the current directory
import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 

# For aws file downloading
import boto3
import botocore

# To load the environment variables
from dotenv import load_dotenv
load_dotenv()

from lib.utils import get_db_object, load_site_content_to_db

RUN_ID = os.getenv("RUN_ID")
NEXT_PROCESS_ID = os.getenv("NEXT_PROCESS_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")


def load(site_content):
    
    """
    This function loads the cleaned_data.csv - CSV file to another pipline filepath
    for further processing

    :param args. output_file_path - csv generated file path
    :param args. count - count value for filename association

    Return type  : response - Boolean value 
    """


    try:
       
        response = load_site_content_to_db(pd.DataFrame(site_content))
        return response


    except botocore.exceptions.ClientError as e:
        logger.error(e)

        
