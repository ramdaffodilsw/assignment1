import numpy as np
import pandas as pd
import os,sys
import time

from loguru import logger

# For aws file downloading
import boto3
import botocore

# To load the environment variables
from dotenv import load_dotenv
load_dotenv()


def extract(file_path):
    """
     This function is to extract the specified data from the Pandas dataframe

    :params args. file_path used for loading sbmc input file from specified directory

    :param returns.  A pandas dataframe conting specific data for further porcessing
    """
    if file_path is not None:
        sheet_name = "Data Input - Port to Port"
        # load the data frame
        df  = load_file(file_path=file_path, sheet_name=sheet_name)
        print("File loaded completely")
        return df



def load_file(file_path, sheet_name):
    """  
    This function is for loading the excel file from a specified location to in memory
    while executing this module/data_pipline

    :param args. 
        file_path : file path to load the file
        sheet_name : name of the sheet to load from the excel file

    :param returns. return a pandas dataframe obeject containg the data from the file
    """

    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        logger.error(e)
    print("Reading the csv completed")


