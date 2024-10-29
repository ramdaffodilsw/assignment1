import numpy as np
import pandas as pd
import os,sys
from loguru import logger
from sqlalchemy import create_engine
from datetime import datetime

from lib.utils import get_source_db_object, get_destination_db_object


def fetch_data_for_fact_table():
    """
    Function to fetch data from the dimension tables to fact table
    :param args  none
    :param returns. query result set
    """

    try:
        # Load db object
        engine_destination  = get_destination_db_object()
        # Fetch the data from the loan table
        loan_fact_tbl_query = ("SELECT "
                                    "bd.borrower_id,"
                                    "ld.loan_id,"
                                    "psd.schedule_id,"
                                    "lpd.payment_id,"
                                    "psd.expected_payment_date,"
                                    "lpd.date_paid,"
                                    "DATEDIFF(lpd.date_paid, psd.expected_payment_date) AS Par_Days,"
                                    "psd.expected_payment_amount,"
                                    "lpd.amount_paid"
                                " FROM"
                                    "  borrower_dim AS bd"
                                        "  LEFT JOIN"
                                    " loan_dim AS ld ON bd.borrower_id = ld.borrower_id"
                                    "     LEFT JOIN"
                                    " payment_schedule_dim AS psd ON psd.loan_id = ld.loan_id"
                                    "     LEFT JOIN"
                                    " loan_payment_dim AS lpd ON lpd.id = psd.id"
                                " ORDER BY psd.expected_payment_date ASC , lpd.date_paid ASC")
        loan_fact_tbl_df = pd.read_sql(loan_fact_tbl_query, con=engine_destination)
        loan_fact_tbl_df.to_sql('loan_fact', engine_destination, if_exists='append', index=False)
        print("Completed")
    except Exception as e:
        print(e)
        return None
    return loan_fact_tbl_df


def transform_loan_payment_data():
    """
    Function to fetch data from the dimension tables to fact table
    :param args  none
    :param returns. query result set
    """

    try:
        # Load db object
        engine_destination  = get_destination_db_object()
        engine_source       = get_source_db_object()
        # Fetch the data from the loan table
        loan_payment_tbl_query = "SELECT * FROM loan_payment;"
        loan_payment_tbl_df = pd.read_sql(loan_payment_tbl_query, con=engine_source)

        # Change data type
        loan_payment_tbl_df['payment_id(pk)'] = loan_payment_tbl_df['payment_id(pk)'].astype('string')
        loan_payment_tbl_df['Amount_Paid_New'] = loan_payment_tbl_df['Date_paid'].astype('float')

        # Payment date
        loan_payment_tbl_df['Amount_Paid_Date'] = loan_payment_tbl_df['Amount_paid'].astype('string')
        loan_payment_tbl_df['Amount_Paid_Date'] = list(map(lambda x: datetime.strptime(x,'%m/%d/%Y').strftime('%Y-%m-%d'), loan_payment_tbl_df['Amount_Paid_Date']))
        loan_payment_tbl_df['Amount_Paid_Date'] = pd.to_datetime(loan_payment_tbl_df['Amount_Paid_Date'], errors='coerce')
        loan_payment_tbl_df['Amount_Paid_Date'] = loan_payment_tbl_df['Amount_Paid_Date'].dt.date
     
        # Rename columns
        loan_payment_tbl_df.rename(columns={'loan_id(fk)': 'loan_id'}, inplace=True)
        loan_payment_tbl_df.rename(columns={'payment_id(pk)': 'payment_id'}, inplace=True)
        loan_payment_tbl_df.rename(columns={'Amount_Paid_New': 'amount_paid'}, inplace=True)
        loan_payment_tbl_df.rename(columns={'Amount_Paid_Date': 'date_paid'}, inplace=True)
        
        # Remove not required columns
        loan_payment_tbl_df = loan_payment_tbl_df.drop('Date_paid', axis=1)
        loan_payment_tbl_df = loan_payment_tbl_df.drop('Amount_paid', axis=1)

        # Load the borrower data to the dimension table
        loan_payment_tbl_df.to_sql('loan_payment_dim', engine_destination, if_exists='append', index=False) 
        
        # Close DB connections
        engine_source.dispose()
        engine_destination.dispose()
        
        
    except Exception as e:
        print(e)
        return None
    return loan_payment_tbl_df


def transform_payment_schedule_data():
    """
    Function to fetch data from the dimension tables to fact table
    :param args  none
    :param returns. query result set
    """

    try:
        # Load db object
        engine_destination  = get_destination_db_object()
        engine_source       = get_source_db_object()
        
        # Fetch the data from the loan table
        payment_schedule_tbl_query = "SELECT * FROM payment_schedule;"
        payment_schedule_tbl_df = pd.read_sql(payment_schedule_tbl_query, con=engine_source)

        # Dates conversion
        payment_schedule_tbl_df['Expected_payment_date'] = list(map(lambda x: datetime.strptime(x,'%m/%d/%Y').strftime('%Y-%m-%d'), payment_schedule_tbl_df['Expected_payment_date']))
        payment_schedule_tbl_df['Expected_payment_date'] = pd.to_datetime(payment_schedule_tbl_df['Expected_payment_date'], errors='coerce')
        #payment_schedule_tbl_df['Expected_payment_date'] = payment_schedule_tbl_df['Expected_payment_date'].astype('datetime64[ns]')
        payment_schedule_tbl_df['Expected_payment_date'] = payment_schedule_tbl_df['Expected_payment_date'].dt.date
        payment_schedule_tbl_df['schedule_id'] = payment_schedule_tbl_df['schedule_id'].astype('string')
        payment_schedule_tbl_df['Expected_payment_amount'] = payment_schedule_tbl_df['Expected_payment_amount'].astype('float')
        # Rename columns
        payment_schedule_tbl_df.rename(columns={'loan_id': 'loan_id'}, inplace=True)
        payment_schedule_tbl_df.rename(columns={'schedule_id': 'schedule_id'}, inplace=True)
        payment_schedule_tbl_df.rename(columns={'Expected_payment_date': 'expected_payment_date'}, inplace=True)
        payment_schedule_tbl_df.rename(columns={'Expected_payment_amount': 'expected_payment_amount'}, inplace=True)
        #payment_schedule_tbl_df = payment_schedule_tbl_df.drop('loan_id', axis=1)
        
        # Load the borrower data to the dimension table
        payment_schedule_tbl_df.to_sql('payment_schedule_dim', engine_destination, if_exists='append', index=False) 
        
        # Close DB connections
        engine_source.dispose()
        engine_destination.dispose()
        
        
    except Exception as e:
        print(e)
        return None
    return payment_schedule_tbl_df


def transform_loan_table_data():
    """
    Function to fetch data from the dimension tables to fact table
    :param args  none
    :param returns. query result set
    """

    try:
        # Load db object
        engine_destination  = get_destination_db_object()
        engine_source       = get_source_db_object()
        
        # Fetch the data from the loan table
        loan_tbl_query = "SELECT * FROM loan_table;"
        loan_tbl_df = pd.read_sql(loan_tbl_query, con=engine_source)

        # Rename columns
        loan_tbl_df.rename(columns={'Borrower_id': 'borrower_id'}, inplace=True)
        loan_tbl_df.rename(columns={'Date_of_release': 'date_of_release'}, inplace=True)
        loan_tbl_df.rename(columns={'Term': 'term'}, inplace=True)
        loan_tbl_df.rename(columns={'InterestRate': 'interest_rate'}, inplace=True)
        loan_tbl_df.rename(columns={'LoanAmount': 'loan_amount'}, inplace=True)
        loan_tbl_df.rename(columns={'Downpayment': 'down_payment'}, inplace=True)
        loan_tbl_df.rename(columns={'Payment_frequency': 'payment_frequency'}, inplace=True)
        loan_tbl_df.rename(columns={'Maturity_date': 'maturity_date'}, inplace=True)
        
        #loan_tbl_df = loan_tbl_df.drop('Borrower_id', axis=1)
        #loan_tbl_df['payment_frequency']    = loan_tbl_df['payment_frequency'].astype('float')
        #loan_tbl_df['interest_rate']         = loan_tbl_df['interest_rate'].astype('float')

        # Load the borrower data to the dimension table
        loan_tbl_df.to_sql('loan_dim', engine_destination, if_exists='append', index=False) 
        
        # Close DB connections
        engine_source.dispose()
        engine_destination.dispose()
        
        
    except Exception as e:
        print(e)
        return None
    return loan_tbl_df


def transform_borrower_table_data():
    """
    Function to fetch data from the dimension tables to fact table
    :param args  none
    :param returns. query result set
    """

    try:
        # Load db object
        engine_destination  = get_destination_db_object()
        engine_source       = get_source_db_object()
        
        # Fetch the data from the borrower table
        borrower_tbl_query = "SELECT * FROM borrower_table;"
        borrower_tbl_df = pd.read_sql(borrower_tbl_query, con=engine_source)

        # Rename columns
        borrower_tbl_df.rename(columns={'Borrower_Id': 'borrower_id'}, inplace=True)
        borrower_tbl_df.rename(columns={'State': 'state'}, inplace=True)
        borrower_tbl_df.rename(columns={'City': 'city'}, inplace=True)
        borrower_tbl_df.rename(columns={'zip code': 'zip_code'}, inplace=True)
        borrower_tbl_df.rename(columns={'borrower_credit_score': 'borrower_credit_score'}, inplace=True)
        # Load the borrower data to the dimension table
        borrower_tbl_df.to_sql('borrower_dim', engine_destination, if_exists='append', index=False) 
        
        # Close DB connections
        engine_source.dispose()
        engine_destination.dispose()
        
    except Exception as e:
        print(e)
        return None
    return borrower_tbl_df



def transform():
    """ 
    This function is to extract the specific data from a pandas dataframe returned by extract 
    and saves it to a csv file

    :param args.
        dataframe. Data frame extracted from extract module
        filename . filename used for generating csv file with filename associated with it


    :param return.  CSV filepath, contating the extracted data

    """
    try:
        # Load DB Objects source and destination
        engine_source       = get_source_db_object()
        engine_destination  = get_destination_db_object()

        
        logger.info("Transforming loan payment data start.")
        # Transform and Load loan table data
        transform_loan_payment_data()
        logger.info("Transforming loan payment data end.")


        logger.info("Transforming payment schedule data start.")
        # Transform and Load loan table data
        transform_payment_schedule_data()
        logger.info("Transforming payment schedule data end.")


        logger.info("Transforming loan table data start.")
        # Transform and Load loan table data
        transform_loan_table_data()
        logger.info("Transforming loan table data end.")


        logger.info("Transforming borrower table data start.")
        # Transform and Load loan table data
        transform_borrower_table_data()
        logger.info("Transforming borrower table data end.")

        logger.info("----------")
        logger.info("Loading fact table start.")
        fetch_data_for_fact_table()
        logger.info("Loading fact table end.")
        
        

    except Exception as identifier:
       logger.error(identifier)

    return True


