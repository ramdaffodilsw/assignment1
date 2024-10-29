from extract import extract
from transform import transform
from load import load

from loguru import logger
import time


import os,sys,inspect
from lib.utils import get_db_object, get_files_list_for_current_session, put_files_list_for_current_session
#from lib.utils import update_log_status

#currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
#parentdir = os.path.dirname(currentdir)
#sys.path.insert(0,parentdir) 
#from lib.utils import aws_file_scanner, download_file_from_aws, save_log_to_db

# Transformation execution id
RUN_ID = os.getenv("RUN_ID")
PROCESS_ID = os.getenv("PROCESS_ID")


def main():
    """ 
    This is the main function which executes to do 3-things for Structuring and cleaning the 
    unstructured data which are as follows : 
    1. Extract : Extract the data from a specified location
    2. Load :  Loads the data from specific location to the DB target location
    3. Transform : Transform the loaded data as per requirements and load to the destination
    
    Function Args : None

    Returns :  None
    """
    status = "inprogress"
    log_file_path = os.getenv("LOG_FILE_PATH")
    logger.add(log_file_path)


    # Input folder path
  
    input_dir_path = r'D:\\Daffo\\Other\\assignment\\data_files\\input\\'
    
    # Get the list of files from the input directory, although the list of file should come from DB if users 
    # are uploading it, for this assignment purpose I am taking these files from a directory 
    # RUN_ID scheduler or job running ID
    # PROCESS_ID id of a process that are runnung
    # These ID will help us in tracking the logs
    files_list = get_files_list_for_current_session(input_dir_path, RUN_ID, PROCESS_ID)



    # Checking if there is atleast 1 file exists in the files_list
    if len(files_list) > 0:

        # counter for file name association
        count = 1
        for file_path in files_list:
            
            # Loading data file
            logger.info("Loading file ##"+ file_path +"## from directory")
            try:
                # Do extraction 
                logger.info("Extracting")
                extract_start = time.time()
                dataframe = extract(input_dir_path+file_path)
                extract_end = time.time()
                logger.info("Extract - Done")

                # Load the data in dataware house
                logger.info("Loading")
                load_start = time.time()
                response = load(dataframe)

                # Tracking execution time
                logger.info("Time Stats:")
                logger.info("Extract - %.2f sec." %(extract_end - extract_start))
                #logger.info("Transform - %.2f sec." %(transform_end - transform_start))
                count += 1       

                # update the etl status
                status = "completed"
                logger.info("Iteration completed.")
                logger.info("------------")
                        
            except Exception as e:
                logger.error("Error in loading the file")  
                logger.error(e) 
                # update the etl status
                status = "failed"
                        
                sys.exit(1)


        # Do Transformation
        logger.info("Transforming")
        transform_start = time.time()
        output_file_path = transform()
        transform_end = time.time()
        logger.info("Transform - Done")

    else:
        logger.error("No files found.")
        # update the etl status
        status = "failed"
                
        sys.exit(1)


  
if __name__ == "__main__":
    main()


    