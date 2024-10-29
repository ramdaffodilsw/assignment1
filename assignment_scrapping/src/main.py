#from extract import extract
#from transform import transform
from load import load

from loguru import logger
import time


import os,sys,inspect
from lib.utils import extract_content_from_site, get_db_object

# import requests
from bs4 import BeautifulSoup
from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
# from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.common.action_chains import ActionChains
import random



# Transformation execution id
RUN_ID = os.getenv("RUN_ID")
PROCESS_ID = os.getenv("PROCESS_ID")
SCRAP_SITE_URL = os.getenv("SCRAP_SITE_URL")

# List of proxies
proxy_list = [
    'http://xxx',
    'http://xxx',
    'http://xxx'
]
# List of uesr-agents
user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
    'Mozilla/5.0 (X11; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15'
]

# Proxy and user-agent rotation setup
def get_driver():
    chrome_options = Options()
    # chrome_options.add_argument(f'--proxy-server={random.choice(proxies)}')
    chrome_options.add_argument(f'user-agent={random.choice(user_agents)}')
    driver = webdriver.Chrome(options=chrome_options)
    return driver


def main():
    """ 
    This is the main function which executes the site scrapping process, headers and proxy things
    Once the data is scrapped, it calls the loading function and load the data in DB
    
    Function Args : None

    Returns :  None
    """
    try:

        # Proxy setup for selnium, commented for now
        '''
        # Choose a random proxy from the list
        proxy = random.choice(proxy_list)
        # Set up Selenium options with a proxy
        chrome_options = Options()
        chrome_options.add_argument(f'--proxy-server=http://{proxy}')
        # Initialize the WebDriver with the proxy
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(SCRAP_SITE_URL)
        '''
        

        driver = get_driver()   # headers (user-agents) and proxies are set using get_driver() function now
        driver.get(SCRAP_SITE_URL)

        site_content = []
        previous_item_count = 0

        while True:

            time.sleep(random.uniform(1,5))
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            
            # Wait for new content to load
            # time.sleep(10)
            # driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            # break

            items = driver.find_elements(By.CLASS_NAME, "_1_1Uy")  # Select all children of the div
            current_item_count = len(items)

            # Check if new items were added to the div
            if current_item_count > previous_item_count:
                logger.info("Additional items loaded. Continuing scrolling...")
                previous_item_count = current_item_count
                no_change_count = 0
            else:
                logger.info("No additional items loaded.")
                if no_change_count >= 1:
                    logger.info("No additional items loaded again. Ending scrolling.")
                    break
                no_change_count += 1

            # time.sleep(random.uniform(1, 3))

        
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        site_content = extract_content_from_site(soup)

        # Close the driver
        driver.quit()

        #print(all_quotes.encode("utf-8"))
        # Load the data in dataware house
        logger.info("Loading site content to db start.")
        load(site_content)
        logger.info("Loading site content to db end.")

        exit("Task complete.")
        
        
        
        ############################## Approach 2
        # '''
        # First we can scrap the inner html in files as it will not take much time, then once the job is done, we can
        # parse these html files and fetch the relevent content
        # '''
        # file = 0
        # for i in range (1,3):
        #     driver.get(f"https://www.cars24.com/buy-used-car?sort=bestmatch&serveWarrantyCount=true&listingSource=ViewAllCars&storeCityId=2")
        #     site_elements = driver.find_elements(By.CLASS_NAME,"_2Out2")
            
        #     for elements in site_elements:
        #         d = elements.get_attribute("outerHTML")
        #         with open(f"01_structure_data/htmlfile_{file}.html", "w", encoding="utf-8") as f:
        #             f.write(d)
        #             file += 1

        #     time.sleep(2)
        # driver.close()
        # exit("=========")
        ##############################

       

    except Exception as e:
        logger.error("Error in scrapping site content.")  
        logger.error(e) 
        sys.exit(1)

  
if __name__ == "__main__":
    main()


    