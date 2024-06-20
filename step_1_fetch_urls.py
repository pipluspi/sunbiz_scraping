from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
import pandas as pd
import os



"""
Define your keyword that you want to start
"""
keyword = '3'

# Url for scrap
url = 'https://dos.sunbiz.org/jlilist.html'

# selenium driver load
options = Options()
options.add_experimental_option("detach", True)
options.add_argument("headless")
options.add_argument('--no-sandbox')

# Selenium Driver
driver = webdriver.Chrome(options=options)
driver.maximize_window()

os.makedirs('data_step_1', exist_ok=True)
if os.path.exists('data_step_1/step_1_urls_'+keyword+'.csv'):
     read_df = pd.read_csv('data_step_1/step_1_urls_'+keyword+'.csv')
     last_url = read_df.iloc[-1]['current_url']
     driver.get(last_url)
else:
    # open broser to scrap data
    driver.get(url)
    time.sleep(2)
    search_input = driver.find_element(By.NAME,"inquiry_value")
    search_input.send_keys(keyword)
    submit_button = driver.find_element(By.NAME,"submit")
    submit_button.click()

# loop to fetch next results
next_list = True
previous_url = ''
debtor_flag = ""
while next_list is True:
    current_url = driver.current_url
    print(previous_url)
    print(current_url)
    if previous_url == current_url:
        print('****************************Not able to fetch next URL******************************')
        time.sleep(15)
    
    container = driver.find_element(By.ID, 'listtable')
    tables = container.find_elements(By.TAG_NAME, 'table')
    rows = tables[1].find_elements(By.TAG_NAME, 'tr')
    main_df = pd.DataFrame()
    for row in rows:
        cells = row.find_elements(By.TAG_NAME, 'td')
        if len(cells) >= 3: 
            href_url = cells[0].find_element(By.TAG_NAME, 'a').get_attribute('href')
            filing_number = cells[0].text
            debtor_name = cells[1].text
            address = cells[2].text
            data = {'href_url': href_url,'filing_number': filing_number, 'debtor_name': debtor_name, 'address': address, 'current_url':current_url}
            df = pd.DataFrame()
            df = df._append(data,ignore_index=True)
            main_df = pd.concat([main_df,df])
            debtor_flag = debtor_name.strip()
            if keyword[0].isalnum():
                debtor_flag = debtor_flag.upper().replace('THE','')
            debtor_flag = ''.join(letter for letter in debtor_flag if letter.isalnum())
    print(main_df)
    if os.path.exists('data_step_1/step_1_urls_'+keyword+'.csv'):
        main_df.to_csv('data_step_1/step_1_urls_'+keyword+'.csv', header=False, mode='a',index=True)
    else:
        main_df.to_csv('data_step_1/step_1_urls_'+keyword+'.csv', header=True, index=True)
    next_elements = driver.find_elements(By.XPATH,'//*[@id="navbarlinkactive"]/a')
    if len(next_elements) > 2:
        next_button = next_elements[1]
        previous_url = driver.current_url
        next_button.click()
        time.sleep(1)
    else:
        next_list = False    
    print('***********************')
    print(debtor_flag)
    print('***********************')
    if debtor_flag[0:len(keyword)].upper() != keyword.upper():
        print('***********************')
        print(debtor_flag[0:len(keyword)].upper())
        print(keyword.upper())
        print('***********************')
        next_list = False    

driver.close()