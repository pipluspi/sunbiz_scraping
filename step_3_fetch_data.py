import requests
from bs4 import BeautifulSoup
from threading import Thread,Event
import queue
import pandas as pd
import datetime
import os
import shutil
import json
import glob

while(True):
    print('Start Time :',datetime.datetime.now())


    '''
    # Reading the series nuber taht we want to process 
    HERE Define your series number
    '''
    input_series = 0
    
    
    file_list = glob.glob(f'data_step_2_chunk/{input_series}*.csv')
    input_file_name = file_list[0].split('\\')[-1]


    # Moving the read file into inprogress folder
    os.makedirs('inprogress/', exist_ok=True)
    shutil.move('data_step_2_chunk/'+input_file_name,'inprogress/'+input_file_name)
    print(input_file_name)
    
    df = pd.read_csv('inprogress/'+input_file_name)

    urls = df['href_url']

    html_queue = queue.Queue()

    def fetch_html(url):
        """Function to fetch HTML content of a URL."""
        print(url)
        try:
            headers = {
                'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
                'authority':'dos.sunbiz.org',
                'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Encoding':'gzip, deflate, br, zstd'
            }
            response = requests.get(url,headers=headers)
            if response.status_code == 200:
                html_queue.put((url, response.text))
            else:
                print(f"Failed to retrieve {url}. Status code: {response.status_code}")
                exceptiondf = pd.DataFrame()
                exceptiondf['url'] = url
                exceptiondf['error'] = "Response"
                exceptiondf.to_csv('exeption_error_data.csv', header=True, mode='a',index=True)
                print(f"Request failed for {url}: {response.status_code}")
                
        except requests.RequestException as e:
            exceptiondf = pd.DataFrame()
            exceptiondf['url'] = url
            exceptiondf['error'] = "Response"
            exceptiondf.to_csv('exeption_error_data.csv', header=True, mode='a',index=True)
            print(f"Request failed for {url}: {e}")
            
    def worker():
        """Thread worker function to process URLs from the queue."""
        while not urls_queue.empty():
            url = urls_queue.get()
            fetch_html(url)
            urls_queue.task_done()

    def parse_html():
        """Function to parse HTML content from the queue."""
        while not html_queue.empty():
            url, html_content = html_queue.get()
            soupHtml = BeautifulSoup(html_content, 'html.parser')
            filling_section = soupHtml.find('table', summary='This table contains the filing information.')
            if filling_section:
                filling_json = {}
                rows = filling_section.find_all('tr')
                for i in range(len(rows)):
                    if i == 0:
                        cells = rows[i].find_all('td')
                        if len(cells) >= 2:
                            key = cells[0].text.strip()
                            value = cells[1].text.strip()
                            filling_json[key] = value
                    elif i==1:
                        pass
                    else:     
                        cells =  rows[i].find_all('td')
                        if len(cells) == 2:
                            key = cells[0].text.strip()
                            value = cells[1].text.strip()
                            filling_json[key] = value
            creditor_json = {}
            creditor_section = soupHtml.find('span', class_='heading', string='Name And Address of Judgment Creditor (Plaintiff)')
            if creditor_section:
                creditor_data_list = creditor_section.find_all_next('tr')
                is_creditor_section = False
                creditor_data = ""
                while is_creditor_section is False:
                    for cred in creditor_data_list:
                        creditor = cred.find('td')
                        if creditor.find('span', class_='heading', string='Name And Address of Judgment Debtor(s) (Defendant(s))'):
                            is_creditor_section = True
                            break
                        else:
                            creditor_data += creditor.get_text('\n', strip=True) + '\n \n' 
                creditor_data = creditor_data.replace('\r\n', '\n \n').replace('                     ',' ').replace('                    ',' ').replace('   ', ' ').replace(':\n',': ').strip()
       
                filling_json['creditors'] = str(creditor_data)
                
            debtor_json = {}
            debtor_section = soupHtml.find('span', class_='heading', string='Name And Address of Judgment Debtor(s) (Defendant(s))')
            if debtor_section:
                debtors_data_list = debtor_section.find_all_next('tr')
                is_event_section = False
                debtor_data = ""
                while is_event_section is False:
                    for deb in debtors_data_list:
                        debtor = deb.find('td')
                        if debtor.find('span', class_='heading', string='Events'):
                            is_event_section = True
                            break
                        else:
                            debtor_data += debtor.get_text('\n', strip=True) + '\n \n' 
                debtor_data = debtor_data.replace('\r\n', '\n \n').replace('                     ',' ').replace('                    ',' ').replace('   ', ' ').replace(':\n',': ').strip()
                filling_json['debtors'] = str(debtor_data)
            filling_json['url'] = url
            # print(filling_json)
            document_number = filling_json['Document Number']
            if not os.path.exists('data_step_3_scrap/'+input_file_name.split('.')[0]+'/'):
                os.makedirs('data_step_3_scrap/'+input_file_name.split('.')[0]+'/')
            with open('data_step_3_scrap/'+input_file_name.split('.')[0]+'/'+document_number+'.json', 'w') as f:
                json.dump(filling_json, f)
            f.close()
            

    # Create a thread-safe queue for URLs
    urls_queue = queue.Queue()

    # Populate the queue with URLs
    for url in urls:
        # print(url)
        url_1 = url.split("inquiry_date")[0] 
        url = url_1 + "inquiry_date=0&return_number=0&return_date=0"
        urls_queue.put(url)

    # Create and start worker threads
    threads = []
    for _ in range(40):  # Number of threads
        thread = Thread(target=worker)
        thread.start()
        threads.append(thread)

    # Wait for all threads to complete
    urls_queue.join()

    # Parse the HTML content once all threads are done fetching
    parse_html()
    os.makedirs('completed/', exist_ok=True)
    shutil.move('inprogress/'+input_file_name,'completed/'+input_file_name)
    print('Stop Time :',datetime.datetime.now())
