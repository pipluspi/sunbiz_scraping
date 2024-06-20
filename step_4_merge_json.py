import pandas as pd
import json
import os
import glob
import re

folder_path = 'data_step_3_scrap'
folder_list = os.listdir(folder_path)

for idx,folder in enumerate(folder_list):
    main_df = pd.DataFrame()
    file_list = glob.glob(os.path.join(folder_path,folder, '*.json'))
    for file in file_list:  
        with open(file, 'r') as f:
            data = json.load(f)
            temp_dict = {}
            temp_dict['Document Number'] = data.get('Document Number', '')
            temp_dict['Status'] = data.get('Status', '')
            temp_dict['Case Number'] = data.get('Case Number', '')
            temp_dict['Name of Court'] = data.get('Name of Court', '')
            temp_dict['File Date'] = data.get('File Date', '')
            temp_dict['Date of Entry'] = data.get('Date of Entry', '')
            temp_dict['Expiration Date'] = data.get('Expiration Date', '')
            temp_dict['Amount Due'] = data.get('Amount Due', '')
            temp_dict['Interest Rate'] = data.get('Interest Rate', '')
            temp_dict['creditors'] = data.get('creditors', '')
            temp_dict['debtors'] = data.get('debtors', '')
            temp_dict['Original Document'] = data.get('Original Document', '')
            temp_dict['Amount Remaining'] = data.get('Amount Remaining', '')
            temp_dict['url'] = data.get('url', '')
        df = pd.json_normalize(temp_dict)
        
        main_df = pd.concat([main_df, df], ignore_index=True)  
        main_df.drop_duplicates(inplace=True)
    os.makedirs('data_step_4_scrap_merged', exist_ok=True)
    if os.path.exists('data_step_4_scrap_merged/scrap_df.csv'):
        main_df.to_csv('data_step_4_scrap_merged/scrap_df.csv', header=False, mode='a',index=False)
    else:
        main_df.to_csv('data_step_4_scrap_merged/scrap_df.csv', header=True, index=False)
    print("_________________________________")
    print(folder)
    print(idx)
    print("_________________________________")

