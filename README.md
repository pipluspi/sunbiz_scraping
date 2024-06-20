# sunbiz_scraping

# Create python environment 
* Using cmd : python -m venv env

# step_0 : Activate your python environment
** Using cmd : env\Scripts\activate


# step_1 : Install all dependencies
* Using : pip install -r requirement.txt

# step_2 : Scrap all URLs of sunbix
* First read file name step_1_fetch_urls_read_first.txt then run 
* File step_1_fetch_urls.py

# step_3 : Divide those urls into chunks of csv file
* First read file name step_2_chunk_read_first.txt then run 
* Using file step_2_chunk.py

# step_4 : Scrap the data from urls (This will create multiple json files)
* First read file name step_3_fetch_data_read_first.txt then run 
* Using file step_3_fetch_data.py

# step_5 : Merge all json 
* First read file name step_4_merge_json_read_first.txt then run 
* Using file step_4_merge_json.py

# step_6 : Transorm data
* First read file name step_1_fetch_urls_read_first_read_first.txt then run 
* Before starting this file select kernal (env(3.12.2)) - This might be change as per the current version of python
** Using file step_5_transform_Data.ipynb


