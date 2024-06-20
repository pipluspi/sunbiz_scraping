# sunbiz_scraping

# Create python environment 
** Using cmd : python -m venv env

# step_0 : Activate your python environment
** Using cmd : env\Scripts\activate

# step_1 : Install all dependencies
** Using : pip install -r requirement.txt

# step_2 : Scrap all URLs of sunbix
** Using file step_1_fetch_urls.py

# step_3 : Divide those urls into chunks of csv file
** Using file step_2_chunk.py

# step_4 : Scrap the data from urls (This will create multiple json files)
** Using file step_3_fetch_data.py

# step_5 : Merge all json 
** Using file step_4_merge_json.py

# step_6 : Transorm data
* Before starting this file select kernal (env(3.12.2)) - This might be change as per the current version of python
** Using file step_5_transform_Data.ipynb


