import os
import pandas as pd

folder = 'data_step_1/'
# reading the files
list_file = os.listdir(folder)

# reading all files and concating into the one file
main_df = pd.DataFrame()
for file_name in list_file:
    df = pd.read_csv(folder+file_name)
    main_df = pd.concat([main_df,df])
main_df.drop(columns=['Unnamed: 0','current_url','debtor_name','address'],inplace=True)
main_df = main_df.drop_duplicates(subset=['filing_number'])
main_df = main_df.sort_values(by=["filing_number"])
print(len(main_df))

os.makedirs('data_step_2_merge', exist_ok=True)
if os.path.isfile('data_step_2_merge/merge_data.csv'):
    os.remove('data_step_2_merge/merge_data.csv')

# Saving the data URLs into one file
main_df.to_csv('data_step_2_merge/merge_data.csv')

# Dividing data into chunks
os.makedirs('data_step_2_chunk', exist_ok=True)
for i,chunk in enumerate(pd.read_csv('data_step_2_merge/merge_data.csv', chunksize=200)):
    chunk.to_csv('data_step_2_chunk/{}_chunk.csv'.format(i), index=False)
    print('{}_chunk.csv has been saved'.format(i))

print("Divided into the chunks")