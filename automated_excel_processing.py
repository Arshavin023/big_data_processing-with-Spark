# Import libraries
from datetime import datetime
import pandas as pd
import os
import time
from io import StringIO
from pathlib import Path


def main():
    def read_files_into_dictionary(raw_folder_path,processed_folder_path):
        # Define the condition for filtering
        condition = lambda x: x['LCY AMOUNT'] >= 10000
        # Sheet to read in
        sheet_name = 'DETAILS'
        # list of columns to read in
        columns_to_read = ['MERCHANT BANK ACCOUNT','LCY AMOUNT','MERCHANT DEPOSIT BANKNAME',
                           'ACQCOUNTRY','TRANSACTION ID','TRANSACTION TYPE','SETTLEMENT DATE']
        # Initialize an empty dictionary to store DataFrames
        dataframes_dict = {}
        
        # Get a list of all files in the folder
        raw_files = [file for file in os.listdir(raw_folder_path) if file.endswith('.xlsx')]
        processed_files = [os.path.splitext(file)[0] for file in os.listdir(processed_folder_path) if file.endswith('.csv')]

        # Read each Excel file into a DataFrame and add it to the dictionary
        for excel_file in raw_files:
            if (os.path.splitext(excel_file)[0] + '_processed') not in processed_files: 
                file_path = os.path.join(raw_folder_path, excel_file)
                try:
                     # Read the EXCEL file where condition meets
                    filtered_df = pd.read_excel(file_path, sheet_name=sheet_name, 
                                                usecols=columns_to_read, skiprows=6).loc[condition]
    #               filtered_df['SETTLEMENT DATE'] = pd.to_datetime(filtered_df['DateTime'])
                    filtered_df['SETTLEMENT DATE'] = filtered_df['SETTLEMENT DATE'].astype('str')
                    # Create a key from the file name (excluding extension)
                    key = os.path.splitext(excel_file)[0]
                    # Add the DataFrame to the dictionary
                    dataframes_dict[key] = filtered_df
                except pd.errors.EmptyDataError:
                    # Handle the case where the Excel file is empty
                    print(f"Warning: {excel_file} is empty.")
                except Exception as e:
                    # Handle other exceptions (e.g., invalid Excel file)
                    print(f"Error reading {excel_file}: {str(e)}")
            else:
                pass

        return dataframes_dict


    def process_dataframe_in_dictionary(result,processed_folder_path):
        # processed_folder_path = os.path.abspath("C:/Users/uche.nnodim/Desktop/isw_upsl/upsl_process_files/test_processed")

        try:
            for name, df in result.items():
                date = df['SETTLEMENT DATE'].iloc[0]
                output_with_D_tran_type = df.groupby('MERCHANT BANK ACCOUNT').agg(
                    {'LCY AMOUNT': lambda x: (x.count() * 50),  # Simplified the lambda function
                     'MERCHANT DEPOSIT BANKNAME': lambda x: 'STAMP DUTY : ' + date + ' ' + str(x.count() + 1),
                     'ACQCOUNTRY': lambda x: 'D',
                     'TRANSACTION ID': lambda x: '',
                     'TRANSACTION TYPE': lambda x: ''})
                
                output_with_D_tran_type.rename(columns={'MERCHANT BANK ACCOUNT':'account_no',
                                          'LCY AMOUNT':'amount',
                                          'MERCHANT DEPOSIT BANKNAME':'narration',
                                          'ACQCOUNTRY': 'tran_type',
                                          'TRANSACTION ID':'column4',
                                          'TRANSACTION TYPE':'pal_account_code'},inplace=True)

                
                output_with_C_tran_type = pd.DataFrame({
                    'MERCHANT BANK ACCOUNT': ['8152159070'],
                    'amount': [output_with_D_tran_type['amount'].sum()],
                    'narration': ['STAMP DUTY '],
                    'tran_type': ['C'],
                    'column4': [''],
                    'pal_account_code': ['']
                })
                output_with_C_tran_type.set_index('MERCHANT BANK ACCOUNT', inplace=True)
                
                final_output = pd.concat([output_with_D_tran_type, output_with_C_tran_type], ignore_index=False)
                final_output = final_output.reset_index()
                final_output.rename({'MERCHANT BANK ACCOUNT':'account_no'},inplace=True)

                if not os.path.exists(processed_folder_path):
                    os.makedirs(processed_folder_path)
                    final_output.to_csv(f'{processed_folder_path}/{name}_processed.csv',index=False)

                else:
                    final_output.to_csv(f'{processed_folder_path}/{name}_processed.csv',index=False)
                    

        except Exception as e:
            print(f'The following error occurred => {e}')

        return 'processing of all files completed'

    raw_direc = "C:/Users/uche.nnodim/Desktop/isw_upsl/upsl_raw_files/test"
    raw_folder_path = os.path.abspath(raw_direc)
    processed_direc = "C:/Users/uche.nnodim/Desktop/isw_upsl/upsl_process_files/test_processed"
    processed_folder_path = os.path.abspath(processed_direc)

    start_time = time.time()
    result = read_files_into_dictionary(raw_folder_path,processed_folder_path)
    process_dataframe_in_dictionary(result,processed_folder_path)
    print('successfully completed')
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Time taken: {elapsed_time:.5f} seconds")

if __name__ == "__main__":
    main()
