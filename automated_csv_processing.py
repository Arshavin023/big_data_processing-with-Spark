from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():

    def read_files_into_dictionary(raw_folder_path, processed_folder_path):
        spark = SparkSession.builder.appName("DataModeling").getOrCreate()

        # Define the condition for filtering
        condition = F.col('Merchant_Receivable') >= 10000

        # List of columns to read in
        columns_to_read = ['Merchant_Account_Nr', 'Merchant_Receivable', 'Merchant_Account_Name',
                           'Transaction_Status', 'Tran_ID', 'Trxn_Category', 'DateTime']

        # Initialize an empty dictionary to store DataFrames
        dataframes_dict = {}

        # Get a list of all files in the folder
        raw_files = [file for file in os.listdir(raw_folder_path) if file.endswith('.csv')]
        processed_files = [os.path.splitext(file)[0] for file in os.listdir(processed_folder_path) if
                           file.endswith('.csv')]

        # Read each CSV file into a DataFrame and add it to the dictionary
        for csv_file in raw_files:
            if (os.path.splitext(csv_file)[0] + '_processed') not in processed_files:
                file_path = os.path.join(raw_folder_path, csv_file)

                try:
                    # Read the CSV file and filter based on the condition
                    filtered_df = spark.read.csv(file_path, header=True, inferSchema=True).select(columns_to_read).filter(
                        condition)

                    # Convert DateTime to timestamp
                    filtered_df = filtered_df.withColumn('DateTime', F.to_timestamp('DateTime'))

                    # Create a key from the file name (excluding extension)
                    key = os.path.splitext(csv_file)[0]

                    # Add the DataFrame to the dictionary
                    dataframes_dict[key] = filtered_df

                except Exception as e:
                    # Handle other exceptions (e.g., invalid CSV file)
                    print(f"Error reading {csv_file}: {str(e)}")

            else:
                pass

        return dataframes_dict

    def process_dataframe_in_dictionary(result, processed_folder_path):
        try:
            for name, df in result.items():
                date = result[name].select('DateTime').first()[0].strftime('%Y%m%d')
                output_with_D_tran_type = df.groupBy('Merchant_Account_Nr').agg(
                    (F.count('*') * 50).alias('Merchant_Receivable'),
                    F.concat(F.lit('STAMP DUTY POS: '), F.lit(date), F.lit(' '), (F.count('*') + 1)).alias(
                        'Merchant_Account_Name'),
                    F.lit('D').alias('Transaction_Status'),
                    F.lit('').alias('Tran_ID'),
                    F.lit('').alias('Trxn_Category')
                )

                output_with_C_tran_type = spark.createDataFrame([(8153907530, output_with_D_tran_type.groupBy().agg(
                    F.sum('Merchant_Receivable').alias('amount')).select('amount').first()[0],
                                                                   'STAMP DUTY ', 'C', '', '')],
                                                                 ['Merchant_Account_Nr', 'amount', 'Merchant_Account_Name',
                                                                  'Transaction_Status', 'Tran_ID', 'Trxn_Category'])

                final_output = output_with_D_tran_type.union(output_with_C_tran_type)

                # Save the final output as CSV
                final_output.write.csv(f'{processed_folder_path}/{name}_processed', header=True, mode='overwrite')

        except Exception as e:
            print(f'The following error occurred => {e}')

        return 'Processing of all files completed'

    raw_direc = "raw_filepath"
    raw_folder_path = os.path.abspath(raw_direc)
    processed_direc = "processed_filepath"
    processed_folder_path = os.path.abspath(processed_direc)

    # Record the start time
    start_time = time.time()

    # Call read file function
    result = read_files_into_dictionary(raw_folder_path, processed_folder_path)

    # Call processing function
    process_dataframe_in_dictionary(result, processed_folder_path)
    print('Successfully completed')

    # Record the end time
    end_time = time.time()

    # Calculate the elapsed time
    elapsed_time = end_time - start_time

    print(f"Time taken: {elapsed_time:.5f} seconds")


if __name__ == "__main__":
    main()
