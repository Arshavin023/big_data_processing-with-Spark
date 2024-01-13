from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, lit

def main():
    def read_files_into_dataframe(spark, raw_folder_path, processed_folder_path):
        condition = col('LCY AMOUNT') >= 10000
        sheet_name = 'DETAILS'
        columns_to_read = ['MERCHANT BANK ACCOUNT', 'LCY AMOUNT', 'MERCHANT DEPOSIT BANKNAME',
                           'ACQCOUNTRY', 'TRANSACTION ID', 'TRANSACTION TYPE', 'SETTLEMENT DATE']

        dataframes_dict = {}

        raw_files = [file for file in os.listdir(raw_folder_path) if file.endswith('.xlsx')]
        processed_files = [os.path.splitext(file)[0] for file in os.listdir(processed_folder_path) if file.endswith('.csv')]

        for excel_file in raw_files:
            if (os.path.splitext(excel_file)[0] + '_processed') not in processed_files:
                file_path = os.path.join(raw_folder_path, excel_file)
                try:
                    df = spark.read.format('com.crealytics.spark.excel') \
                        .option('sheetName', sheet_name) \
                        .option('useHeader', 'true') \
                        .load(file_path, header=True)

                    filtered_df = df.filter(condition)
                    filtered_df = filtered_df.withColumn('SETTLEMENT DATE', col('SETTLEMENT DATE').cast('string'))

                    key = os.path.splitext(excel_file)[0]
                    dataframes_dict[key] = filtered_df
                except Exception as e:
                    print(f"Error reading {excel_file}: {str(e)}")
            else:
                pass

        return dataframes_dict

    def process_dataframe_in_dictionary(spark, result, processed_folder_path):
        try:
            for name, df in result.items():
                date = df.select('SETTLEMENT DATE').first()[0]
                output_with_D_tran_type = df.groupBy('MERCHANT BANK ACCOUNT').agg(
                    (count('LCY AMOUNT') * lit(50)).alias('amount'),
                    lit(f'STAMP DUTY : {date}').alias('narration'),
                    lit('D').alias('tran_type'),
                    lit('').alias('TRANSACTION ID'),
                    lit('').alias('TRANSACTION TYPE')
                )

                output_with_D_tran_type = output_with_D_tran_type.withColumnRenamed('MERCHANT BANK ACCOUNT', 'account_no') \
                    .withColumn('column4', lit('')) \
                    .withColumn('pal_account_code', lit(''))

                output_with_C_tran_type = spark.createDataFrame([(8152159070, output_with_D_tran_type.agg(sum('amount').alias('amount')).first()['amount'],
                                                                  'STAMP DUTY ', 'C', '', '')],
                                                                 ['account_no', 'amount', 'narration', 'tran_type', 'column4', 'pal_account_code'])

                final_output = output_with_D_tran_type.union(output_with_C_tran_type)

                if not os.path.exists(processed_folder_path):
                    os.makedirs(processed_folder_path)
                    final_output.write.csv(f'{processed_folder_path}/{name}_processed.csv', header=True, mode='overwrite')
                else:
                    final_output.write.csv(f'{processed_folder_path}/{name}_processed.csv', header=True, mode='overwrite')

        except Exception as e:
            print(f'The following error occurred => {e}')

        return 'processing of all files completed'

    spark = SparkSession.builder.appName("DataProcessing").getOrCreate()

    raw_direc = "C:/Users/uche.nnodim/Desktop/isw_upsl/upsl_raw_files/test"
    raw_folder_path = os.path.abspath(raw_direc)
    processed_direc = "C:/Users/uche.nnodim/Desktop/isw_upsl/upsl_process_files/test_processed"
    processed_folder_path = os.path.abspath(processed_direc)

    start_time = time.time()
    result = read_files_into_dataframe(spark, raw_folder_path, processed_folder_path)
    process_dataframe_in_dictionary(spark, result, processed_folder_path)
    print('successfully completed')
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Time taken: {elapsed_time:.5f} seconds")

    spark.stop()

if __name__ == "__main__":
    main()
