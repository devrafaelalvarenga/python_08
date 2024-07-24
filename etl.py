import pandas as pd
import os
import glob

file = 'data'

def extract_data_json_file(path: str) -> pd.DataFrame:
    json_file = glob.glob(os.path.join(file, '*.json'))
    df_json_file = [pd.read_json(file) for file in json_file]
    df_json_file_concat = pd.concat(df_json_file, ignore_index=True)
    return df_json_file_concat

def calculate_total_sales(df: pd.DataFrame) -> pd.DataFrame:
    df['Total'] = df['Quantidade'] * df['Venda']
    return df

def load_data_file(df: pd.DataFrame, output_format: list):
    for format in output_format:
        if format == 'csv':
            df.to_csv('sales.csv')
        if format == 'parquet':
            df.to_parquet('sales.parquet')


def calculate_sales(path: str, output_format: list):
    df = extract_data_json_file(path=file)   
    df_sales = calculate_total_sales(df=df)     
    load_data_file(df=df, output_format=output_format)



if __name__ == "__main__":
   # file = 'data'
   # print(extract_data_json_file(path=file)) teste1
   # df = extract_data_json_file(path=file)   teste2
   # print(calculate_total_sales(df=df))      teste2
   # df = extract_data_json_file(path=file)   teste3
   # load_data_file(df=df, output_format=['csv', 'parquet'])   teste3
   print(calculate_sales('data', ['csv'])) 


