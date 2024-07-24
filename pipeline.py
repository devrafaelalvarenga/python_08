from etl import calculate_sales

path: str = 'data'
output_format: list = ['csv', 'parquet']

calculate_sales(path, output_format)