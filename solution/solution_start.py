import argparse
import pandas as pd
import arrow
import json


def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="C:/Users/Admin/PycharmProjects/python-assignment-level2/input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="C:/Users/Admin/PycharmProjects/python-assignment-level2/input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="C:/Users/Admin/PycharmProjects/python-assignment-level2/input_data/starter/transactions/d=replace-date/transactions.json")
    parser.add_argument('--output_location', required=False, default="C:/Users/Admin/PycharmProjects/python-assignment-level2/output_data/outputs/output.json")
    return vars(parser.parse_args())

def main():
    params = get_params()
    get_weeks = list(get_week('2019-01-03'))
    week_list = []
    for i in get_weeks:
       week_list.append(i.format('YYYY-MM-DD'))
    process(params, week_list)

# Method to get week dates
def get_week(dt):
    mydate = arrow.get(dt)
    start = mydate.floor('week')
    end = mydate.ceil('week')
    return arrow.Arrow.range('day', start, end)
# Method to read the data of different formats
def read_data(location, type ):
    if type == 'csv':
        return pd.read_csv(location)
    elif type == 'json':
        return pd.read_json(location, lines=True)
    else:
        print("Please choose the file type as Json or csv")
        exit(1)

# Method for processing the data of Customers, Products, Transactions

def process(params,week_list):
    customer_data = read_data(params['customers_location'], 'csv')
    products_data = read_data(params['products_location'], 'csv')
    # Empty Dataframe
    transactiondf = pd.DataFrame()
    # for loop to read the week transaction data
    for i in week_list:
        transaction_location = params['transactions_location'].replace('replace-date', i)
        transactions_data = read_data(transaction_location, 'json')
        transactiondf = pd.concat([transactiondf, transactions_data])
    transdf = pd.DataFrame()
# For loop to get the list of records from the basket in transactions json file
    for index, row in transactiondf.iterrows():
        transaction_basket = (row['basket'])
        purchase_count = len(transaction_basket)
        for j in transaction_basket:
            transdf = pd.concat([transdf, pd.DataFrame.from_records([{'customer_id': row['customer_id'],
                                                                      'date_of_purchase': row['date_of_purchase'],
                                                                      'product_id': j['product_id'],
                                                                      'price': j['price'],
                                                                      'purchase_count': purchase_count}])])

    result = pd.merge(products_data, transdf, on='product_id', how='inner')
    customer_data_result = pd.merge(customer_data, result, on='customer_id', how='left')
    # Output of the final columns
    final_result = customer_data_result[['customer_id', 'loyalty_score', 'product_id', 'product_category', 'purchase_count']]
    # writing the final data to output_location
    final_result.to_json(params['output_location'], orient='records')

if __name__ == "__main__":
    main()
