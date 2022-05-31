import argparse
import pandas as pd
import arrow
import json


def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="C:/Users/Admin/PycharmProjects/python-assignment-level2/input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="C:/Users/Admin/PycharmProjects/python-assignment-level2/input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="C:/Users/Admin/PycharmProjects/python-assignment-level2/input_data/starter/transactions/d=replacedate/transactions.json")

    parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
    return vars(parser.parse_args())

def main():
    params = get_params()
    dateget = list(get_week('2019-01-03'))
    week_list = []
    for i in dateget:
       week_list.append(i.format('YYYY-MM-DD'))
    print(week_list)
    read_data(params,week_list)

def get_week(dt):
    mydate = arrow.get(dt)
    start = mydate.floor('week')
    end = mydate.ceil('week')
    return arrow.Arrow.range('day', start, end)

def read_data(params,week_list):
    customer_data = pd.read_csv(params['customers_location'])
    products_data = pd.read_csv(params['products_location'])
    transactiondf = pd.DataFrame()
    for i in week_list:
        transaction_location = params['transactions_location'].replace('replacedate',i)
        transactions_data = pd.read_json(transaction_location, lines=True)
        transactiondf = pd.concat([transactiondf,transactions_data])

   # for row in transactiondf:
        #transaction_basket = (transactiondf['basket']).to_json
    df = pd.json_normalize(transactiondf, record_path=['basket'])
    print(df)
        #length = row['basket']length

        #print(params['transactions_location'].replace('replacedate',i))
       # print(transactions_data.count())
   # print(transactiondf.count())
    #print(customer_data.info())
    #print(products_data.info())



#customer_id, loyalty_score, product_id, product_category, purchase_count
if __name__ == "__main__":
    main()
