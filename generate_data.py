import os
import json
import time
import argparse
import pandas as pd
from faker import Factory
from random import randrange
from datetime import datetime
from collections import OrderedDict

CUSTOMER_NUM = 100
ORDER_NUM = 1000
ORDER_START_DATE = datetime(2021, 1, 1)
DATASET_ID = 'ecommerce'


class DataGenerator:
    def __init__(self):

        self.fake = Factory.create()

    def generate_customer(self, customer_id):
        '''
        Generate a single customer using the default config of fake.profile()
        '''
        customer_data = {}
        customer_data['customer_id'] = customer_id
        customer_data.update(self.fake.profile())
        
        return customer_data

    def generate_customers(self, customer_count):
        '''
        Batch generate customers.
        '''
        customers = []
        for i in range(1, customer_count + 1):
            customer_data = self.generate_customer(customer_id=i)
            customers.append(customer_data)
        return pd.DataFrame(customers)

    def generate_order(self, customer_id, order_start_date=ORDER_START_DATE):
        '''
        Generate a single order.
        '''
        amount = self.fake.pydecimal(
            left_digits=3, right_digits=2, positive=True)
        order_datetime = self.fake.date_time_between_dates(
            datetime_start=order_start_date,
            datetime_end=datetime.today(),
        )
        order_status = self.fake.random_element(elements=OrderedDict(
            [("Open", 0.20), ("Closed", 0.75), ("Cancelled", 0.05)]))
        order_data = {
            'customer_id': customer_id,
            'amount': float(amount),
            'order_status': order_status,
            'order_datetime': order_datetime
        }
        return order_data

    def generate_orders(self, order_count, customer_range):
        '''
        Batch generate orders.
        '''
        orders_data = []
        for _ in range(1, order_count + 1):
            customer_id = randrange(1, customer_range)
            order_data = self.generate_order(customer_id)
            orders_data.append(order_data)
        return pd.DataFrame(orders_data)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate fake data')
    subparser = parser.add_subparsers(dest='command')

    batch = subparser.add_parser('batch')
    batch.add_argument('--customer_count', type=int)
    batch.add_argument('--order_count', type=int)
    batch.add_argument('--project_id')

    stream = subparser.add_parser('stream')
    stream.add_argument('--customer_range', type=int)

    args = parser.parse_args()

    if args.command == 'batch':
        dg = DataGenerator()
        customers = dg.generate_customers(args.customer_count)
        customers.to_gbq(f'{DATASET_ID}.customers', project_id=args.project_id, if_exists='replace')

        orders = dg.generate_orders(
            order_count=args.order_count, customer_range=args.customer_count)
        orders.to_gbq(f'{DATASET_ID}.orders', project_id=args.project_id, if_exists='replace')

    if args.command == 'stream':
        dg = DataGenerator()
        while True:
            order_data = {
                'customer_id': randrange(1, args.customer_range),
                'amount': float(randrange(50000, 70000)) / 100,
                'order_status': 'Open',
                'order_datetime': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            message = json.dumps(order_data)
            command = f"gcloud --project={args.project_id} pubsub topics publish orders --message='{message}'"
            print(command)
            os.system(command)
            time.sleep(randrange(1, 5))
