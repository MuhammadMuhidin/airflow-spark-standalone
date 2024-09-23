import pandas as pd

class SrcToLanding:
    def __init__(self, src_path, landing_path):
        self.src_path = src_path
        self.landing_path = landing_path
        self.files = ['customers', 'products', 'orders', 'shippings', 'payments', 'returns']
        self.data = self.read_src()

    def read_src(self):
        return {f'df_{file}': pd.read_csv(f'{self.src_path}/{file}.csv') for file in self.files}

    def merge_data(self):
        df_transactions = self.data['df_orders'].merge(self.data['df_customers'], on='customer_id')
        for key in ['df_shippings', 'df_payments', 'df_returns', 'df_products']:
            df_transactions = df_transactions.merge(self.data[key], on=key.split('_')[-1][:-1] + '_id')
        return df_transactions

    def save_landing(self):
        df_transactions = self.merge_data()
        df_transactions.to_parquet(self.landing_path+'/transactions_landing.parquet', engine='pyarrow')
        print(f"Data saved to {self.landing_path}/transactions_landing.parquet")

class LandingToHist:
    def __init__(self, landing_path, hist_path):
        self.landing_path = landing_path
        self.hist_path = hist_path
        self.data = self.read_landing()
    
    def read_landing(self):
        return pd.read_parquet(self.landing_path+'/transactions_landing.parquet', engine='pyarrow')
    
    def transform_data(self):
        df_transactions = self.data
        df_transactions = df_transactions.drop(columns=['order_id', 'customer_id', 'product_id',
                                                        'payment_id', 'return_id','shipping_id'])
        return df_transactions

    def save_hist(self):
        df_transactions = self.transform_data()
        df_transactions.to_parquet(self.hist_path+'/transactions_hist.parquet', engine='pyarrow')
        print(f"Data saved to {self.hist_path}/transactions_hist.parquet")
