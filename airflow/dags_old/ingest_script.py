import os

import pandas as pd
from sqlalchemy import create_engine
from time import time
import pyarrow.dataset as ds



def ingest_callable(user, password, host, port, db, table_name, parquet_file):
    print(table_name, parquet_file)

    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print('connection established successfully, inserting data...')

    
    dataset = ds.dataset(parquet_file, format="parquet")
    
    # for batch in dataset.to_batches(batch_size=100000):
    #     t_start = time()
        
    #     batch_df = batch.to_pandas()
    #     batch_df.to_sql(name='yellow_data', con=engine, index=False, if_exists='append')
        
    #     t_end = time()
    #     print('inserted another chunk, took %.3f second' % (t_end - t_start))
        
    #     break

    df_iter = dataset.to_batches(batch_size=100000)
    batch = next(df_iter)
    batch_df = batch.to_pandas()
    batch_df.to_sql(name=table_name, con=engine, index=False, if_exists='replace')

    while True: 
            t_start = time()

            try:
                batch = next(df_iter)
                batch_df = batch.to_pandas()
            except StopIteration:
                print("completed")
                break

            batch_df.to_sql(name=table_name, con=engine, index=False, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))
            