
import pandas as pd
import requests
from sqlalchemy import create_engine
import psycopg2
from time import time
import os
import gzip
import shutil
def main():
    engine= create_engine("postgresql://ahmed:1234@PG:5432/green_taxi")
    try:
        url='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz'
        input_file = 'green_tripdata_2019-10.csv.gz'
        output_file = 'green_tripdata_2019-10.csv'
        zone_file = 'taxi_zone_lookup.csv'

        if not os.path.exists(input_file):
            print("Downloading green tripdata file...")
            response = requests.get(url)
            with open('green_tripdata_2019-10.csv.gz', 'wb') as f:
                    f.write(response.content)
                    response.raise_for_status()
        else:
            print("Green tripdata file already exists.")
            
        if not os.path.exists(output_file):
            with gzip.open(input_file,'rb') as f_in:
                with open(output_file,'wb') as f_out:
                    shutil.copyfileobj(f_in,f_out)
        else:
            print("CSV file already extracted.")
                
        df=pd.read_csv(output_file,low_memory=False)
        print(f"Total rows in CSV: {len(df)}")
        
        # print(pd.io.sql.get_schema(df,name='green_taxi',con=engine))
        first_chunk = True 
        data_taxi_iterat=pd.read_csv(output_file,iterator=True, chunksize=10000)
        while True:
            start_time=time()
            try:
                next_data=next(data_taxi_iterat)
                next_data['lpep_pickup_datetime'] = pd.to_datetime(next_data['lpep_pickup_datetime'])
                next_data['lpep_dropoff_datetime'] = pd.to_datetime(next_data['lpep_dropoff_datetime'])
                if first_chunk:
                        next_data.to_sql(name='green_taxi', con=engine, if_exists='replace', index=False)
                        first_chunk = False
                else:
                    next_data.to_sql(name='green_taxi', con=engine, if_exists='append', index=False)
                    total_rows=pd.read_sql('select count(*) from green_taxi',con=engine)
                    print(f"Rows in database: {total_rows.iloc[0, 0]}")
                    end_time=time()
                    print('inserted another chunk....,took %.3f'%(end_time-start_time))
            except StopIteration:
                    print("All chunks processed successfully.")
                    break        
    except Exception as e:
        print(f"Failed to process green tripdata: {e}")
    
    try:
        if not os.path.exists(zone_file):
            zone_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'
            zone_response = requests.get(zone_url)
            zone_response.raise_for_status()  
            
            with open(zone_file, 'wb') as f:
                f.write(zone_response.content)  
            print("File downloaded successfully")
        else:
            print("Taxi zone lookup file already exists.")


        if os.path.exists(zone_file):
            data_zone = pd.read_csv(zone_file)
            print(f"total_size_befor_load {len(data_zone)}")
        
            data_zone.to_sql(name='zone', con=engine, if_exists='replace', index=False) 
            if not data_zone.empty: 
                 zone_check=pd.read_sql('select count(*) from zone',con=engine)
                 print(f"total_size_after_load {zone_check}")
            else:
                print("Zone file is empty.")
            print("Data inserted into SQL successfully")

    except Exception as e:
        print(f"Failed to create zone table: {e}")

if __name__=='__main__':
    main()
     



