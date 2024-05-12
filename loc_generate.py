
from concurrent.futures import ThreadPoolExecutor
from geopy.geocoders import Nominatim
import pyarrow.parquet as pq
import pandas as pd
import numpy as np
import os
from multiprocessing import Pool

import time

# Start time measurement
start_time = time.time()

geolocator = Nominatim(user_agent="MyApp")

table2 = pq.read_table('part-00000-9c75a8e6-de15-486e-9a9c-e45c28ad796e-c000.gz.parquet')

df = table2.to_pandas()
print(df.info())

st1 = time.time()
# Groupby lat, long and aggregate data
grouped_df = df.groupby(['LATITUDE', 'LONGITUDE']).agg(
    user_ids=('USERID', lambda x: list(x.unique())),
    user_count=('USERID', 'nunique'),
    start_time=('TIME', 'min'),
    end_time=('TIME', 'max')
).reset_index()

et1 = time.time()
execution_time=et1-st1
print(f"Groupby execution time: {execution_time:.2f} seconds")

#print(len(grouped_df))
print(grouped_df.head(3))
test_df=grouped_df[:30000]

# list of lat, long values
#address_list = []
#for i, loc in enumerate(loc_list):
#    print(i)
#    location = geolocator.reverse((loc[0], loc[1]), language="en")
#    address_list.append(location.raw['address'])

#def get_location_details(loc):
    #location = geolocator.reverse((loc[0], loc[1]), language="en", timeout=10)
    #if location:
    #return location.raw['address']
        

st2 = time.time()
# Assuming loc_list is your list of coordinates
loc_list = test_df[["LATITUDE", "LONGITUDE"]].values.tolist()
print("Loc List: "+str(len(loc_list)))

# Use ThreadPoolExecutor for concurrent processing
#with ThreadPoolExecutor() as executor:
#    address_list = list(executor.map(get_location_details, loc_list))
#num_cpus = os.cpu_count()  # Get the number of available CPUs
#with Pool(processes=num_cpus) as pool:
#    address_list = pool.map(get_location_details, loc_list)
address_list = []
for i, loc in enumerate(loc_list):
    if i%1000==0:
        print("============================================================="+str(i))
    location = geolocator.reverse((loc[0], loc[1]), language="en", timeout=2)
    address_list.append(location.raw['address'])
et2 = time.time()
execution_time=et2-st2
print(f"Gen loc execution time: {execution_time:.2f} seconds")

address_df = pd.DataFrame(address_list)
# Concatenate the address DataFrame with the original DataFrame
result_df = pd.concat([test_df, address_df], axis=1)
print(result_df.head(3))

# Save the DataFrame to a CSV file (replace 'data.csv' with your desired filename)
result_df.to_csv('loc_data.csv', index=False)


# End time measurement
end_time = time.time()

# Calculate execution time
execution_time = end_time - start_time

# Print the execution time
print(f"Code execution time: {execution_time:.2f} seconds")
