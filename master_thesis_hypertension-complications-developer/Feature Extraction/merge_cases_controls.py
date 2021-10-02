import glob
import sys
import time
from functools import reduce

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

duration = str(sys.argv[1])  # duration of the data to be saved

CasePath = "Parquets/Cases/Consolidated/" + duration + "/"
ControlPath = "Parquets/Controls/Consolidated/" + duration + "/"

dir = CasePath
Cases_list = []
for name in glob.glob(dir + "*"):
    case_temp = pq.read_table(name).to_pandas()
    case_temp.reset_index(inplace=True)
    Cases_list.append(case_temp)
Cases_merged = reduce(
    lambda left, right: pd.merge(
        left, right, how="inner", on="medical_record_number", suffixes=("", "_y")
    ),
    Cases_list,
)
Cases_merged.drop(
    Cases_merged.filter(regex="_y$").columns.tolist(), axis=1, inplace=True
)

#list_of_dfs = [Cases_merged, Controls_merged]
list_of_dfs = [Cases_merged] #only for one cohort extraction

common_cols = list(set.intersection(*(set(df.columns) for df in list_of_dfs)))

#Final_df = pd.concat([Cases_merged[common_cols], Controls_merged[common_cols]])

Final_df = pd.concat([Cases_merged[common_cols]]) #only for one cohort extraction

print("Final dataframe shape:", Final_df.shape)
print("Count of Nan values:", Final_df.isnull().sum().sum())

"""This part is to check uniqueness of columns"""
# columns = list(Final_df.columns)
# print('Total columns:',len(columns))
# x = np.array(columns)
# print('Unique columns:',len(np.unique(x)))

# for i in columns:
#     print(i)
#     time.sleep(0.6)


Final_df.to_parquet(
    "Parquets/Final/" + duration + "/Merged_Cases_" + duration + ".parquet"
)
