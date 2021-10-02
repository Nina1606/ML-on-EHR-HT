import glob
import sys

import pandas as pd
import pyarrow.parquet as pq

case = (
    True if sys.argv[1] == "True" else False
)  # Change for case and controls--> 'True' for case and 'False' for controls
type = str(sys.argv[2])  # type of data to be fetched
duration = str(sys.argv[3])  # duration of the data fetch

CasePath = (
    "Parquets/Cases/Unsupervised_Fetched_Data/" + duration + "/"
    if case
    else "Parquets/Controls/Unsupervised_Fetched_Data/" + duration + "/"
)

CaseStr = "_Cases_" if case else "_Controls_"

dir = CasePath + type + CaseStr
list = []
for name in glob.glob(dir + "*"):
    case_temp = pq.read_table(name).to_pandas()
    list.append(case_temp)

Cases = pd.concat(list, join="inner")

Cases_all = Cases.set_index("medical_record_number")
if case:
    Cases_all.insert(0, "HT", "1")
else:
    Cases_all.insert(0, "HT", "0")

# Cases_all= Cases_all.replace(to_replace=True, value=1.0, regex=True)
# Cases_all = Cases_all.replace({None: 0.0})

# print(len(Cases_all.index.unique()))

savePath = (
    "Parquets/Cases/Consolidated/" + duration + "/"
    if case
    else "Parquets/Controls/Consolidated/" + duration + "/"
)

Cases_all.to_parquet(savePath + type + CaseStr + "Unsupervised_All.parquet")
