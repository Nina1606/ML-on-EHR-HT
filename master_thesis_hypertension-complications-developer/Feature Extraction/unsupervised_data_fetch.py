import math
import os
import sys
from functools import reduce
import fiber

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from fiber import fiber
from fiber.cohort import Cohort
from fiber.condition import (
    Diagnosis,
    Drug,
    Encounter,
    LabValue,
    Measurement,
    MRNs,
    Patient,
    Procedure,
    TobaccoUse,
    VitalSign,
)
from fiber.storage import yaml as fiberyaml
from fiber.utils import Timer

case = (
    True if sys.argv[1] == "True" else False
)  # Change for case and controls--> 'True' for case and 'False' for controls
type = str(sys.argv[2])  # type of data to be fetched
duration = str(sys.argv[3])  # duration of the data fetch
threshold = float(sys.argv[4])  # threshold for the fetching of data


def config(type, duration, threshold):
    switcher = {
        "Drug": {
            Drug(): {
                "window": (-math.inf, -int(duration)),
                "pivot_table_kwargs": {
                    "columns": ["code"],
                    "aggfunc": {"code": "count"},
                },
                "threshold": threshold,  # 0.1
            }
        },
        "Diagnosis": {
            Diagnosis(): {
                "window": (-math.inf, -int(duration)),
                "pivot_table_kwargs": {"columns": ["code"], "aggfunc": {"code": "any"}},
                "threshold": threshold,  # 0.1
            }
        },
        "VitalSign": {
            VitalSign(): {
                "window": (-math.inf, -int(duration)),
                "pivot_table_kwargs": {
                    "columns": ["code"],
                    "aggfunc": {"numeric_value": ["min", "median", "max"]},
                },
                "threshold": threshold,  # 0.5
            }
        },
        "LabValue": {
            LabValue(): {
                "window": (-math.inf, -int(duration)),
                "pivot_table_kwargs": {
                    "columns": ["test_name"],
                    "aggfunc": {"numeric_value": ["min", "median", "max"]},
                },
                "threshold": threshold,  # 0.1
            }
        },
        "Procedure": {
            Procedure(): {
                "window": (-math.inf, -int(duration)),
                "pivot_table_kwargs": {
                    "columns": ["code"],
                    "aggfunc": {"code": "count"},
                },
                "threshold": threshold,  # 0.1
            }
        },
    }
    return switcher.get(type, "Invalid Value")


DEFAULT_PIVOT_CONFIG = config(type, duration, threshold)

# Reading cohort as dataframe
Case = pd.read_pickle('Unsupervised_Cohorts/Unsupervised_ALL.pkl')
Control = pd.read_pickle('Unsupervised_Cohorts/Unsupervised_Cohorts_Final/Controls_arterial_disease_unsupervised.pkl')


CaseOrControl = Case if case else Control

CaseOrControlPath = "Cases" if case else "Controls"
print(case)
savePath = (
    "Parquets/"
    + CaseOrControlPath
    + "/Unsupervised_Fetched_Data/"
    + duration
    + "/"
    + type
    + "_"
    + CaseOrControlPath
    + "_"
)
print(savePath)

# running through entire cohort in batches and saving each batch as parquet
for limit in range(0, len(Case), 5000):
    print("Begin of iteration: " + str(limit))

    temp = Case[limit : (limit + 5000)]
    p_condition = MRNs(temp)  # how to create cohort from dataframe
    cohort = Cohort(p_condition)
    try:
        result = cohort.get_pivoted_features(pivot_config=DEFAULT_PIVOT_CONFIG)
        result.to_parquet(savePath + str(limit + 5000) + ".parquet")
        print(result.shape)
    except Exception as e:
        print(e)
