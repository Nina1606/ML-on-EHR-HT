# Master Thesis Digital Health 

## Predicting Hypertension Complications on EHR data



### A. Cohort Extraction

**1. Hypertension Phenotype**
- With the notebook in the hypertension (HT) phenotype folder, one can create the hypertensin cohort from the EHR data based on the phenotyping algorithm. There at least two out of three conditions (blood pressure medication, consecutive high blood pressure measurements, and ICD codes) have to be met in order to classify as hypertensive patient. 

**2. Cohort Extraction**
- In the Master Thesis Cohort Extraction notebook, you take as input the Merged_Cases_Control_180 file from the HT Project to define the HT patients (131k HT patients with 540 Total Record Time & 15 encounters - data entries). 
- As output ones gets the cohort with HT before 2013 with 61k patients. 
- For each complication, create the cohorts for Cerebro, Heart, Renal and All3 combined
- In the Complications_after_HT_diagnosed notebook, check if complications exist for the first time after HT onset occurs
- Results are saved in the Complications folder
- To check the age distribution, the notebook for age distribution for HT onset & complication age should be used 

### B. Feature Extraction

**3. Unsupervised Feature Extraction** with python scripts for entire HT cohort- for min, median, max values return
- Create Unsupervised_Cohorts_Creation_for_feature_extraction 
- Use the output cohort: Unsueprvised_All for python files
- Unsupervised_data_fetch (description below)
- Merge unsupervised feature (description below)
- Clean data frames with notebook from Consolidates Cases
- Out: Clean Unsupervised Dataframes


**4. Time series data extraction**
- Extract TimeSeries, load merged cases MRNs with threshold
- Select those parameters that were received by unsupervised feature extraction
- Take them as input for time series data
- Store in Timeseries-Fetched-Data
- TimeSeries Exploration Notebook to make sure that 'time_delta_in_days'] < 0] -> 0 is the timepoints when HT has been detected 
- Age_in_days = HT_onset; 
- Time delta = delta between HT onset & complication event taking place
- Notebook Timeseries preparation should be used to prepare the data as input for deep learning (create sequences for dynamic data)

**4a. Extra Blood Pressure Data**
- Notebook Extra Blood Pressure Data from HT_onset until 30 days before complication onset
    - Difference between HT_onset and complication_onset of at least 30 days to ensure that BP data of at least 1 month before is taken into consideration

**5. Case-Control Creation**
- Add demographic notebook to include some demographics in the cohorts
- One notebook to split intro retro & pro, into cases & controls and to prepare for ML_pipeline
- Out folder: For_ML_Pipeline


### C. Machine Learning

**6. Machine Learning**
- Use ML_pipeline folder - pipeline for ML try with parameters, classifier, KN fold & pro/retro split
- ML Unsupervised (more manual with Hyperoptimization & GS) + ML including HP & GS
- Feature importance notebooks
- Plots folder: for SHAP plots 


### D. Deep Learning

**7. Deep Learning - LSTM**
- Different LSTM notebook for each cohort
- Takes in: static (drug and diagnosis) and dynamic (lab value, vital signs, procedures) time series data and trains LSTM


--------------------------

##### For the Unsupervised Feature Extraction Part and as pre-step for the TimeSeries Extraction:<h5>

To fetch and save data begin by creating a tree like structure as below
![Folder Structure](folder_structure.jpeg)

----------------------------------------

##### Fetching the data

To fetch data execute the 'unsupervised_data_fetch.py' file with four extra command line arguments as mentioned below

- Param 1. Whether it's a case or control('True' for Cases and 'False' for Controls)
- Param 2. Type of data to be fetched(i.e. 'Drug','Diagnosis','VitalSign','LabValue' or 'Procedure')
- Param 3. Duration for which the data is to be fetched (i.e. 180 or 365 or whatever value you want)
- Param 4. Threshold for the configuration(i.e.  0.1, 0.5)

the command should look like this

`python unsupervised_data_fetch.py True Drug 365 0.1`

----------------------------------------

##### Merging the unsupervised fetched data

To merge the fetched data execute the 'merge_unsupervised_feature.py' file with three extra command line arguments as mentioned below

- Param 1. Whether it's a case or control('True' for Cases and 'False' for Controls)
- Param 2. Type of data to be fetched(i.e. 'Drug','Diagnosis','VitalSign','LabValue' or 'Procedure')
- Param 3. Duration for which the data is to be fetched (i.e. 180 or 365 or whatever value you want)

the command should look like this

`python merge_unsupervised_feature.py True Drug 365`

All the merged data will get saved in the respective 'Consolidated' folder under cases or controls. 

----------------------------------------

##### Merging cases and controls (if needed) of the consolidated data to a final parquet file

Now, to merge all the data into a final parquet file we have to execute the 'merge_cases_controls.py' file with just one command line arguments as mentioned below

- Param 1. Duration for which the data is fetched (i.e. 180 or 365 or whatever value you want)

the command should look like this

`python merge_cases_controls.py 365`

The final merged data will get saved in the 'Parquets/Final' folder.


