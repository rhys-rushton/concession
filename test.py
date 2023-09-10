import glob
import helpers
import os
import numpy as np
import pandas as pd

###################Shitty style 
dsp_data_path = r'C:\Users\RRushton\Desktop\concession\DSP'
dsp_data_csv = glob.glob(os.path.join(dsp_data_path, "*.csv"))

# Read the data from the two folders. 
dsp_data = helpers.getCsv(dsp_data_csv)

print(dsp_data.dtypes)
dsp_data = dsp_data[['FILE_NUMBER','PATIENT_HEALTH_CARE_CARD', 'AGE']]
dsp_data['FILE_NUMBER'] = pd.to_numeric(dsp_data['FILE_NUMBER'], errors ='coerce').fillna(-1).astype(np.int64)
print(dsp_data)
dsp_data['PATIENT_HEALTH_CARE_CARD'] = pd.to_numeric(dsp_data['PATIENT_HEALTH_CARE_CARD'], errors ='coerce').fillna(-1).astype(np.int64)
#dsp_data = dsp_data[dsp_data.PATIENT_HEALTH_CARE_CARD.isnull()]
print(dsp_data)
dsp_data['AGE']= ((dsp_data['AGE'] <= 65) & (dsp_data['AGE'] >= 16))
dsp_data = dsp_data[dsp_data['AGE'] == True]
dsp_data['PATIENT_HEALTH_CARE_CARD']= dsp_data['PATIENT_HEALTH_CARE_CARD'] != -1
print(dsp_data)
dsp_data = dsp_data[dsp_data['PATIENT_HEALTH_CARE_CARD'] == False]
print(dsp_data)
print(dsp_data.dtypes)