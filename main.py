import pandas as pd
import numpy as np
import datetime as dt
import glob
import os
# Helper functions for script
import helpers
import items_to_filter as itf

# Load data from the the relevant folders
# NB the first two lines need to be changed so that they're not specific to remote computer
transaction_data_path = r'C:\Users\RRushton\Desktop\concession\transactions'
attendance_data_path = r'C:\Users\RRushton\Desktop\concession\attendance'
transaction_data_csv = glob.glob(os.path.join(transaction_data_path, "*.csv"))
attendance_data_csv = glob.glob(os.path.join(attendance_data_path, "*.csv"))

# Read the data from the two folders. 
transaction_data = helpers.getCsv(transaction_data_csv)
attendance_data = helpers.getCsv(attendance_data_csv)

#print(len(transaction_data))

# Sort transaction data by file number and service date. 
transaction_data = transaction_data.sort_values(by=['File #', 'ServDate'])

# Create data frame that has the number of attendances for a ptient on a given date
# Use file number and date to match billings on the same date for a particular patient. 
transaction_data_size = transaction_data.groupby(by=['File #', 'ServDate']).size()
# Below is a testing line to convert the dataframe with number of billings for a patient on a given day
# Just for testing purposes.
#transaction_data_size.to_csv('size.csv',index=True)

# Get the the attendances that have no 10990/have a 10990
transaction_data_no_concession = transaction_data[transaction_data.Item != '10990']
transaction_data_with_concession = transaction_data[transaction_data.Item == '10990']

# Cleaning data so that invoice number is a number and not a string.
transaction_data_no_concession['Inv #'] = pd.to_numeric(transaction_data_no_concession['Inv #'], errors ='coerce').fillna(-1).astype(np.int64)
transaction_data_with_concession['Inv #'] = pd.to_numeric(transaction_data_with_concession['Inv #'], errors ='coerce').fillna(-1).astype(np.int64)


# Get all attendances that don't have a 10990.
no_10990s = transaction_data_no_concession.merge(transaction_data_with_concession, on='Inv #', how='left', indicator=True)

# Map values based on merge values to make clear what status they are. 
indicator_values = {"left_only": "No 10990", "right_only": "No Matching Item for 10990", "both": "10990 Billed"}
no_10990s['_merge'] = no_10990s['_merge'].map(indicator_values)

# Potential 10990's is merge column value that doesn't contain '10990 billed'
potential_10990s = no_10990s[no_10990s._merge != '10990 Billed']

# Clean Up Attendance data and add DOB
# Here we are getting data that we need from attendance data dataframe. 
# We just want file number and D.O.B.
attendance_data_new = attendance_data[['File_Number', 'Date_Of_Birth']]
attendance_data_new = attendance_data_new.drop_duplicates()

# Merge the data of birth into the relevant patient.
potential_with_DOB = potential_10990s.merge(attendance_data_new, how='left', left_on='File #_x', right_on='File_Number')

# Calculate the age of the patient based on their date of birth.
# NB we are not using today's date - we are using the date that they attended the clinic. 
potential_with_DOB['Date_x'] = pd.to_datetime(potential_with_DOB['Date_x'],format='%d/%m/%Y')
potential_with_DOB['Date_Of_Birth'] = pd.to_datetime(potential_with_DOB['Date_Of_Birth'],format='%d/%m/%Y', errors='coerce')
potential_with_DOB['AGE'] = (potential_with_DOB.Date_x - potential_with_DOB.Date_Of_Birth)
potential_with_DOB['AGE'] = potential_with_DOB['AGE'] /np.timedelta64(1,'Y')

# Get everyone with a DOB that is greater than 65 or less than 16
potential_with_DOB['AGE'] = ((potential_with_DOB['AGE'] > 65) | (potential_with_DOB['AGE'] < 16))
not_age_eligible = potential_with_DOB[potential_with_DOB['AGE'] == False]
potential_with_DOB = potential_with_DOB[potential_with_DOB['AGE'] == True]

# Currently we have all of those who have absolutely no 10990's billed. 
# Now get everyone who is age eligble
# Find those who may be missing a 10990
# Merging the dataframe of those without 10990s onto the people who are in the date range.
transaction_data_no_concession_w_dob = transaction_data_no_concession.merge(attendance_data_new, how='left', left_on='File #', right_on='File_Number')

# Get those who are eligible 
# Below we are calculating the age of the patient at the time of the encounter.
transaction_data_no_concession_w_dob['ServDate'] = pd.to_datetime(transaction_data_no_concession_w_dob['ServDate'],format='%d/%m/%Y')
transaction_data_no_concession_w_dob['Date_Of_Birth'] = pd.to_datetime(transaction_data_no_concession_w_dob['Date_Of_Birth'],format='%d/%m/%Y', errors='coerce')
transaction_data_no_concession_w_dob['AGE'] = (transaction_data_no_concession_w_dob.ServDate - transaction_data_no_concession_w_dob.Date_Of_Birth)
transaction_data_no_concession_w_dob['AGE'] = transaction_data_no_concession_w_dob['AGE'] /np.timedelta64(1,'Y')
# Filter based on age
transaction_data_no_concession_w_dob['AGE'] = ((transaction_data_no_concession_w_dob['AGE'] >= 66) | (transaction_data_no_concession_w_dob['AGE'] < 16))
transaction_data_no_concession_w_dob = transaction_data_no_concession_w_dob[transaction_data_no_concession_w_dob['AGE'] == True]

# Get those who have a discrepency between number of non-10990s and 10990s billed
# We are doing this so that we can flag those that have maybe had one 10990 but are eligible for others
# on any one encounter. Using 'grouby' to get a count.
missing_some_10990 = transaction_data_no_concession_w_dob[['File #', 'Patient','Inv #', 'Item', 'ServDate', 'Date_Of_Birth']]
missing_some_10990['Num_Billings'] = missing_some_10990.groupby(by=['File #', 'ServDate']).transform('size')
non_10990_size = missing_some_10990
transaction_data_with_concession['ServDate'] = pd.to_datetime(transaction_data_with_concession['ServDate'],format='%d/%m/%Y')
transaction_data_with_concession_match = transaction_data_with_concession
transaction_data_with_concession_match['Num_Billings'] = transaction_data_with_concession.groupby(by=['File #', 'ServDate']).transform('size')
possible_missed_10990s = non_10990_size.merge(transaction_data_with_concession_match, how='left', on=['File #', 'ServDate', 'Num_Billings'], indicator=True)
possible_missed_10990s = possible_missed_10990s[possible_missed_10990s._merge != 'both']
potential_with_DOB['ServDate_x'] = pd.to_datetime(potential_with_DOB['ServDate_x'],format='%d/%m/%Y')
final = possible_missed_10990s.merge(potential_with_DOB, how='left', left_on =['File #', 'Inv #_x', 'ServDate', 'Item_x'], right_on=['File #_x', 'Inv #', 'ServDate_x', 'Item_x'])

# Clean up the data (removing uneccesary x's)
final = final[['File #', 'Patient_x_x', 'Inv #_x','Item_x', 'ServDate','Date_Of_Birth_x', 'Account Payer Type_x', 'Doc_x', 'Stf_x',  'Transaction Type_x', 'Transaction Status_x','Amount_x','Fee Type_x', 'Analysis Group_x']]
# Dictionary mapping old column names to new column names
column_mapping = {
    'File #': 'File',
    'Patient_x_x': 'Patient',
    'Inv #_x': 'Invoice',
    'Item_x': 'Item',
    'ServDate': 'Service Date',
    'Date_Of_Birth_x': 'Date of Birth',
    'Account Payer Type_x': 'Account Payer Type',
    'Doc_x': 'Document',
    'Stf_x': 'Staff',
    'Transaction Type_x': 'Transaction Type',
    'Transaction Status_x': 'Transaction Status',
    'Amount_x': 'Amount',
    'Fee Type_x': 'Fee Type',
    'Analysis Group_x': 'Analysis Group'
}

# Renaming columns using the .rename method
final.rename(columns=column_mapping, inplace=True)

# Columns to add for people working through billings.
columns_to_add = ['10990 Billed (Y/N)', 'Processed By (Initials)', 'Notes']

# Add columns to the DataFrame using .assign method
final = final.assign(**{column: None for column in columns_to_add})


# Filter out non medicare item numbers.
item_numbers_filtered = final[~final.Item.isin(itf.item_numbers_to_filter)]
billing_types_filtered = item_numbers_filtered[~item_numbers_filtered['Fee Type'].isin(itf.fee_types_to_filter)]

# Write final dataframe to csv
billing_types_filtered.to_csv('Possible Missed 10990\'s.csv', index = False)



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
transaction_data['File #'] = pd.to_numeric(transaction_data['File #'], errors ='coerce').fillna(-1).astype(np.int64)
non_concession_people = pd.merge(transaction_data, dsp_data, left_on='File #', right_on='FILE_NUMBER', how='inner')
print(non_concession_people)

