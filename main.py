import pandas as pd
import numpy as np
import datetime as dt
import glob
import os
# Helper functions for script
import helpers

# Load data from the the relevant folders
# NB the first two lines need to be changed so that they're not specific to remote computer
transaction_data_path = r'C:\Users\RRushton\Desktop\concession\transactions'
attendance_data_path = r'C:\Users\RRushton\Desktop\concession\attendance'
transaction_data_csv = glob.glob(os.path.join(transaction_data_path, "*.csv"))
attendance_data_csv = glob.glob(os.path.join(attendance_data_path, "*.csv"))

# Read the data from the two folders. 
transaction_data = helpers.getCsv(transaction_data_csv)
attendance_data = helpers.getCsv(attendance_data_csv)


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
potential_with_DOB = potential_with_DOB[potential_with_DOB['AGE'] == True]

# Currently we have all of those who have absolutely no 10990's billed. 
# Now get everyone who is age eligble
# Find those who may be missing a 10990
# Merging the dataframe of those without 10990s onto the people who are in the date range.
transaction_data_no_concession_w_dob = transaction_data_no_concession.merge(attendance_data_new, how='left', left_on='File #', right_on='File_Number')

# Get those who are eligible 
# Below we are cleaning the data.
transaction_data_no_concession_w_dob['ServDate'] = pd.to_datetime(transaction_data_no_concession_w_dob['ServDate'],format='%d/%m/%Y')
transaction_data_no_concession_w_dob['Date_Of_Birth'] = pd.to_datetime(transaction_data_no_concession_w_dob['Date_Of_Birth'],format='%d/%m/%Y', errors='coerce')
transaction_data_no_concession_w_dob['AGE'] = (transaction_data_no_concession_w_dob.ServDate - transaction_data_no_concession_w_dob.Date_Of_Birth)
transaction_data_no_concession_w_dob['AGE'] = transaction_data_no_concession_w_dob['AGE'] /np.timedelta64(1,'Y')
transaction_data_no_concession_w_dob['AGE'] = ((transaction_data_no_concession_w_dob['AGE'] >= 66) | (transaction_data_no_concession_w_dob['AGE'] < 16))
transaction_data_no_concession_w_dob = transaction_data_no_concession_w_dob[transaction_data_no_concession_w_dob['AGE'] == True]
#print(transaction_data_no_concession_w_dob)
# Get those who have a discrepence between number of non-10990s and 10990s billed
missing_some_10990 = transaction_data_no_concession_w_dob[['File #', 'Patient','Inv #', 'Item', 'ServDate', 'Date_Of_Birth']]
#print(missing_some_10990)
missing_some_10990['Num_Billings'] = missing_some_10990.groupby(by=['File #', 'ServDate']).transform('size')
non_10990_size = missing_some_10990
print(non_10990_size)
transaction_data_with_concession['ServDate'] = pd.to_datetime(transaction_data_with_concession['ServDate'],format='%d/%m/%Y')
transaction_data_with_concession_match = transaction_data_with_concession
transaction_data_with_concession_match['Num_Billings'] = transaction_data_with_concession.groupby(by=['File #', 'ServDate']).transform('size')


print(transaction_data_with_concession_match)


possible_missed_10990s = non_10990_size.merge(transaction_data_with_concession_match, how='left', on=['File #', 'ServDate', 'Num_Billings'], indicator=True)
possible_missed_10990s = possible_missed_10990s[possible_missed_10990s._merge != 'both']

possible_missed_10990s.to_csv('Possible Missed.csv',index=True)
print(possible_missed_10990s)
print(potential_with_DOB)
potential_with_DOB['ServDate_x'] = pd.to_datetime(potential_with_DOB['ServDate_x'],format='%d/%m/%Y')
final = possible_missed_10990s.merge(potential_with_DOB, how='left', left_on =['File #', 'Inv #_x', 'ServDate', 'Item_x'], right_on=['File #_x', 'Inv #', 'ServDate_x', 'Item_x'])
print(final)
#final = final.drop(['File #', 'Patient_x_x', 'Inv #_x', 'ServDate','Date_Of_Birth_x', 'Account Payer Type_x', 'Doc_x', 'Stf_x',  'Transaction Type_x', 'Transaction Status_x','Amount_x','Fee Type_x', 'Analysis Group_x'], axis=1)

final = final[['File #', 'Patient_x_x', 'Inv #_x', 'ServDate','Date_Of_Birth_x', 'Account Payer Type_x', 'Doc_x', 'Stf_x',  'Transaction Type_x', 'Transaction Status_x','Amount_x','Fee Type_x', 'Analysis Group_x']]
# Dictionary mapping old column names to new column names
column_mapping = {
    'File #': 'File',
    'Patient_x_x': 'Patient',
    'Inv #_x': 'Invoice',
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

# Columns to add
columns_to_add = ['10990 Billed (Y/N)', 'Processed By ( Initials)', 'Notes']

# Add columns to the DataFrame using .assign method
final = final.assign(**{column: None for column in columns_to_add})

final.to_csv('Possible Missed 10990\'s.csv', index = True)



