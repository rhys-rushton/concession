import pandas as pd
import numpy as np
import datetime as dt
import glob
import os
import helpers

transaction_data_path = r'C:\Users\RRushton\Desktop\concession\transactions'
attendance_data_path = r'C:\Users\RRushton\Desktop\concession\attendance'

transaction_data_csv = glob.glob(os.path.join(transaction_data_path, "*.csv"))
attendance_data_csv = glob.glob(os.path.join(attendance_data_path, "*.csv"))

print(transaction_data_csv)
print(attendance_data_csv)

transaction_data = helpers.getCsv(transaction_data_csv)
attendance_data = helpers.getCsv(attendance_data_csv)
#transaction_data = pd.read_csv(r'C:\Users\RRushton\Desktop\concession\TR Single Line Details KRM 01012021-13012023.csv', encoding='cp1252', low_memory=False)
#attendance_data = pd.read_csv(r'C:\Users\RRushton\Desktop\concession\Attendance Details (1 Jan 2021 - 31 Dec 2022).csv', encoding='cp1252', low_memory=False)


transaction_data = transaction_data.sort_values(by=['File #', 'ServDate'])
#print(transaction_data)
transaction_data_size = transaction_data.groupby(by=['File #', 'ServDate']).size()
#print(transaction_data_size)


transaction_data_size.to_csv('size.csv',index=True)

transaction_data_no_concession = transaction_data[transaction_data.Item != '10990']
transaction_data_with_concession = transaction_data[transaction_data.Item == '10990']

transaction_data_no_concession['Inv #'] = pd.to_numeric(transaction_data_no_concession['Inv #'], errors ='coerce').fillna(-1).astype(np.int64)
transaction_data_with_concession['Inv #'] = pd.to_numeric(transaction_data_with_concession['Inv #'], errors ='coerce').fillna(-1).astype(np.int64)


# Merge everyone and get the people who don't 10990's 
no_10990s = transaction_data_no_concession.merge(transaction_data_with_concession, on='Inv #', how='left', indicator=True)



indicator_values = {"left_only": "No 10990", "right_only": "No Matching Item for 10990", "both": "10990 Billed"}
no_10990s['_merge'] = no_10990s['_merge'].map(indicator_values)

potential_10990s = no_10990s[no_10990s._merge != '10990 Billed']

#print(no_10990s)
#print(potential_10990s)


# Clean Up Attendance data and add DOB
attendance_data_new = attendance_data[['File_Number', 'Date_Of_Birth']]
attendance_data_new = attendance_data_new.drop_duplicates()

potential_with_DOB = potential_10990s.merge(attendance_data_new, how='left', left_on='File #_x', right_on='File_Number')
###Calclulating age
potential_with_DOB['Date_x'] = pd.to_datetime(potential_with_DOB['Date_x'],format='%d/%m/%Y')
potential_with_DOB['Date_Of_Birth'] = pd.to_datetime(potential_with_DOB['Date_Of_Birth'],format='%d/%m/%Y', errors='coerce')


potential_with_DOB['AGE'] = (potential_with_DOB.Date_x - potential_with_DOB.Date_Of_Birth)
potential_with_DOB['AGE'] = potential_with_DOB['AGE'] /np.timedelta64(1,'Y')

potential_with_DOB['AGE'] = ((potential_with_DOB['AGE'] >= 66) | (potential_with_DOB['AGE'] < 16))

potential_with_DOB = potential_with_DOB[potential_with_DOB['AGE'] == True]


#print(potential_with_DOB)


# Currently we have all of those who have absolutely no 10990's billed. 

# Now get everyone who is age eligble
# Find those who may be missing a 10990

# Get age 
transaction_data_no_concession_w_dob = transaction_data_no_concession.merge(attendance_data_new, how='left', left_on='File #', right_on='File_Number')
#print(transaction_data_no_concession_w_dob)
# Get those who are eligible 
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



