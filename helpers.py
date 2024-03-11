import glob as glob
import os as os
import magic as magic
import pandas as pd

# Function to read csv's from path argument (Can be multiple paths)
# Returns one whole dataframe from a csv
# Takes a glob iterable of path names
def getCsv(path):

    # Dataframe list that we concat if needed
    df_li = []
    
    for f in path:

        f = r"%s" % f

        with open(f, "rb") as file:
            data = file.read()
            print(chardet.detect(data))
            data_type = chardet.detect(data)
            data_string = data_type['encoding']
            data_string = data_string.lower()
            file.close()

        if data_string == 'windows-1252':
            data_string = 'latin1'
            
        df = pd.read_csv(f, encoding=data_string, low_memory=False)  
        df_li.append(df)

    df_final = pd.concat(df_li, axis=0, ignore_index=True)

    return df_final




def createGlob (folder_location):
    csv_location = glob.glob(os.path.join(folder_location, "*.csv"))
    return csv_location

def createDataFrame(glob_var):
    
    # Dataframe list that we concat if needed
    df_li = []

    for f in glob_var:
    
        f = r"%s" %f
        print(f)
        with open(f, "rb") as file:
            data_string = getEncodingMagic(f)
            file.close()

        df = pd.read_csv(f, encoding=data_string, low_memory=False)  
        df_li.append(df)

    df_final = pd.concat(df_li, axis=0, ignore_index=True)

    return df_final

def getEncodingMagic(file_path):
    magic_var = magic.Magic(mime_encoding=True).from_file(file_path)
    
    if magic_var == 'binary' or magic_var == 'us-ascii' or 'windows-1252':
        magic_var = 'latin1'
    return magic_var