import pandas as pd
import chardet

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


