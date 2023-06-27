import pandas as pd


def getCsv(path):
    
    df_li = []

    for f in path:
        f = r"%s" % f
        df = pd.read_csv(f, encoding='cp1252', low_memory=False)
        df_li.append(df)
    
    df_final = pd.concat(df_li, axis=0, ignore_index=True)
    return df_final



