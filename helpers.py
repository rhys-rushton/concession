import pandas as pd
def getCsv(path):
    for f in path:
        f = r"%s" % f
        df = pd.read_csv(f, encoding='cp1252', low_memory=False)
    return df