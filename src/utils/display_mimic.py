import pandas

def display(df, limit=100):
    return df.limit(limit).toPandas()