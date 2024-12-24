import pandas as pd
df = pd.read_csv(r'D:\Projects\CBDTP\System Summary\ICDTripsDetailv3.csv')
df.to_parquet(r'D:\Projects\CBDTP\System Summary\ICDTripsDetailv3.parquet')