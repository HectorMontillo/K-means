import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler

df = np.array(pd.read_csv('iris.csv'))
#print(df)
scaler = StandardScaler()
scaler.fit(df)
#print("mean: ")
#print(scaler.mean_)
data = scaler.transform(df)
np.savetxt('data.csv', data, delimiter=',')