import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler

pddf = pd.read_csv('iris2.csv')
df = np.array(pddf[['sepal_length','sepal_width','petal_length','petal_width']])
clas = np.array(pddf[['species']])
#headers = np.array(pddf.columns.values)

scaler = StandardScaler()
scaler.fit(df)
data = scaler.transform(df)
dataclas = np.append(data, clas, axis=1)
np.savetxt('data.csv', data, delimiter=',')
np.savetxt('dataclas.csv', dataclas, delimiter=',', fmt='%s')
