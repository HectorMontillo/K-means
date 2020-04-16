#import pandas as pd
import numpy as np
def read_data(rows):
  samples = list()
  for i,f in enumerate(open('data.csv')):
    if i in rows:
      data = f.strip('\n').split(',')
      samples.append(data)
  return samples

print(np.array(read_data([1,2,3]),dtype=float))