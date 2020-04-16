from sklearn.cluster import KMeans
import numpy as np
import pandas as pd

df = pd.read_csv('iris.csv', sep="\t")
print("Actual classification")
print(df.groupby('species').size())

X = np.array(df[["sepal_length","sepal_width","petal_length","petal_width"]])

kmeans = KMeans(n_clusters=3).fit(X)
centroids = kmeans.cluster_centers_
print("Clusters")
print(centroids)

# Predicting the clusters
labels = kmeans.predict(X)

print(np.unique(labels,return_counts=True))
