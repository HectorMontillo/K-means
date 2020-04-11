import zmq
import sys
import pandas as pd
import numpy as np
import math
from tabulate import tabulate
import matplotlib.pyplot as plt
plt.rcParams['axes.facecolor'] = 'black'

class K_means_Fan:
	def __init__(self,data_set,k,target_columns,sink_address,port_worker='5555',port_sink='5556',max_iterations=1000, n=10,tolerance=0.0000000001):
		#Initial data
		self.data_set_file = data_set
		self.data_set = self.read_data(data_set)[target_columns]
		self.k = k #number of clusters
		self.n = n #number of samples to send to workers
		self.target_columns = target_columns
		self.max_iterations = max_iterations
		self.tolerance = tolerance
		#self.init_samples = init_samples # number of samples to be read to calculate initial centroids

		#data
		self.number_samples = self.data_set.shape[0]
		self.number_tasks = math.ceil(self.number_samples/self.n)

		#Clusters
		self.clusters = None


		#Zqm push pull implementation
		self.port_worker = port_worker
		self.port_sink = port_sink
		self.sink_address = sink_address

		self.context = zmq.Context()
		self.workers = self.context.socket(zmq.PUSH)
		self.sink_push = self.context.socket(zmq.PUSH)
		self.sink_pull = self.context.socket(zmq.PULL)

		self.connect()
				
				#Initialize
	def connect(self):
		self.workers.bind(f"tcp://*:{self.port_worker}")
		self.sink_push.connect(f"tcp://{self.sink_address}")
		self.sink_pull.bind(f"tcp://*:{self.port_sink}")

	def read_data(self,data_set):
		df = pd.read_csv(data_set)
		return df.sample(frac=1)


	def run(self):
		print("Press enter when workers and sink are ready...", end='')
		input()
		#sys.stdout.flush()


		print(f"Number of samples: {self.number_samples}, Number of task per iteration: {self.number_tasks}")
		print("Initial Clusters:")

		self.initialize_clusters()
		iterations = 0
		finished = False

		while True:
			# send sink the number of results
			self.sink_push.send_json({
				"finished": finished,
				"n_tasks":self.number_tasks,
				"n_clusters": self.k,
				"dim": len(self.target_columns)
			})
			# send workers the taks
			ci = 0
			cj = self.n
			for i in range(self.number_tasks):
				#print(f"task:{i}, ci:{ci}, cj:{cj}")
				row = [ci,cj]
				self.workers.send_json({
					"finished": finished,
					"clusters":self.clusters,
					"rows": row
				})
				ci += self.n
				cj += self.n
			# recive the new clusters from sink
			if finished:
				clasification = self.sink_pull.recv_json()["clasification"]
				break
			else:
				print(f"Iteration: {iterations}")
				new_clusters = self.sink_pull.recv_json()["new_clusters"]
				self.print(new_clusters,self.target_columns)
			# compare clusters to stop
			if(iterations >= self.max_iterations):
				finished = True
				print("Iteration limit reached!")
			elif self.compare_clusters(new_clusters):
				finished = True
				print("Tolerance acepted!")
			else:
				iterations+=1
			self.clusters = new_clusters
		
		if len(self.target_columns) > 2:
			self.check_clasification(clasification)
		else:
			self.plot_clasification(clasification)

	def plot_clasification(self,clasification):
		df = pd.read_csv(self.data_set_file)
		x = np.array(df[[self.target_columns[0]]])
		y = np.array(df[[self.target_columns[1]]])
		c_x = np.array(self.clusters)[:,0]
		c_y = np.array(self.clusters)[:,1]
		'''
		b: blue
g: green
r: red
c: cyan
m: magenta
y: yellow
k: black
w: white
		'''
		colors = ['b','g','r','c','m','y','w','b','g','r','c','m','y','w','b','g','r','c','m','y',]
		color_label = [None]*df.shape[0]
		for i,c in enumerate(clasification):
			for p in c:
				color_label[p] = colors[i] 
		
	
		plt.scatter(x, y, c=color_label, s=1)
		plt.scatter(c_x, c_y, c='w',s=50, marker="x")
		plt.show()
	
	def check_clasification(self,clasification):
		df = pd.read_csv(self.data_set_file).iloc[:,-1].to_dict()
		class_dict = dict()
		for i,c in enumerate(clasification):
			#print(f"Cluster: {i}")
			class_dict[f"cluster_{i}"] = {"count":len(clasification[i])}
			for s in c:
				#print(f"Sample: {s}, Clasification: {df[s]}")
				try:
					class_dict[f"cluster_{i}"][f"{df[s]}"] += 1
				except:
					class_dict[f"cluster_{i}"][f"{df[s]}"] = 1

		self.print(class_dict.items())

	def initialize_clusters(self):
		self.clusters = self.data_set.sample(n=self.k).to_dict(orient='split')['data']
		self.print(self.clusters,self.target_columns)
		#print(self.clusters.to_dict(orient='split'))

	def compare_clusters(self, new_clusters):
		#map(,zip(self.clusters,new_clusters))
		#print(self.clusters, new_clusters)
		for i in range(len(self.clusters)):
			for j in range(len(self.clusters[0])):
				if(abs(self.clusters[i][j]-new_clusters[i][j]) > self.tolerance):
					return False
		return True

	def print(self,matrix,headers=[]):
		print(tabulate(matrix,headers=headers,showindex="always",tablefmt="github"))
	
if __name__ == "__main__":
		try:
				data_set = sys.argv[1]
				target_columns = sys.argv[2].strip('[]').split(',')
				k = int(sys.argv[3])
				sink_address = sys.argv[4]
		except IndexError:
				print("Mising arguments!")

		fan = K_means_Fan(data_set,k,target_columns,sink_address)
		fan.run()
