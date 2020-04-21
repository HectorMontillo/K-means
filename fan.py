import zmq
import sys
import pandas as pd
import numpy as np
import math
from tabulate import tabulate
import matplotlib.pyplot as plt
plt.rcParams['axes.facecolor'] = 'black'
import random

class K_means_Fan:
	def __init__(self,data_set,k,sink_address='localhost:5565',port_worker='5555',port_sink='5556',max_iterations=1000, n=10,tolerance=0.0):
		#Initial data
		self.data_set = data_set 
		self.k = k #number of clusters
		self.n = n #number of samples to send to workers
		self.max_iterations = max_iterations
		self.tolerance = tolerance

		#data
		self.dimentions = self.get_dimentions()
		self.number_samples = self.get_number_samples()
		self.number_tasks = math.ceil(self.number_samples/self.n)

		#state vars 
		self.clusters = self.init_clusters()
		self.new_clusters = None
		self.iteration = 0
		self.finished = False

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

	def get_number_samples(self):
		with open(self.data_set) as f:
			for i, _ in enumerate(f):
				pass
		return i

	def get_dimentions(self):
		with open(self.data_set) as f:
			return len(f.readline().split(','))

	def read_data(self, rows):
		samples = list()
		for i,f in enumerate(open(self.data_set)):
			if i in rows:
				data = list(map(lambda x: float(x),f.strip('\n').split(',')))
				samples.append(data)
		return samples

	def begin_iteration(self):
		self.sink_push.send_json({
				"finished": self.finished,
				"n_tasks":self.number_tasks,
				"n_clusters": self.k,
				"dim": self.dimentions
			})

	def distribute_load(self):
		ci = 1
		cj = self.n
		for i in range(self.number_tasks):
			row = [ci,cj]
			self.workers.send_json({
				"finished": self.finished,
				"clusters":self.clusters,
				"rows": row
			})
			ci += self.n
			cj += self.n

	def finish_iterations(self):
		if(self.iteration >= self.max_iterations):
			print("Iteration limit reached!")
			self.finished = True
		elif self.compare_clusters():
			print("Tolerance acepted!")
			self.finished = True
		else:
			self.iteration+=1
		self.clusters = self.new_clusters

	def init_clusters(self):
		return self.read_data(random.sample(range(1, self.number_samples+1), self.k))

	def compare_clusters(self):
		for i in range(len(self.clusters)):
			for j in range(len(self.clusters[0])):
				if(abs(self.clusters[i][j]-self.new_clusters[i][j]) > self.tolerance):
					return False
		return True

	def print(self,matrix,headers=[]):
		print(tabulate(matrix,headers=headers,showindex="always",tablefmt="github"))

	def run(self):
		print("Press enter when workers and sink are ready...", end='')
		input()
		print(f"Number of samples: {self.number_samples}, Number of task per iteration: {self.number_tasks}")
		print("Initial Clusters:")
		#self.print(self.clusters)

		while True:
			
			# send sink the number of results
			self.begin_iteration()

			# send workers the taks
			self.distribute_load()

			# recive the new clusters from sink
			if self.finished:
				clasification = self.sink_pull.recv_json()["clasification"]
				break
			else:
				print(f"Iteration: {self.iteration}")
				self.new_clusters = self.sink_pull.recv_json()["new_clusters"]
				#self.print(self.new_clusters)

			# compare clusters to stop
			self.finish_iterations()
			
		
		if self.dimentions > 2:
			self.check_clasification(clasification)
		else:
			self.plot_clasification(clasification)

	def plot_clasification(self,clasification):
		df = pd.read_csv(self.data_set)
		x = np.array(df[['x']])
		y = np.array(df[['y']])
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
		colors = ['b','g','r','c','m','y','w','b','g','r','c','m','y','w','b','g','r','c','m','y']
		color_label = [None]*df.shape[0]
		print(len(color_label))
		for i,c in enumerate(clasification):
			for p in c:
				color_label[p-1] = colors[i] 
		
	
		plt.scatter(x, y, c=color_label, s=1)
		plt.scatter(c_x, c_y, c='w',s=50, marker="x")
		plt.show()
	
	def check_clasification(self,clasification):
		df = list()
		with open('iris2.csv') as f:
			for r in f.readlines():
				df.append(r.strip('\n').split(',')[-1])
		class_dict = dict()
		for i,c in enumerate(clasification):
			#print(f"Cluster: {i}")
			class_dict[f"cluster_{i}"] = {"count":len(c)}
			for s in c:
				try:
					class_dict[f"cluster_{i}"][f"{df[s]}"] += 1
				except:
					class_dict[f"cluster_{i}"][f"{df[s]}"] = 1

		self.print(class_dict.items())
		
	
	
if __name__ == "__main__":
	try:
			data_set = sys.argv[1]
			k = int(sys.argv[2])
	except IndexError:
			print("Mising arguments!")

	fan = K_means_Fan(data_set,k)
	fan.run()
