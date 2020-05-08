import zmq
import sys
import pandas as pd
import numpy as np
import math
from tabulate import tabulate
import matplotlib.pyplot as plt
plt.rcParams['axes.facecolor'] = 'black'
import random
from time import time
import os
import json

class K_means_Fan:
	def __init__(self,data_set,k=0,sink_address='localhost:5565',port_worker='5555',port_sink='5556',max_iterations=1000, n=100,tolerance=0.01,clusters_file=""):
		#Initial data
		self.data_set = data_set 
		self.k = k #number of clusters
		self.n = n #number of samples to send to workers
		self.max_iterations = max_iterations
		self.tolerance = tolerance

		#data
		#self.dimentions = self.get_dimentions()
		self.number_samples = self.get_number_samples()
		self.number_tasks = math.ceil(self.number_samples/self.n)

		#state vars 
		self.clusters_file = clusters_file
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
				
	def connect(self):
		self.workers.bind(f"tcp://*:{self.port_worker}")
		self.sink_push.connect(f"tcp://{self.sink_address}")
		self.sink_pull.bind(f"tcp://*:{self.port_sink}")

	def get_number_samples(self):
		print(".....Getting number of samples: ", end='')
		itime = time()
		with open(self.data_set) as f:
			for i, _ in enumerate(f):
				pass
		etime = time()
		print(f"{i+1}, time: {(etime-itime):.2f} seconds")
		return i + 1

	def get_dimentions(self):
		with open(self.data_set) as f:
			return len(f.readline().split(','))

	def read_data(self, rows):
		#print("initializing clusters: ", end="")
		samples = list()
		for i,f in enumerate(open(self.data_set)):
			f = f.strip()
			if i in rows:
				#data = list(map(lambda x: float(x),f.strip('\n').split(',')))
				#data = map(lambda t: t.split(' '),map(lambda t: t[1:-1], f.split(',')))
				data = dict(map(lambda t: map(int,t.split(' ')),map(lambda t: t[1:-1], f.split(','))))
				samples.append(data)
		return samples

	def begin_iteration(self):
		print(".....Begin iteration: Send number task to sink: ", end="")
		itime = time()
		self.sink_push.send_json({
				"k": self.k,
				"finished": self.finished,
				"n_tasks":self.number_tasks,
				#"n_clusters": self.k,
				#"dim": self.dimentions
			})
		etime = time()
		print(f"time: {(etime-itime):.2f} seconds")

	def save_clusters(self):
		clusters_file = "clusters.txt"
		print(".....Saving clusters: ", end="")
		itime = time()

		if os.path.exists(clusters_file):
			os.remove(clusters_file)
		
		with open(clusters_file, "a+") as f:
			for c in self.clusters:
				c = json.dumps(c)+"\n"
				f.write(c)
		etime = time()
		print(f"time: {(etime - itime):.2f}")

	def read_clusters(self):
		print(".....Reading clusters: ", end="")
		itime = time()

		clusters = []
		for f in open(self.clusters_file):
			f = f.strip()
			cluster = json.loads(f)
			clusters.append(cluster)

		etime = time()
		print(f"time: {(etime - itime):.2f}")
		print(clusters)
		return clusters

	def distribute_load(self):
		print(".....Distributing load to workers: ", end="")
		itime = time()
		ci = 0
		cj = self.n - 1
		for i in range(self.number_tasks):
			row = [ci,cj]
			self.workers.send_json({
				"iteration": self.iteration,
				"finished": self.finished,
				#"clusters":self.clusters,
				"rows": row
			})
			ci += self.n
			cj += self.n
		etime = time()
		print(f"time: {(etime-itime):.2f} seconds")

	def finish_iterations(self):
		print(".....Finishing iteration: ", end="")
		itime = time()
		if(self.iteration >= self.max_iterations):
			print("Iteration limit reached!")
			self.finished = True
		elif self.compare_clusters():
			print("Tolerance acepted!")
			self.finished = True
		else:
			self.iteration+=1
		self.clusters = self.new_clusters
		etime = time()
		print(f"time: {(etime - itime):.2f} seconds")

	def init_clusters(self):
		if self.clusters_file != "":
			clusters = self.read_clusters()
			self.k = len(clusters)
		else:
			print(".....Initialating clusters: ", end="")
			itime = time()
			clusters = self.read_data(random.sample(range(0, self.number_samples), self.k))
			etime = time()
			print(f"time: {(etime-itime):.2f} seconds")
		return clusters

	def compare_clusters(self):
		for i in range(len(self.clusters)):
			for k,v in self.clusters[i].items():
				try:
					if(abs(v - self.new_clusters[i][k]) > self.tolerance):
						return False
				except KeyError:
					return False
		return True
		'''
		for i in range(len(self.clusters)):
			for j in range(len(self.clusters[0])):
				if(abs(self.clusters[i][j]-self.new_clusters[i][j]) > self.tolerance):
					return False
		return True
		'''

	def print(self,matrix,headers=[]):
		print(tabulate(matrix,headers=headers,showindex="always",tablefmt="github"))

	def elbow_method(self):
		print("ELBOW METHOD: Press enter when workers and sink are ready...", end='')
		input()
		for k in range(2,int(math.sqrt(self.number_samples))):
			print("ELBOW METHOD:-------------------->")
			print(f"K = {k}")
			self.k = k
			self.clusters = self.init_clusters()
			self.new_clusters = None
			self.iteration = 0
			self.finished = False
			self.run(auto=True)
		

	def run(self, auto=False):
		if not auto:
			print("Press enter when workers and sink are ready...", end='')
			input()
		print(f"Number of task per iteration: {self.number_tasks}\n")
		
		#print(self.clusters)

		while True:
			print(f"Iteration: {self.iteration}")
			iterTime = time()
			# send sink the number of results
			self.begin_iteration()

			# save cluster in file
			self.save_clusters()

			# send workers the taks
			self.distribute_load()

			# recive the new clusters from sink
			print(".....Waiting response from sink")
			if self.finished:
				results = self.sink_pull.recv_json()
				print("Result saved:",results["file"])
				print("INERTIA:", results["inertia"])
				break
			else:
				
				self.new_clusters = self.sink_pull.recv_json()["new_clusters"]
				#self.print(self.new_clusters)

			# compare clusters to stop
			self.finish_iterations()
			eterTime = time()
			print(f"Iteration: time: {((eterTime-iterTime)/60):.2f} minutos")
		
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
			k = sys.argv[2]
			try:
				k = int(k)
				print("Loading initial data: ")
				itime = time()
				fan = K_means_Fan(data_set,k=k)
				etime = time()
				print(f"Loading initial data: time: {(etime-itime):.2f} seconds\n")
				fan.elbow_method()
				#fan.run()
			except ValueError:
				if not os.path.exists(k):
					raise Exception("The cluster file does not exist")

				print("Loading initial data: ")
				itime = time()
				fan = K_means_Fan(data_set,clusters_file=k)
				etime = time()
				print(f"Loading initial data: time: {(etime-itime):.2f} seconds\n")
				fan.elbow_method()
				#fan.run()

	except IndexError:
			print("Mising arguments!")

	
