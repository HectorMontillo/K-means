import sys
import zmq
import pandas as pd
import math
from operator import itemgetter
from tabulate import tabulate
from time import time
import os
import json


class K_means_Worker:
	def __init__(self,data_set="",fan_address='localhost:5555' ,sink_address='localhost:5565'):
		self.data_set = data_set
		self.fan_address = fan_address
		self.sink_address = sink_address

		self.clusters = None
		self.sqrt_sum_powB = None
		self.iteration = None

		self.context = zmq.Context()
		self.fan = self.context.socket(zmq.PULL)
		self.sink = self.context.socket(zmq.PUSH)

		self.connect()

	def connect(self):
		self.fan.connect(f"tcp://{self.fan_address}")
		self.sink.connect(f"tcp://{self.sink_address}")

	def read_data(self,range_rows,batch_size):
		print(".....Reading samples: ", end="")
		itime = time()

		samples = list()
		fileIndex = math.floor(range_rows[0]/batch_size)*batch_size
		fileName = f"./batch/{fileIndex}.txt"

		for i,f in enumerate(open(fileName),start=fileIndex):

			if i >= range_rows[0] and i <= range_rows[1]:
				f = f.strip()
				data = dict(map(lambda t: (t.split(' ')[0],float(t.split(' ')[1])),map(lambda t: t[1:-1], f.split(','))))
				samples.append(data)
			elif i > range_rows[1]:
				break

		etime = time()
		print(f"time: {(etime - itime):.5f} seconds")
		return samples
	
	def update_norm_clusters(self):
		print(".....Updating norm clusters: ", end="")
		itime = time()
		self.sqrt_sum_powB = list()
		for i,c in enumerate(self.clusters):
			self.sqrt_sum_powB.append(0)
			for k in c.keys():
				self.sqrt_sum_powB[i] += c[k]**2
			self.sqrt_sum_powB[i] = math.sqrt(self.sqrt_sum_powB[i])

		etime = time()
		print(f"time: {(etime - itime):.5f}")

	def update_clusters(self):
		print(".....Updating clusters: ", end="")
		itime = time()

		clusters_file = "clusters.txt"
		new_clusters = []
		for f in open(clusters_file):
			f = f.strip()
			cluster = json.loads(f)
			new_clusters.append(cluster)
		self.clusters = new_clusters
		etime = time()
		print(f"time: {(etime - itime):.5f}")

	def run(self):
		n_tasks = 0
		while True:	
			print(f"Task: {n_tasks} ---------------------->")
			print("Worker ready and waiting for task...")
			task = self.fan.recv_json()
			if task["iteration"] != self.iteration:
				self.update_clusters()
				self.update_norm_clusters()
				self.iteration = task["iteration"]

			samples = self.read_data(task["rows"],task["batch"])
			#print(task)
			#print(samples)
			#clasify the samples and sum
			
			if task["finished"]:
				clasify_dict = self.get_clasify_dict_finish(self.clusters,samples,self.cosine_similarity,task["rows"][0])
			else:
				clasify_dict = self.get_clasify_dict(self.clusters,samples,self.cosine_similarity)
			#print(clasify_dict)
			#send result to sink
			self.sink.send_json(clasify_dict)

			n_tasks += 1

	def euclidean_distance(self,x1,x2):
		keys = {**x1,**x2}
		#print(keys)
		distance = 0
		for k in keys.keys():
			a = x1.get(k,0)
			b = x2.get(k,0)
			distance += (a-b)**2
		return math.sqrt(distance)

	def cosine_similarity(self,x1,x2,i):
		sum_AB = 0
		sum_powA = 0 
		#sum_powB = 0 

		for k in x1.keys():
			sum_AB += x1[k]*x2.get(k,0)
			sum_powA += x1[k]**2
		'''
		for k in x2.keys():
			sum_powB += x2[k]**2
		'''
		return sum_AB / (math.sqrt(sum_powA)*self.sqrt_sum_powB[i])

	def sum_points(self,x1,x2):
		sum_p = {**x1,**x2}
		for k in sum_p.keys():
			sum_p[k] = x1.get(k,0) + x2.get(k,0)
		return sum_p

	def get_clasify_dict(self,clusters,points,distance_func):
		#print(clusters)
		#print('--calc clasify--')
		print(".....Getting clasify: ", end="")
		itime = time()
		clasify_dict = {}
		for i,c in enumerate(clusters):
			clasify_dict[i] = {
						'sum': {},
						'count': 0
					}
		for p in points:
			maximun = 0
			for i,c in enumerate(clusters):
				#distance = math.sqrt(sum([(a - b) ** 2 for a, b in zip(p,c)]))
				distance = distance_func(p,c,i)
				#print(f"{p},{c},{distance}")
				if distance >= maximun:
					maximun = distance
					cluster = i
			
			
			clasify_dict[cluster]['sum'] = self.sum_points(clasify_dict[cluster]['sum'],p)
			clasify_dict[cluster]['count'] += 1
		etime = time()
		print(f"time: {(etime - itime):.5f} seconds")	
		return clasify_dict
	
	def get_clasify_dict_finish(self,clusters,points,distance_func,ci):
		print(".....Getting clasify finish: ", end="")
		itime = time()
		clasify_dict = {}
		for i,c in enumerate(clusters):
			clasify_dict[i] = {
						'distance-sum' : 0,
						'samples': [],
						'count': 0
					}
		for i,p in enumerate(points):
			maximun = 0
			for j,c in enumerate(clusters):
				#distance = math.sqrt(sum([(a - b) ** 2 for a, b in zip(p,c)]))
				distance = distance_func(p,c,j)
				if distance >= maximun:
					maximun = distance
					cluster = j
			
			clasify_dict[cluster]['distance-sum'] += maximun
			clasify_dict[cluster]['samples'].append(ci+i)
			clasify_dict[cluster]['count'] += 1
		etime = time()
		print(f"time: {(etime - itime):.5f} seconds")
		return clasify_dict

	def print_clasify(self,clasify_matrix,headers=[]):
		print(tabulate(clasify_matrix,headers=headers,showindex="always",tablefmt="github"))
		



if __name__ == "__main__":
	'''
	try:
		data_set = sys.argv[1]
	except IndexError:
		print("Aguments mising!")
	'''
	worker = K_means_Worker()
	worker.run()
