import sys
import zmq
import pandas as pd
import math
from operator import itemgetter
from tabulate import tabulate


class K_means_Worker:
	def __init__(self,data_set,fan_address='localhost:5555' ,sink_address='localhost:5565'):
		self.data_set = data_set
		self.fan_address = fan_address
		self.sink_address = sink_address

		self.context = zmq.Context()
		self.fan = self.context.socket(zmq.PULL)
		self.sink = self.context.socket(zmq.PUSH)

		self.connect()

	def connect(self):
		self.fan.connect(f"tcp://{self.fan_address}")
		self.sink.connect(f"tcp://{self.sink_address}")

	def read_data(self,range_rows):
		samples = list()
		for i,f in enumerate(open(self.data_set)):
			f = f.strip()
			if i >= range_rows[0] and i <= range_rows[1]:
				data = dict(map(lambda t: (t.split(' ')[0],int(t.split(' ')[1])),map(lambda t: t[1:-1], f.split(','))))
				samples.append(data)
			elif i > range_rows[1]:
				break
		return samples
		

	def run(self):
		print("Worker ready and waiting for task...")
		n_tasks = 0
		while True:
			
			task = self.fan.recv_json()
			samples = self.read_data(task["rows"])
			print(f"{n_tasks}:---------------->")
			#print(task)
			#print(samples)
			#clasify the samples and sum
			if task["finished"]:
				clasify_dict = self.get_clasify_dict_finish(task["clusters"],samples,self.cosine_similarity,task["rows"][0])
			else:
				clasify_dict = self.get_clasify_dict(task["clusters"],samples,self.cosine_similarity)
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

	def cosine_similarity(self,x1,x2):
		sum_AB = 0
		sum_powA = 0 
		sum_powB = 0 

		for k in x1.keys():
			sum_AB += x1[k]*x2.get(k,0)
			sum_powA += x1[k]**2

		for k in x2.keys():
			sum_powB += x2[k]**2

		return sum_AB / (math.sqrt(sum_powA)*math.sqrt(sum_powB))

	def sum_points(self,x1,x2):
		sum_p = {**x1,**x2}
		for k in sum_p.keys():
			sum_p[k] = x1.get(k,0) + x2.get(k,0)
		return sum_p

	def get_clasify_dict(self,clusters,points,distance_func):
		#print('--calc clasify--')
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
				distance = distance_func(p,c)
				#print(f"{p},{c},{distance}")
				if distance >= maximun:
					maximun = distance
					cluster = i
			
			
			clasify_dict[cluster]['sum'] = self.sum_points(clasify_dict[cluster]['sum'],p)
			clasify_dict[cluster]['count'] += 1
			
		return clasify_dict
	
	def get_clasify_dict_finish(self,clusters,points,distance_func,ci):
		clasify_dict = {}
		for i,c in enumerate(clusters):
			clasify_dict[i] = {
						'samples': [],
						'count': 0
					}
		for i,p in enumerate(points):
			maximun = 0
			for j,c in enumerate(clusters):
				#distance = math.sqrt(sum([(a - b) ** 2 for a, b in zip(p,c)]))
				distance = distance_func(p,c)
				if distance >= maximun:
					maximun = distance
					cluster = j
			
			clasify_dict[cluster]['samples'].append(ci+i)
			clasify_dict[cluster]['count'] += 1

		return clasify_dict

	def print_clasify(self,clasify_matrix,headers=[]):
		print(tabulate(clasify_matrix,headers=headers,showindex="always",tablefmt="github"))
		



if __name__ == "__main__":
	try:
		data_set = sys.argv[1]
	except IndexError:
		print("Aguments mising!")

	worker = K_means_Worker(data_set)
	worker.run()
