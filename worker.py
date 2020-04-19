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
			if i >= range_rows[0] and i <= range_rows[1]:
				data = list(map(lambda x: float(x),f.strip('\n').split(',')))
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

			#calc the distance betwn the samples set and all clusters
			distances_matrix = self.euclidean_distance(task["clusters"],samples)

			#clasify the samples and sum
			if task["finished"]:
				pass
				clasify_matrix = self.clasify_finished(distances_matrix, task["rows"][0])
				#self.print_clasify(clasify_matrix)
			else:
				clasify_matrix = self.clasify(distances_matrix,samples)
				#print("Clasification: ")
				#self.print_clasify(clasify_matrix,["cluster","sum","count"])

			#send result to sink
			self.sink.send_json({
				"clasification": clasify_matrix
			})

			n_tasks += 1



	def euclidean_distance(self,clusters,points):
		distances_matrix = []
		for p in points:
			distances_list = []
			for c in clusters:
				
				distance = math.sqrt(sum([(a - b) ** 2 for a, b in zip(p,c)]))
				distances_list.append(distance)
			distances_matrix.append(distances_list)
		return distances_matrix

	def clasify(self,distances_matrix,samples):
		clasify_matrix = self.init_matrix_zeros(len(distances_matrix[0]),len(samples[0]))
		for i,p in enumerate(distances_matrix):
			min_index = min(enumerate(p), key=itemgetter(1))[0]
			for j in range(len(samples[0])):
				clasify_matrix[min_index][0][j] += samples[i][j]
			clasify_matrix[min_index][1] += 1
		return clasify_matrix

	def clasify_finished(self,distances_matrix, ci):
		clasify_matrix = self.init_matrix_zeros_finished(len(distances_matrix[0]))
		for i,p in enumerate(distances_matrix):
			min_index = min(enumerate(p), key=itemgetter(1))[0]
			clasify_matrix[min_index].append(i+ci)
		return clasify_matrix

	def init_matrix_zeros(self,n_clusters, dim):
		clasify_matrix = list()
		for _ in range(n_clusters):
			point = list()
			for _ in range(dim):
				point.append(0)
			data = [point,0]
			clasify_matrix.append(data)
		return clasify_matrix

	def init_matrix_zeros_finished(self,n_clusters):
		clasify_matrix = list()
		for _ in range(n_clusters):
			point = list()
			clasify_matrix.append(point)
		return clasify_matrix

	def print_clasify(self,clasify_matrix,headers=[]):
		print(tabulate(clasify_matrix,headers=headers,showindex="always",tablefmt="github"))
		



if __name__ == "__main__":
	try:
		data_set = sys.argv[1]
	except IndexError:
		print("Aguments mising!")

	worker = K_means_Worker(data_set)
	worker.run()
