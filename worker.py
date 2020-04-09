import sys
import zmq
import pandas as pd
import math
from operator import itemgetter
from tabulate import tabulate


class K_means_Worker:
	def __init__(self,data_set,target_columns,fan_address,sink_address):
		self.data_set = self.read_data(data_set)[target_columns]
		self.fan_address = fan_address
		self.sink_address = sink_address

		self.context = zmq.Context()
		self.fan = self.context.socket(zmq.PULL)
		self.sink = self.context.socket(zmq.PUSH)

		self.connect()

	def connect(self):
		self.fan.connect(f"tcp://{self.fan_address}")
		self.sink.connect(f"tcp://{self.sink_address}")

	def run(self):
		print("Worker ready and waiting for task...")
		n_tasks = 0
		while True:
			
			task = self.fan.recv_json()
			print(f"Task {n_tasks}")
			ci = task["rows"][0]
			cj = task["rows"][1]
			samples = self.data_set.iloc[ci:cj, :].to_dict(orient='split')['data']
			#calc the distance betwn the samples set and all clusters
			distances_matrix = self.euclidean_distance(task["clusters"],samples)
			#clasify the samples and sum
			clasify_matrix = self.clasify(distances_matrix)
			print("Clasification: ")
			self.print_clasify(clasify_matrix)
			#send result to sink
			self.sink.send_json({
				"clasification": clasify_matrix
			})

			n_tasks += 1

	def read_data(self,data_set):
		return pd.read_csv(data_set)

	def euclidean_distance(self,clusters,points):
		distances_matrix = []
		for p in points:
			distances_list = []
			for c in clusters:
				distance = math.sqrt(sum([(a - b) ** 2 for a, b in zip(p,c)]))
				distances_list.append(distance)
			distances_matrix.append(distances_list)
		return distances_matrix

	def clasify(self,distances_matrix):
		clasify_matrix = self.init_matrix_zeros(len(distances_matrix[0]))
		for p in distances_matrix:
			res = min(enumerate(p), key=itemgetter(1))
			clasify_matrix[res[0]][0] += res[1]
			clasify_matrix[res[0]][1] += 1
		return clasify_matrix

	def init_matrix_zeros(self,n):
		clasify_matrix = list()
		for i in range(n):
			data = [0,0]
			clasify_matrix.append(data)
		return clasify_matrix

	def print_clasify(self,clasify_matrix):
		print(tabulate(clasify_matrix,headers=["cluster","sum","count"],showindex="always",tablefmt="github"))
		



if __name__ == "__main__":
	try:
		data_set = sys.argv[1]
		target_columns = sys.argv[2].strip('[]').split(',')
		fan_address = sys.argv[3]
		sink_address = sys.argv[4]
	except IndexError:
		print("Aguments mising!")

	worker = K_means_Worker(data_set,target_columns,fan_address,sink_address)
	worker.run()



'''
context = zmq.Context()

work = context.socket(zmq.PULL)
work.connect("tcp://localhost:5557")

# Socket to send messages to
sink = context.socket(zmq.PUSH)
sink.connect("tcp://localhost:5558")

# Process tasks forever
while True:
		s = work.recv()

		# Simple progress indicator for the viewer
		sys.stdout.write('.')
		sys.stdout.flush()

		# Do the work
		time.sleep(int(s)*0.001)

		# Send results to sink
		sink.send(b'')
'''
