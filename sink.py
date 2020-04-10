import sys
import zmq
from tabulate import tabulate

class K_means_Sink:
	def __init__(self,port_pull,fan_address):
		self.port_pull = port_pull
		self.fan_address = fan_address

		self.context = zmq.Context()

		self.fan_pull = self.context.socket(zmq.PULL)
		self.fan_push = self.context.socket(zmq.PUSH)

		self.connect()
	
	def connect(self):
		self.fan_pull.bind(f"tcp://*:{self.port_pull}")
		self.fan_push.connect(f"tcp://{self.fan_address}")

	def run(self):
		while True:
			print("Sink waiting")
			from_fan = self.fan_pull.recv_json()
			
			print(f"Number of tasks: {from_fan['n_tasks']}")
			if from_fan["finished"]:
				clasify_matrix = self.init_matrix_zeros_finished(from_fan['n_clusters'])
				for i in range(from_fan['n_tasks']):
					clasification = self.fan_pull.recv_json()["clasification"]
					clasify_matrix = self.update_clasify_matrix_finished(clasify_matrix,clasification)
				#self.print(clasify_matrix)
				print("New clusters: ")
				print(clasify_matrix)
				self.fan_push.send_json({
					"clasification": clasify_matrix
				})
				print("Finished")
				#break
			else:
				clasify_matrix = self.init_matrix_zeros(from_fan['n_clusters'],from_fan['dim'])
				for i in range(from_fan['n_tasks']):
					clasification = self.fan_pull.recv_json()["clasification"]
					clasify_matrix = self.update_clasify_matrix(clasify_matrix,clasification)
				self.print(clasify_matrix)
				new_clusters = self.get_mean(clasify_matrix)	
				self.fan_push.send_json({
					"new_clusters": new_clusters
				})

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

	def update_clasify_matrix(self,clasify_matrix,clasification):
		for i,c in enumerate(clasification):
			for j,d in enumerate(c[0]):
				clasify_matrix[i][0][j] += d
			clasify_matrix[i][1] += c[1]
		return clasify_matrix

	def update_clasify_matrix_finished(self,clasify_matrix,clasification):
		for i,c in enumerate(clasification):
			clasify_matrix[i] += c
		return clasify_matrix

	def get_mean(self,clasify_matrix):
		clusters = list()
		for c in clasify_matrix:
			cluster = list()
			for d in c[0]:
				cluster.append(d/c[1])
			clusters.append(cluster)
		return clusters

	def print(self,matrix,headers=[]):
		print(tabulate(matrix,headers=headers,showindex="always",tablefmt="github"))
	
if __name__ == "__main__":
	try:
		port_pull = sys.argv[1]
		fan_address = sys.argv[2]
	except IndexError:
		print("Arguments missing!")
	
	sink = K_means_Sink(port_pull,fan_address)
	sink.run()
		