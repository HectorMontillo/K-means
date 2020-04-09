import zmq
import sys
import pandas as pd
import numpy as np
import math

class K_means_Fan:
	def __init__(self,data_set,k,target_columns,sink_address,port_worker='5555',port_sink='5556',max_iterations=1000, n=10,tolerance=0.001):
		#Initial data
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
		return pd.read_csv(data_set)


	def run(self):
		print("Press enter when workers and sink are ready...")
		_ = input()
		print("sending tasks to workers")


		print(f"Number of samples: {self.number_samples}, Number of task per iteration: {self.number_tasks}")
		
		self.initialize_clusters()
		iterations = 0
		while True:
			
			# send sink the number of results
			self.sink_push.send(str(self.number_tasks).encode('ascii'))

			# send workers the taks

			# recive the new clusters from sink
			message = self.sink_pull.recv()
			print(message)

			# compare clusters to stop
			if(iterations >= self.max_iterations):
				break
			iterations+=1
			break

		
	def initialize_clusters(self):
		self.clusters = self.data_set.sample(n=self.k).to_dict(orient='split')['data']
		print(self.clusters)
		#print(self.clusters.to_dict(orient='split'))


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
		


'''

context = zmq.Context()

# socket with workers
workers = context.socket(zmq.PUSH)
workers.bind("tcp://*:5557")

# socket with sink
sink = context.socket(zmq.PUSH)
sink.connect("tcp://localhost:5558")


print("Press enter when workers are ready...")
_ = input()
print("sending tasks to workers")

sink.send(b'0')


totalTime = 0
for task in range(100):
		workload = random.randint(1,100)
		totalTime += workload
		workers.send_string(u'%i' % workload)

print("Total expected cost: %s msec" % totalTime)
while True:
		pass
'''