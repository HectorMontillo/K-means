import zmq
import sys
import pandas as pd
import numpy as np
class K_means_Fan:
	def __init__(self,data_set,k,sink_address,port_worker='5555',port_sink='5556',max_iterations=1000, n=10,tolerance=0.001, init_samples = None):
		#Initial data
		self.data_set = self.read_data(data_set)
		self.k = k #number of clusters
		self.n = n #number of samples to send to workers
		self.max_iterations = max_iterations
		self.tolerance = tolerance
		self.init_samples = init_samples # number of samples to be read to calculate initial centroids

		#Clusters
		self.clusters = list()


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
		self.sink_push.bind(f"tcp://*:{self.port_sink}")
		self.sink_pull.connect(f"tcp://{self.sink_address}")

	def read_data(self,data_set):
		return pd.read_csv(data_set)


	def run(self):
		print(type(self.data_set))
		print(self.data_set.shape)
		for i in range(self.max_iterations):
			break
		
	def initialize_clusters(self):
		pass


if __name__ == "__main__":
		try:
				data_set = sys.argv[1]
				k = sys.argv[2]
				sink_address = sys.argv[3]
		except IndexError:
				print("'data_set','k' or 'port' are mising!")

		fan = K_means_Fan(data_set,int(k),sink_address)
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