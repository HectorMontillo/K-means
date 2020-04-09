import sys
import zmq


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
		print("Sink waiting")
		from_fan = self.fan_pull.recv_json()
		print(f"Number of tasks: {from_fan['n_tasks']}")
		clasify_matrix = self.init_matrix_zeros(from_fan['n_clusters'])
		for i in range(from_fan['n_tasks']):
			clasification = self.fan_pull.recv_json()["clasification"]
			self.update_clasify_matrix(clasify_matrix,clasification)

		self.get_mean(clasify_matrix)	

		self.fan_push.send(b"Llego")

	def init_matrix_zeros(self,n):
		clasify_matrix = list()
		for i in range(n):
			data = [0,0]
			clasify_matrix.append(data)
		return clasify_matrix

	def update_clasify_matrix(self,clasify_matrix,clasification):
		for i,c in enumerate(clasification):
			clasify_matrix[i][0] += c[0]
			clasify_matrix[i][1] += c[1]



if __name__ == "__main__":
	try:
		port_pull = sys.argv[1]
		fan_address = sys.argv[2]
	except IndexError:
		print("Arguments missing!")
	
	sink = K_means_Sink(port_pull,fan_address)
	sink.run()
		


'''
context = zmq.Context()

fan = context.socket(zmq.PULL)
fan.bind("tcp://*:5558")

# Wait for start of batch
s = fan.recv()

# Start our clock now
tstart = time.time()

# Process 100 confirmations
for task in range(100):
		print(task)
		s = fan.recv()
		if task % 10 == 0:
				sys.stdout.write(':')
		else:
				sys.stdout.write('.')
		sys.stdout.flush()
		print("-")

# Calculate and report duration of batch
tend = time.time()
print("Total elapsed time: %d msec" % ((tend-tstart)*1000))
'''