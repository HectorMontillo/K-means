import sys
import time
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
		message = self.fan_pull.recv()
		print(message)
		self.fan_push.send(b"Llego")


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