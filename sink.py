import sys
import time
import zmq

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
