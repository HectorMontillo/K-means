import sys
import zmq
from tabulate import tabulate
from os import path
from os import remove
from time import time

class K_means_Sink:
	def __init__(self,port_pull="5565",fan_address="localhost:5556"):
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
				print("Initialazing work: Finish flag ")
				itime = time()

				clasify_dict =  self.fan_pull.recv_json()
				
				for i in range(from_fan['n_tasks']-1):
					clasify_dict_worker =  self.fan_pull.recv_json()
					clasify_dict = self.update_clasify_dict_finish(clasify_dict,clasify_dict_worker)
				
				self.save_clasify(clasify_dict, f'clasification/{from_fan["k"]}_clasification.txt')
				inertia = self.getInertia(clasify_dict)
				self.fan_push.send_json({
					"file": f'clasification/{from_fan["k"]}_clasification.txt',
					"inertia": inertia
				})

				etime = time()
				print(f"Finishin work: Finish flag time: {(etime - itime):.2f} seconds")	

				print("Finished")
				#break
			else:
				print("Initialazing work: ")
				itime = time()
				clasify_dict =  self.fan_pull.recv_json()
				#print(clasify_dict.keys())
				for i in range(from_fan['n_tasks']-1):
					clasify_dict_worker =  self.fan_pull.recv_json()
					clasify_dict = self.update_clasify_dict(clasify_dict,clasify_dict_worker)
				new_clusters = self.get_mean(clasify_dict)
	
				self.fan_push.send_json({
					"new_clusters": new_clusters
				})
				etime = time()
				print(f"Finishin work: time: {(etime - itime):.2f} seconds")


	def getInertia(self, clasify_dict):
		inertia = 0
		for k in clasify_dict.keys():
			inertia += clasify_dict[k]['distance-sum']
		return inertia

	def save_clasify(self,clasify_dict,namefile):
		print(".....saving clasify: ", end="")
		itime = time()

		if path.exists(namefile):
			print(f"The file {namefile} already exist! it was remove")
			remove(namefile)
		print(f"Saving file {namefile} wait...")
		with open("./seq_user_ids.csv", "r") as f:
			f.readline()
			id_users = []
			for row in f:
				row = row.strip()
				id_users.append(row.split(',')[0])
				
		with open(namefile, "a") as f:
			
			inertia = 0
			for k in clasify_dict.keys():
				inertia += clasify_dict[k]['distance-sum']
			f.write(f"inertia: {inertia}\n")

			f.write('cluster,count,distance-sum\n')
			for k in clasify_dict.keys():
				f.write(f'{k},{clasify_dict[k]["count"]},{clasify_dict[k]["distance-sum"]}\n')

			f.write('\nseq_id_user,id_user,cluster\n')
			for k in clasify_dict.keys():
				for s in clasify_dict[k]['samples']:
					f.write(f'{s},{id_users[s]},{k}\n')
		
		etime = time()
		print(f"time: {(etime - itime):.2f} seconds")	
		print("Finished succefully!")


	def sum_points(self,x1,x2):
		sum_p = {**x1,**x2}
		for k in sum_p.keys():
			sum_p[k] = x1.get(k,0) + x2.get(k,0)
		return sum_p

	def update_clasify_dict_finish(self,clasify_dict, clasify_dict_worker):
		print(".....update clasify: ", end="")
		itime = time()
		for k in clasify_dict.keys():
			clasify_dict[k]['distance-sum'] += clasify_dict_worker[k]['distance-sum']
			clasify_dict[k]['samples'] += clasify_dict_worker[k]['samples'] 
			clasify_dict[k]['count'] += clasify_dict_worker[k]['count']

		etime = time()
		print(f"time: {(etime - itime):.2f} seconds")	
		return clasify_dict

	def update_clasify_dict(self,clasify_dict, clasify_dict_worker):
		print(".....update clasify: ", end="")
		itime = time()
		for k in clasify_dict.keys():
			clasify_dict[k]['sum'] = self.sum_points(clasify_dict[k]['sum'],clasify_dict_worker[k]['sum']) 
			clasify_dict[k]['count'] += clasify_dict_worker[k]['count']
		
		etime = time()
		print(f"time: {(etime - itime):.2f} seconds")	
		return clasify_dict

	def get_mean(self, clasify_dict):
		print(".....Get mean: ", end="")
		itime = time()
		new_clusters = []
		for ck in clasify_dict.keys():
			for k in clasify_dict[ck]['sum'].keys():
				clasify_dict[ck]['sum'][k] = clasify_dict[ck]['sum'][k]/clasify_dict[ck]['count']
				
			new_clusters.append(clasify_dict[ck]['sum'])
		
		etime = time()
		print(f"time: {(etime - itime):.2f} seconds")	
		return new_clusters

	def print(self,matrix,headers=[]):
		print(tabulate(matrix,headers=headers,showindex="always",tablefmt="github"))
	
if __name__ == "__main__":
	sink = K_means_Sink()
	sink.run()
		