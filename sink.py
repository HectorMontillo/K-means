import sys
import zmq
from tabulate import tabulate
from os import path
from os import remove


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
				clasify_dict =  self.fan_pull.recv_json()
				
				for i in range(from_fan['n_tasks']-1):
					clasify_dict_worker =  self.fan_pull.recv_json()
					clasify_dict = self.update_clasify_dict_finish(clasify_dict,clasify_dict_worker)
				
				self.save_clasify(clasify_dict, 'clasification.txt')
				self.fan_push.send_json({
					"file": 'clasification.txt'
				})
				print("Finished")
				#break
			else:
				clasify_dict =  self.fan_pull.recv_json()
				#print(clasify_dict.keys())
				for i in range(from_fan['n_tasks']-1):
					clasify_dict_worker =  self.fan_pull.recv_json()
					clasify_dict = self.update_clasify_dict(clasify_dict,clasify_dict_worker)
				new_clusters = self.get_mean(clasify_dict)
	
				self.fan_push.send_json({
					"new_clusters": new_clusters
				})
	def save_clasify(self,clasify_dict,namefile):
		if path.exists(namefile):
			print(f"The file {namefile} already exist! it was remove")
			remove(namefile)
		print(f"Saving file {namefile} wait...")
		with open("./Netflix/proccessed/seq_user_ids.csv", "r") as f:
			f.readline()
			id_users = []
			for row in f:
				row = row.strip()
				id_users.append(row.split(',')[0])
				
		with open(namefile, "a") as f:
			f.write('cluster,count\n')
			for k in clasify_dict.keys():
				f.write(f'{k},{clasify_dict[k]["count"]}\n')

			f.write('\nseq_id_user,id_user,cluster\n')
			for k in clasify_dict.keys():
				for s in clasify_dict[k]['samples']:
					f.write(f'{s},{id_users[s]},{k}\n')
		print("Finished succefully!")


	def sum_points(self,x1,x2):
		sum_p = {**x1,**x2}
		for k in sum_p.keys():
			sum_p[k] = x1.get(k,0) + x2.get(k,0)
		return sum_p

	def update_clasify_dict_finish(self,clasify_dict, clasify_dict_worker):

		for k in clasify_dict.keys():
			clasify_dict[k]['samples'] += clasify_dict_worker[k]['samples'] 
			clasify_dict[k]['count'] += clasify_dict_worker[k]['count']
		return clasify_dict

	def update_clasify_dict(self,clasify_dict, clasify_dict_worker):
		for k in clasify_dict.keys():
			clasify_dict[k]['sum'] = self.sum_points(clasify_dict[k]['sum'],clasify_dict_worker[k]['sum']) 
			clasify_dict[k]['count'] += clasify_dict_worker[k]['count']
		return clasify_dict

	def get_mean(self, clasify_dict):
		new_clusters = []
		for ck in clasify_dict.keys():
			for k in clasify_dict[ck]['sum'].keys():
				clasify_dict[ck]['sum'][k] = clasify_dict[ck]['sum'][k]/clasify_dict[ck]['count']
				
			new_clusters.append(clasify_dict[ck]['sum'])
		return new_clusters

	def print(self,matrix,headers=[]):
		print(tabulate(matrix,headers=headers,showindex="always",tablefmt="github"))
	
if __name__ == "__main__":
	sink = K_means_Sink()
	sink.run()
		