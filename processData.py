from os import path
from os import remove
from os import rename

def get_seq_user_ids(files):
  user_ids = {}
  seq_id = 0
  print("Get user ids: Reading files...")
  for namefile in files:
    print("File: "+namefile)
    with open(namefile) as f:
      for row in f:
        row = row.strip()
        if row[-1] is not ':':
          user_id, _, _ = row.split(",")
          if not user_id in user_ids:
            user_ids[user_id] = seq_id
            seq_id += 1  
  return user_ids

def save_dict(namefile, headers, dict):
  if path.exists(namefile):
    print(f"The file {namefile} already exist! it was remove")
    remove(namefile)
  print(f"Saving file {namefile} wait...")
  with open(namefile, "a") as f:
    f.write(','.join(headers) + '\n')
    for item in dict.items():
      f.write(','.join(map(str,item)) + '\n')
  print("Finished succefully!")

def save_matrix(namefile,matrix):
  if path.exists(namefile):
    print(f"The file {namefile} already exist! it was remove")
    remove(namefile)
  print(f"Saving file {namefile} wait...")
  with open(namefile, "a") as f:
    for row in matrix:
      data = ""
      
      for tup in row:
        data += f'({tup[0]} {tup[1]}),'
      data = data[:-1]+"\n"
      f.write(data)
  print("Finished succefully!")

def create_data_set(files,user_ids):
  print("Create data set: Reading files...")
  current_movie = 0
  user_data = [None]*len(user_ids)
  for namefile in files:
    print("File: "+namefile)
    with open(namefile) as f:
      for row in f:
        row = row.strip()
        if row[-1] is ':':
          current_movie = int(row[0:-1])     
        else:
          user_id, rate, _ = row.split(",")
          index = user_ids[user_id]
          if user_data[index] == None:
            user_data[index] = list()
          user_data[index].append((current_movie,rate))
  return user_data

if __name__ == "__main__":
  
  files = [
    './Netflix/combined_data_1.txt',
    #'./Netflix/combined_data_2.txt',
    #'./Netflix/combined_data_3.txt',
    #'./Netflix/combined_data_4.txt'
  ]
  '''
  files = [
    './pruebasDataSets/pruebas2.txt',
    './pruebasDataSets/pruebas2_1.txt'
  ]
  '''
  user_ids = get_seq_user_ids(files)
  save_dict('./pruebasDataSets/seq_user_ids.csv',['id','id_seq'],user_ids)
  user_data = create_data_set(files,user_ids)
  #print(user_data)
  save_matrix('./pruebasDataSets/data_set_complete.csv',user_data)

    
        
      

        