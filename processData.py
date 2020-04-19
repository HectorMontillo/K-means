from os import path
from os import remove

def get_seq_user_ids(files):
  user_ids = {}
  #seq_id = 0
  print("Get user ids: Reading files...")
  for namefile in files:
    print("File: "+namefile)
    with open(namefile) as f:
      for row in f:
        row = row.strip()
        if row[-1] is not ':':
          user_id, _, _ = row.split(",")
          if not user_id in user_ids:
            user_ids[user_id] = 0  
  return user_ids

def save_dict(namefile, headers, dict):
  if path.exists(namefile):
    print("The file already exist! it was remove")
    remove(namefile)
  print("Saving file wait...")
  with open(namefile, "a") as f:
    f.write(','.join(headers) + '\n')
    for item in dict.items():
      f.write(','.join(map(str,item)) + '\n')
  print("Finished succefully!")

def append_column():
  pass

def create_data_set(files, user_ids):
  print("Create data set: Reading files...")
  current_movie = 0
  for namefile in files:
    print("File: "+namefile)
    with open(namefile) as f:
      for row in f:
        row = row.strip()
        if row[-1] is ':':
          if not current_movie is '1':
            append_column()
            user_ids = dict.fromkeys( user_ids.iterkeys(), 0 )

          current_movie = row[0:-1]
        else:
          user_id, rate, _ = row.split(",")
          user_ids[user_id] = rate
  

if __name__ == "__main__":
  '''
  files = [
    './Netflix/combined_data_1.txt',
    './Netflix/combined_data_2.txt',
    './Netflix/combined_data_3.txt',
    './Netflix/combined_data_4.txt'
  ]
  '''
  files = [
    './pruebasDataSets/pruebas2.txt'
  ]
  user_ids = get_seq_user_ids(files)
  save_dict('./pruebasDataSets/seq_user_ids.csv',['id','id_seq'],user_ids)

    
        
      

        