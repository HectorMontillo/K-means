from os import path
def save_dict(namefile, headers, dict):
  if path.exists(namefile):
    print("The file already exist!")
  else:
    with open(namefile, "a") as f:
      f.write(','.join(headers) + '\n')
      for item in dict.items():
        f.write(','.join(map(str,item)) + '\n')

if __name__ == "__main__":
  d = {
    '8558': 0,
    '8559': 1,
    '8560': 2,
    '8561': 3,
    '8562': 4,
    '8563': 5,
  }
  save_dict('./DataSets/prueba.csv',['id','id_seq'],d)
