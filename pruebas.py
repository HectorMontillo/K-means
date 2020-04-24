import math
def cosine_similarity(x1,x2):
  sum_AB = 0
  sum_powA = 0 
  sum_powB = 0 

  for k in x1.keys():
    sum_AB += x1[k]*x2.get(k,0)
    sum_powA += x1[k]**2

  for k in x2.keys():
    sum_powB += x2[k]**2

  return sum_AB / (math.sqrt(sum_powA)*math.sqrt(sum_powB))

def sum_points(x1,x2):
  keys = {**x1,**x2}.keys()
  sum_p = {}
  for k in keys:
    sum_p[k] = x1.get(k,0) + x2.get(k,0)
  return sum_p

def get_clasify_dict(clusters,points,func):
  clasify_dict = {}
  for p in points:
    maximun = 0
    for i,c in enumerate(clusters):
      #distance = math.sqrt(sum([(a - b) ** 2 for a, b in zip(p,c)]))
      distance = func(p,c)
      if distance >= maximun:
        maximun = distance
        cluster = i
    if cluster in clasify_dict:
      clasify_dict[cluster]['sum'] = sum_points(clasify_dict[cluster]['sum'],p)
      clasify_dict[cluster]['count'] += 1
    else:
      clasify_dict[cluster] = {
        'sum': p,
        'count': 1
      }
  return clasify_dict

def get_mean(clasify_dict):
  new_clusters = []
  for ck in clasify_dict.keys():
    for k in clasify_dict[ck]['sum'].keys():
      clasify_dict[ck]['sum'][k] = clasify_dict[ck]['sum'][k]/clasify_dict[ck]['count']
      
    new_clusters.append(clasify_dict[ck]['sum'])
  return new_clusters


def read_data(range_rows):
  samples = list()
  for i,f in enumerate(open('test.txt')):
    f = f.strip()
    if i >= range_rows[0] and i <= range_rows[1]:
      data = dict(map(lambda t: (t.split(' ')[0],int(t.split(' ')[1])),map(lambda t: t[1:-1], f.split(','))))
      samples.append(data)
    elif i > range_rows[1]:
      break
  return samples

if __name__ == "__main__":
  '''
  clusters = [{1:2,2:1},{2:2,1:1}]
  points = [{1:2,2:1},{1:2,2:1},{3:2,2:2},{4:2,2:1}]
  clasify = get_clasify_dict(clusters,points,cosine_similarity)
  print(clasify)
  new_clusters = get_mean(clasify)
  print(new_clusters)
  '''
  print(read_data([0,1]))
