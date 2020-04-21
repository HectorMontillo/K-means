l = [(15,2),(16,7)]
data = ""
for t in l:
  data += f'({t[0]} {t[1]}),'
data = data[:-1]
print(data)
