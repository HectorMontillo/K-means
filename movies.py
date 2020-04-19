with open("Desktop/movies.txt", "r") as file:
  seqUsers = {}
  currentMovie = 0
  for line in file:
    line = line.strip()
    if line[-1] is ':':
      currentMovie = int(line[0:-1])
    else:
      print("This is a user")
      user, rate, _ = line.split(",")
      seqId = 0
      if user in seqUsers:
        print("Id for {} is {}",format(user,seqUsers[user]))
      else:
        seqUsers[user] = len(seqUsers)
        print("added new user {} {}".format(user,seqUsers[user]))


