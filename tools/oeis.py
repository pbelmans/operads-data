import httplib

def getData(sequence):
  connection = httplib.HTTPConnection("oeis.org")
  connection.request("GET", "/search?fmt=text&q=" + sequence)
  response = connection.getresponse()
  
  if response.status == 200:
    return response.read()
  else:
    return ""

def getSequence(data, key):
  inSequence = False
  sequence = dict([])

  for line in data.split("\n"):
    if line[0:3] == "%I ":
      if line[3:10] == key: inSequence = True
      else: inSequence = False

    if inSequence:
      if line[0:3] == "%N ":
        sequence["name"] = line[11:-1]

  return sequence

def getSequences(data):
  keys = getKeys(data)
  sequences = dict([])
  for key in keys:
    sequences[key] = getSequence(data, key)

  return sequences

def getKeys(data):
  keys = []
  for line in data.split("\n"):
    if line[0:3] == "%I ":
      keys.append(line[3:10])

  return keys
