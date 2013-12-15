import general
import json, os, sqlite3


# create an operad in the database
def createOperad(key, name, notation):
  assert not operadExists(key)

  try:
    query = "INSERT INTO operads (key, name, notation) VALUES (?, ?, ?)"
    cursor.execute(query, (key, name, notation))

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

# add a property to an operad
def addProperty(key, name):
  assert not operadHasProperty(key, name)

  try:
    query = "INSERT INTO operad_property (key, name) VALUES (?, ?)"
    cursor.execute(query, (key, name))

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

# create a property in the database
def createProperty(name):
  assert not propertyExists(name)

  try:
    query = "INSERT INTO properties (name) VALUES (?)"
    cursor.execute(query, (name))

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

# return the filename to an operad JSON file
def operadFile(key):
  return "../operads/" + key + ".json"

# check whether an operad exists in the database
def operadExists(key):
  try:
    query = "SELECT COUNT(*) FROM operads WHERE key = ?"
    result = connection.execute(query, [key])

    return result.fetchone()[0]

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

  return False

# check whether an operad exists in the filesystem
def operadFileExists(key):
  return os.path.isfile(operadFile(key))

# check whether an operad has a property (i.e. whether there is an element in the operad_property table)
def operadHasProperty(key, name):
  try:
    query = "SELECT COUNT(*) FROM operad_property WHERE key = ? AND name = ?"
    result = connection.execute(query, [key, name])

    return result.fetchone()[0]

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

  return False

# check whether a property exists in the database
def propertyExists(name):
  try:
    query = "SELECT COUNT(*) FROM properties WHERE name = ?"
    result = connection.execute(query, [name])

    return result.fetchone()[0]

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

  return False

# import an operad (represented as JSON data structure) from the filesystem
def readOperad(key):
  assert operadFileExists(key)

  f = open(operadFile(key))
  operad = json.load(f)

  return operad

# update an operad
def updateOperad(key, operad):
  # update its properties
  for name in operad["properties"]:
    if not operadHasProperty(key, name):
      print "Adding the property ", name, " to the operad with key ", key
      addProperty(key, name)
    #else:
    #  print "The operad with key ", key, " already has the property ", name
  # TODO remove properties that no longer exist

# actual execution code
global connection
(connection, cursor) = general.connect()


operad = readOperad("lie")
print operad

if not operadExists("lie"):
  createOperad("lie", operad["name"], operad["notation"])

updateOperad("lie", operad)

general.close(connection)