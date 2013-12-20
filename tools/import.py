import general
import json, os, sqlite3

# the list of all fields in the JSON that correspond to columns in the operads table
fields = ["dual", "representation", "dimension", "dimension_expression", "series", "unit"]


# create an operad in the database
def createOperad(key, name, notation):
  assert not operadExists(key)

  print "Creating the operad with key ", key

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
    cursor.execute(query, (name,))

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

# return the filename to an operad JSON file
def operadFile(key):
  return "../operads/" + key + ".json"

# check whether an operad exists in the database
def operadExists(key):
  try:
    query = "SELECT COUNT(*) FROM operads WHERE key = ?"
    result = connection.execute(query, (key,))

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
    result = connection.execute(query, (key, name))

    return result.fetchone()[0]

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

  return False

def operadHasReference(key, citation_key):
  try:
    query = "SELECT COUNT(*) FROM operad_reference WHERE key = ? AND citation_key = ?"
    result = connection.execute(query, (key, citation_key))

    return result.fetchone()[0]

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

  return False

# check whether a property exists in the database
def propertyExists(name):
  try:
    query = "SELECT COUNT(*) FROM properties WHERE name = ?"
    result = connection.execute(query, (name,))

    return result.fetchone()[0]

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

  return False

# get all references associated to an operad
def getReferences(key):
  try:
    query = "SELECT citation_key FROM operad_reference WHERE key = ?"
    result = connection.execute(query, (key,))

    result = result.fetchall()
    for i in range(len(result)):
      result[i] = result[i][0]
    return result

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

  return []

# remove a reference from an operad
def removeReference(key, citation_key):
  assert operadHasReference(key, citation_key)

  try:
    query = "DELETE FROM operad_reference WHERE key = ? AND citation_key = ?"
    cursor.execute(query, (key, citation_key))

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

# import an operad (represented as JSON data structure) from the filesystem
def readOperad(key):
  assert operadFileExists(key)

  f = open(operadFile(key))
  operad = json.load(f)

  return operad

# add a reference to an operad
def addReference(key, citation_key):
  assert not operadHasReference(key, citation_key)

  try:
    query = "INSERT INTO operad_reference (key, citation_key) VALUES (?, ?)"
    cursor.execute(query, (key, citation_key))

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

# update an operad
def updateOperad(key, operad):
  # update its properties
  for name in operad["properties"]:
    if not propertyExists(name):
      createProperty(name)

    if not operadHasProperty(key, name):
      print "Adding the property ", name, " to the operad with key ", key
      addProperty(key, name)
    #else:
    #  print "The operad with key ", key, " already has the property ", name
  # TODO remove properties that no longer exist

  for field in fields:
    if field in operad.keys() and not getValue(key, field) == operad[field]:
      print "Updating the field ", field, " in ", key
      setValue(key, field, operad[field])
  # dimensions need a different way of handling
  if "dimensions" in operad.keys() and not getValue(key, "dimensions") == str(operad["dimensions"]):
    print "Updating the field dimensions in ", key
    setValue(key, "dimensions", str(operad["dimensions"]))
  # references need a different way of handling
  if "references" in operad.keys():
    oldReferences = getReferences(key)

    # checking which references have been removed
    for reference in oldReferences:
      if reference not in operad["references"]:
        print "The reference ", reference, " has been removed from the operad with key ", key
        removeReference(key, reference)

    # checking which references are new and adding them
    for reference in operad["references"]:
      if not operadHasReference(key, reference):
        print "Adding the reference ", reference, " to the operad with key ", key
        addReference(key, reference)

# generic code to get a value of a column in the operads table
def getValue(key, field):
  try:
    query = "SELECT " + field + " FROM operads WHERE key = ?"
    result = connection.execute(query, (key,))

    return result.fetchone()[0]

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

  return ""

# generic code to change a column of an operad
def setValue(key, field, value):
  try:
    query = "UPDATE operads SET " + field + " = ? WHERE key = ?"
    result = connection.execute(query, (value, key))

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

def importOperad(filename):
  operad = readOperad(filename)
  
  if not operadExists(operad["key"]):
    createOperad(operad["key"], operad["name"], operad["notation"])

  updateOperad(operad["key"], operad)

# actual execution code
global connection
(connection, cursor) = general.connect()

files = os.listdir("../operads")
for filename in files:
  if filename.endswith(".json"):
    importOperad(filename.split(".")[0])


general.close(connection)
