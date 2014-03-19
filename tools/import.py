import general, oeis
import json, os, sqlite3

# the list of all fields in the JSON that correspond to columns in the operads table
fields = ["dual", "representation", "dimension", "dimension_expression", "series", "unit"]


# create an operad in the database
def createOperad(operadKey, name, notation):
  assert not operadExists(operadKey)

  print "Creating the operad with key ", operadKey

  try:
    query = "INSERT INTO operads (key, name, notation) VALUES (?, ?, ?)"
    cursor.execute(query, (operadKey, name, notation))

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

# add a property to an operad
def addProperty(operadKey, propertyKey):
  assert not operadHasProperty(operadKey, propertyKey)

  try:
    query = "INSERT INTO operad_property (operad, property) VALUES (?, ?)"
    cursor.execute(query, (operadKey, propertyKey))

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

# create a property in the database
def createProperty(operadKey, propertyKey, slogan, definition):
  assert not propertyExists(propertyKey)

  try:
    query = "INSERT INTO properties (key, name, slogan, definition) VALUES (?, ?, ?, ?)"
    cursor.execute(query, (operadKey, propertyKey, slogan, definition))

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

# return the filename to an operad JSON file
def operadFile(key):
  return "../operads/" + key + ".json"

# check whether an operad exists in the database
def operadExists(operadKey):
  try:
    query = "SELECT COUNT(*) FROM operads WHERE key = ?"
    result = connection.execute(query, (operadKey,))

    return result.fetchone()[0]

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

  return False

# check whether an operad exists in the filesystem
def operadFileExists(key):
  return os.path.isfile(operadFile(key))

# check whether an operad has a property (i.e. whether there is an element in the operad_property table)
def operadHasProperty(operadKey, propertyKey):
  try:
    query = "SELECT COUNT(*) FROM operad_property WHERE operad = ? AND property = ?"
    result = connection.execute(query, (operadKey, propertyKey))

    return result.fetchone()[0]

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

  return False

def operadHasReference(operadKey, citationKey):
  try:
    query = "SELECT COUNT(*) FROM operad_reference WHERE key = ? AND citation_key = ?"
    result = connection.execute(query, (operadKey, citationKey))

    return result.fetchone()[0]

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

  return False

# check whether a property exists in the database
def propertyExists(propertyKey):
  try:
    query = "SELECT COUNT(*) FROM properties WHERE key = ?"
    result = connection.execute(query, (propertyKey,))

    return result.fetchone()[0]

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

  return False

# get all references associated to an operad
def getReferences(operadKey):
  try:
    query = "SELECT citation_key FROM operad_reference WHERE key = ?"
    result = connection.execute(query, (operadKey,))

    result = result.fetchall()
    for i in range(len(result)):
      result[i] = result[i][0]
    return result

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

  return []

# remove a reference from an operad
def removeReference(operadKey, citationKey):
  assert operadHasReference(operadKey, citationKey)

  try:
    query = "DELETE FROM operad_reference WHERE key = ? AND citation_key = ?"
    cursor.execute(query, (operadKey, citationKey))

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

# import an operad (represented as JSON data structure) from the filesystem
def readOperad(key):
  assert operadFileExists(key)

  f = open(operadFile(key))
  operad = json.load(f)

  return operad

# add a reference to an operad
def addReference(operadKey, citationKey):
  assert not operadHasReference(operadKey, citationKey)

  try:
    query = "INSERT INTO operad_reference (key, citation_key) VALUES (?, ?)"
    cursor.execute(query, (operadKey, citationKey))

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

# check whether an operad already has a certain "expression" related to it
def operadHasExpression(operadKey, category, expression, description):
  try:
    query = "SELECT COUNT(*) FROM expressions WHERE key = ? AND category = ? AND expression = ? AND description = ?"
    result = connection.execute(query, (operadKey, category, expression, description))

    return result.fetchone()[0]

  except sqlite3.Error, e:
    print query
    print "An error occurred:", e.args[0]

  return False

# create an "expression", i.e. an operation, symmetry or relation
def createExpression(operadKey, category, expression, description):
  assert not operadHasExpression(operadKey, category, expression, description)
  assert category in ["operation", "symmetry", "relation"]

  try:
    query = "INSERT INTO expressions (key, category, expression, description) VALUES (?, ?, ?, ?)"
    cursor.execute(query, (operadKey, category, expression, description))

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]


# update an operad
def updateOperad(operadKey, operad):
  # update its properties
  for propertyKey in operad["properties"]:
    if not operadHasProperty(operadKey, propertyKey):
      print "Adding the property ", propertyKey, " to the operad with key ", operadKey
      addProperty(operadKey, propertyKey)

  # general fields with a single value can be parsed this way
  for field in fields:
    if field in operad.keys() and not getValue(operadKey, field) == operad[field]:
      print "Updating the field ", field, " in ", operadKey
      setValue(operadKey, field, operad[field])

  # dimensions need a different way of handling
  if "dimensions" in operad.keys() and not getValue(operadKey, "dimensions") == str(operad["dimensions"]):
    print "Updating the field dimensions in ", operadKey
    setValue(operadKey, "dimensions", str(operad["dimensions"]))

  # handle references to OEIS
  if "dimensions" in operad.keys() and len(operad["dimensions"]) == 7 and getValue(operadKey, "oeis") == None:
    query = ",".join(str(n) for n in operad["dimensions"])
    data = oeis.getData(query)
    print "looking for " + str(operad["dimensions"])
    candidates = oeis.getSequences(data)

    if len(candidates) == 0:
      print "There is no OEIS sequence matching these dimensions"
    elif len(candidates) == 1:
      if getValue(operadKey, "oeis") != candidates.keys()[0]:
        print "There is one OEIS sequence matching these dimensions, adding it to ", operadKey
        setValue(operadKey, "oeis", candidates.keys()[0])
    else:
      print "There are more candidates matching these dimensions"
      for OEISkey, sequence in candidates.iteritems():
        print "  ", OEISkey, ": ", sequence["name"]


  # references need a different way of handling
  if "references" in operad.keys():
    # checking which references have been removed
    oldReferences = getReferences(operadKey)
    for reference in oldReferences:
      if reference not in operad["references"]:
        print "The reference ", reference, " has been removed from the operad with key ", operadKey
        removeReference(operadKey, reference)

    # checking which references are new and adding them
    for reference in operad["references"]:
      if not operadHasReference(operadKey, reference):
        print "Adding the reference ", reference, " to the operad with key ", operadKey
        addReference(operadKey, reference)

  # expressions need a different way of handling
  for category in [("operation", "operations"), ("symmetry", "symmetries"), ("relation", "relations")]:
    if category[1] in operad.keys():
      for expression in operad[category[1]]:
        # checking which references have been removed
        # TODO implement this

        # checking which expressions are new and adding them
        if not operadHasExpression(operadKey, category[0], expression[category[0]], expression["description"]):
          print "Adding the expression ", expression[category[0]], " of category ", category[0], " to the operad with key ", operadKey
          createExpression(operadKey, category[0], expression[category[0]], expression["description"])

# generic code to get a value of a column in the operads table
def getValue(operadKey, field):
  try:
    query = "SELECT " + field + " FROM operads WHERE key = ?"
    result = connection.execute(query, (operadKey,))

    return result.fetchone()[0]

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

  return ""

# generic code to change a column of an operad
def setValue(operadKey, field, value):
  try:
    query = "UPDATE operads SET " + field + " = ? WHERE key = ?"
    result = connection.execute(query, (value, operadKey))

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

def importOperad(filename):
  operad = readOperad(filename)
  
  if not operadExists(operad["key"]):
    createOperad(operad["key"], operad["name"], operad["notation"])

  updateOperad(operad["key"], operad)

def clearProperties():
  try:
    query = "DELETE FROM properties"
    connection.execute(query)

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

def importProperties():
  f = open("../properties.json")
  properties = json.load(f)

  for p in properties:
    createProperty(p["key"], p["name"], p["slogan"], p["definition"])


# actual execution code
global connection
(connection, cursor) = general.connect()

clearProperties() # this is easier than checking whether they have been updated
importProperties()

files = os.listdir("../operads")
for filename in files:
  if filename.endswith(".json"):
    importOperad(filename.split(".")[0])


general.close(connection)
