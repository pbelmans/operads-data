import general, oeis
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

# check whether an operad already has a certain "expression" related to it
def operadHasExpression(key, category, expression, description):
  try:
    query = "SELECT COUNT(*) FROM expressions WHERE key = ? AND category = ? AND expression = ? AND description = ?"
    result = connection.execute(query, (key, category, expression, description))

    return result.fetchone()[0]

  except sqlite3.Error, e:
    print query
    print "An error occurred:", e.args[0]

  return False

# create an "expression", i.e. an operation, symmetry or relation
def createExpression(key, category, expression, description):
  assert not operadHasExpression(key, category, expression, description)
  assert category in ["operation", "symmetry", "relation"]

  try:
    query = "INSERT INTO expressions (key, category, expression, description) VALUES (?, ?, ?, ?)"
    cursor.execute(query, (key, category, expression, description))

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

  # general fields with a single value can be parsed this way
  for field in fields:
    if field in operad.keys() and not getValue(key, field) == operad[field]:
      print "Updating the field ", field, " in ", key
      setValue(key, field, operad[field])

  # dimensions need a different way of handling
  if "dimensions" in operad.keys() and not getValue(key, "dimensions") == str(operad["dimensions"]):
    print "Updating the field dimensions in ", key
    setValue(key, "dimensions", str(operad["dimensions"]))

  # handle references to OEIS
  if "dimensions" in operad.keys() and len(operad["dimensions"]) == 7 and getValue(key, "oeis") == None:
    query = ",".join(str(n) for n in operad["dimensions"])
    data = oeis.getData(query)
    print "looking for " + str(operad["dimensions"])
    candidates = oeis.getSequences(data)

    if len(candidates) == 0:
      print "There is no OEIS sequence matching these dimensions"
    elif len(candidates) == 1:
      if getValue(key, "oeis") != candidates.keys()[0]:
        print "There is one OEIS sequence matching these dimensions, adding it to ", key
        setValue(key, "oeis", candidates.keys()[0])
    else:
      print "There are more candidates matching these dimensions"
      for OEISkey, sequence in candidates.iteritems():
        print "  ", OEISkey, ": ", sequence["name"]


  # references need a different way of handling
  if "references" in operad.keys():
    # checking which references have been removed
    oldReferences = getReferences(key)
    for reference in oldReferences:
      if reference not in operad["references"]:
        print "The reference ", reference, " has been removed from the operad with key ", key
        removeReference(key, reference)

    # checking which references are new and adding them
    for reference in operad["references"]:
      if not operadHasReference(key, reference):
        print "Adding the reference ", reference, " to the operad with key ", key
        addReference(key, reference)

  # expressions need a different way of handling
  for category in [("operation", "operations"), ("symmetry", "symmetries"), ("relation", "relations")]:
    if category[1] in operad.keys():
      for expression in operad[category[1]]:
        # checking which references have been removed
        # TODO implement this

        # checking which expressions are new and adding them
        if not operadHasExpression(key, category[0], expression[category[0]], expression["description"]):
          print "Adding the expression ", expression[category[0]], " of category ", category[0], " to the operad with key ", key
          createExpression(key, category[0], expression[category[0]], expression["description"])

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
