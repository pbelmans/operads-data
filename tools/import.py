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


# return the filename to an operad JSON file
def operadFile(key):
  return "../operads/" + key + ".json"

# check whether an operad exists in the database
def operadExists(key):
  try:
    query = 'SELECT COUNT(*) FROM operads WHERE key = ?'
    result = connection.execute(query, [key])

    return result.fetchone()[0]

  except sqlite3.Error, e:
    print "An error occurred:", e.args[0]

  return False

# check whether an operad exists in the filesystem
def operadFileExists(key):
  return os.path.isfile(operadFile(key))


def readOperad(key):
  assert operadFileExists(key)

  f = open(operadFile(key))
  operad = json.load(f)

  return operad

# actual execution code
global connection
(connection, cursor) = general.connect()


operad = readOperad("lie")
print operad

if not operadExists("lie"):
  createOperad("lie", operad["name"], operad["notation"])


general.close(connection)
