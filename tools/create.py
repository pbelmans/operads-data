# This script creates an empty database as used by operads-website
# After creation it should be placed in a directory with the correct chmod

import os.path, sqlite3, sys

def execute(filename):
  query = open("database/" + filename, "r").read()
  cursor = connection.cursor()
  cursor.execute(query)

tables = ["operads.sql"]
indices = "indices.sql"

if os.path.isfile("operads.sqlite"):
  print "The file operads.sqlite already exists in this folder, aborting"
  sys.exit()

print "Creating the database in operads.sqlite"

connection = sqlite3.connect("operads.sqlite")

map(execute, tables)
execute(indices)

connection.commit()
connection.close()

print "The database has been created"

