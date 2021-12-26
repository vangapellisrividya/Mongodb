import collections
import os
import pymongo
import random
from pymongo.results import DeleteResult

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["demodb"]

dblist = myclient.list_database_names()
if "demodb" in dblist:
  print("The database exists.")
else:
    print("not exists")
print(myclient.list_database_names())


def insert_many(mydb):
  '''
        Description : To insert documents into collection
        parameter : mydb
  '''
  collist = mydb.get_collection("customers4")
  mylist = [
  { "_id": 1, "name": "John", "address": "Highway 37"},
  { "_id": 2, "name": "Peter", "address": "Lowstreet 27"},
  { "_id": 3, "name": "Amy", "address": "Apple st 652"},
  { "_id": 4, "name": "Hannah", "address": "Mountain 21"},
  { "_id": 5, "name": "Michael", "address": "Valley 345"},
  { "_id": 6, "name": "Sandy", "address": "Ocean blvd 2"},
  { "_id": 7, "name": "Betty", "address": "Green Grass 1"},
  { "_id": 8, "name": "Richard", "address": "Sky st 331"},
  { "_id": 9, "name": "Susan", "address": "One way 98"},
  { "_id": 10, "name": "Vicky", "address": "Yellow Garden 2"},
  { "_id": 11, "name": "Ben", "address": "Park Lane 38"},
  { "_id": 12, "name": "William", "address": "Central st 954"},
  { "_id": 13, "name": "Chuck", "address": "Main Road 989"},
  { "_id": 14, "name": "Viola", "address": "Sideway 1633"}
]

  x = collist.insert_many(mylist)
  print(x.inserted_ids)
  for x in collist.find():
    print(x) 

  
  # response=collist.insert_many(customers1)
  # last_inserted_ids=response.inserted_ids
  # print(last_inserted_ids)

def delete(mydb):
  '''
        Description : To delete documents in the collection
        parameter : mydb
  '''
  collist = mydb.get_collection("customers3")
  myquery = { "_id": 14, "name": "Viola", "address": "Sideway 1633"}
  collist.delete_one(myquery) 
  x = collist.delete_one(myquery)
  print(x.deleted_count, " documents deleted.") 
  
def read(mydb):
  '''
        Description : To read documents in he collection
        parameter : mydb
  '''
  collist = mydb.get_collection("customers3")
  for x in collist.find():
    print(x)

def sort(mydb):
  '''
        Description :sort the documents in descendingorder
        parameter : mydb
  '''
  collist = mydb.get_collection("customers3")
  mydoc = collist.find().sort("name", -1)

  for x in mydoc:
    print(x) 

def drop(mydb):
  '''
        Description : To drop the collection
        parameter : mydb
  '''
  collist = mydb.get_collection("customers")
  collist.drop()
  print("collection is deleted")
  mydoc = collist.find()
  for x in mydoc:
    print(x)

def update(mydb):
  '''
        Description : To update the  documents in collection
        parameter : mydb
  '''
  collist = mydb.get_collection("customers3")
  myquery = { "_id": 1, "name": "John", "address": "Highway 37" }
  newvalues = { "$set": {  "name": "Jack", "address": "Hyd" } }
  collist.update_one(myquery, newvalues)

#print "customers" after the update:
  for x in collist.find():
    print(x) 

def index(mydb):
  '''
        Description : creates index of collection
        parameter : mydb
  '''
  
  for i in range(1000):
    name= "name"
    name+=str(i)
    salary=int(1000*random.random())
    mydb.indexpymongo.insert_one({"name":name,"salary":salary})
    
  
  collist=mydb.indexpymongo.find()
  #for doc in collist:
    #print(doc)
  
  mydb.indexpymongo.create_index('salary')
  collist=mydb.indexpymongo.index_information()
  for doc in collist:
    print(doc)
  collist=mydb.indexpymongo.find({'salary':100}).explain()
  # collist['executionStats']
  print(collist)
  for doc in collist:
    print(doc)


def _importexport(mydb):
  
  '''
        Description : To export and import the documents and files in
        mongodb
        parameter : mydb
  '''
  os.system("mongoexport -d demodb  -c indexpymongo -o test.csv") npm install run-rs -g

  os.system("sudo mongoimport --db sample2 --collection sample1 --file sample.csv")


if __name__=='__main__':
  #insert_many(mydb)
  #delete(mydb)
  #read(mydb)
  #sort(mydb)
  #drop(mydb)
  #update(mydb)
  #index(mydb)
  _importexport(mydb)
