from pymongo import MongoClient
from .base import BaseModel


class DataBase(BaseModel):

    def __init__(self, uri="mongodb://localhost:27017/", db_name="mydb"):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]


    def __getitem__(self, collection_name):
        return self.db[collection_name]


    def insert(self, collection, data):
        if isinstance(data, list):
            return self.db[collection].insert_many(data).inserted_ids
        else:
            return self.db[collection].insert_one(data).inserted_id

    
    def get(self, collection, query=None, fields=None):
        return list(self.db[collection].find(query or {}, fields or {}))


    def update(self, collection, query, new_values):
        return self.db[collection].update_many(query, {"$set": new_values})

    def delete(self, collection, query):
        return self.db[collection].delete_many(query)
    

if __name__=="__main__":
        aa=DataBase(uri="mongodb://localhost:27017/", db_name="mydbtest1")
        aa.insert("maryam",{"ali":"20"})