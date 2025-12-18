from abc import ABC, abstractmethod

class BaseModel(ABC):

    @abstractmethod
    def insert(self, collection, data):
        pass

    @abstractmethod
    def get(self, collection, query=None):
        pass

    @abstractmethod
    def update(self, collection, query, new_values):
        pass

    @abstractmethod
    def delete(self, collection, query):
        pass
