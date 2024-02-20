from abc import abstractmethod


class ObjectStorageInterface:
    
    @abstractmethod
    def is_exists(self, name: str) -> bool:
        raise NotImplementedError
    
    @abstractmethod
    def get_object(self, name: str) -> str:
        raise NotImplementedError
    
    @abstractmethod
    def put_object(self, name: str, data: str) -> None:
        raise NotImplementedError
    
    @abstractmethod
    def del_object(self, name: str) -> bool:
        raise NotImplementedError
    
    @abstractmethod
    def list_object(self, path: str) -> list:
        raise NotImplementedError
