""" Producer, Consumer, and Admin interface classes """

from abc import ABC, abstractmethod


class ProducerInterface(ABC):
    @abstractmethod
    def send(self):
        pass

    @abstractmethod
    def close(self):
        pass


class ConsumerInterface(ABC):
    @abstractmethod
    def recieve(self):
        pass

    @abstractmethod
    def subscribe(self):
        pass

    @abstractmethod
    def close(self):
        pass


class AdminInterface(ABC):
    @abstractmethod
    def validate_connection(self):
        pass

    @abstractmethod
    def close(self):
        pass
