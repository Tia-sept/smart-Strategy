from abc import ABC, abstractmethod


class Strategy(ABC):
    @abstractmethod
    def detect_leader(self):
        pass