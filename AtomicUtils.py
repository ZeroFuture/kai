import threading

class AtomicBoolean(object):

    def __init__(self, start = False):
        self.lock = threading.Lock()
        self.value = start
    
    @classmethod
    def negate(cls):
        cls.lock.acquire()
        try:
            cls.value = not cls.value
        finally:
            cls.lock.release()

    @classmethod
    def get(cls):
        cls.lock.acquire()
        try:
            res = cls.value
        finally:
            cls.lock.release()
        return res