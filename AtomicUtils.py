import threading

class AtomicBoolean(object):

    def __init__(self, start = False):
        self.lock = threading.Lock()
        self.value = start
    
    def negate(self):
        self.lock.acquire()
        try:
            self.value = not self.value
        finally:
            self.lock.release()

    def get(self):
        return self.value

    def set(self, new_state):
        self.lock.acquire()
        try:
            self.value = new_state
        finally:
            self.lock.release()