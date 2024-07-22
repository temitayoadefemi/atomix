
class IdentityContext:
    def __init__(self, atomix, identity_id):
        self.atomix = atomix
        self.identity_id = identity_id

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def commit(self):
        self.atomix.commit(self.identity_id)

    def abort(self):
        self.atomix.abort(self.identity_id)

    def read(self, key):
        return self.atomix.read(self.identity_id, key)

    def write(self, key, value):
        self.atomix.write(self.identity_id, key, value)

    def delete(self, key):
        self.atomix.delete(self.identity_id, key)

    def store(self, key, value):
        self.atomix.store(self.identity_id, key, value)

    def offload_store(self):
        self.atomix.offload_store(self.identity_id)
