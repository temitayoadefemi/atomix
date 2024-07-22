
class IdentityContext:
    def __init__(self, atomix, identity_id):
        self.identity_id = identity_id

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def commit(self, atomix):
        atomix.commit(self.identity_id)

    def abort(self, atomix):
        atomix.abort(self.identity_id)

    def read(self, key, atomix):
        return atomix.read(self.identity_id, key)

    def write(self, key, value, atomix):
        atomix.write(self.identity_id, key, value)

    def delete(self, key, atomix):
        atomix.delete(self.identity_id, key)

    def store(self, key, value, atomix):
        atomix.store(self.identity_id, key, value)

    def offload_store(self, atomix):
        atomix.offload_store(self.identity_id)
