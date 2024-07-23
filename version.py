class VersionedData:
    def __init__(self, value, version, committed=False):
        self.value = value
        self.version = version
        self.committed = committed
        self.deleted = False