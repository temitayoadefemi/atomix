class VersionedValue():
    def __init__(self, value, version):
        self.value = value
        self.version = version
        self.deleted = False