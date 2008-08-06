import re

type_patterns = {
    'checksums': re.compile(r"^snapshot-(.*)\.(\w+)sums$"),
    'segments': re.compile(r"^([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(\.\S+)?$"),
    'snapshots': re.compile(r"^snapshot-(.*)\.lbs$")
}

class Store:
    """Base class for all cumulus storage backends."""

    def list(self, type):
        raise NotImplementedException

    def get(self, type, name):
        raise NotImplementedException

    def put(self, type, name, fp):
        raise NotImplementedException

    def delete(self, type, name):
        raise NotImplementedException
