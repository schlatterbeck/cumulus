import re, urlparse

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

def open(url):
    (scheme, netloc, path, params, query, fragment) \
        = urlparse.urlparse(url)

    if scheme == "file":
        import cumulus.store.file
        return cumulus.store.file.FileStore(path)
    elif scheme == "s3":
        import cumulus.store.s3
        while path.startswith("/"): path = path[1:]
        (bucket, path) = path.split("/", 1)
        return cumulus.store.s3.S3Store(bucket, path)
    else:
        raise NotImplementedException
