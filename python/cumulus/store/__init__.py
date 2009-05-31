import exceptions, re, urlparse

type_patterns = {
    'checksums': re.compile(r"^snapshot-(.*)\.(\w+)sums$"),
    'segments': re.compile(r"^([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(\.\S+)?$"),
    'snapshots': re.compile(r"^snapshot-(.*)\.lbs$")
}

class NotFoundError(exceptions.KeyError):
    """Exception thrown when a file is not found in a repository."""

    pass

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

    def stat(self, type, name):
        raise NotImplementedException

    def scan(self):
        """Cache file information stored in this backend.

        This might make subsequent list or stat calls more efficient, but this
        function is intended purely as a performance optimization."""

        pass

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
