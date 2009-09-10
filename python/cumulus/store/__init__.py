import exceptions, re, urlparse

type_patterns = {
    'checksums': re.compile(r"^snapshot-(.*)\.(\w+)sums$"),
    'segments': re.compile(r"^([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(\.\S+)?$"),
    'snapshots': re.compile(r"^snapshot-(.*)\.lbs$")
}

class NotFoundError(exceptions.KeyError):
    """Exception thrown when a file is not found in a repository."""

    pass

class Store (object):
    """Base class for all cumulus storage backends."""

    def __new__ (cls, url, **kw):
        """ Return the correct sub-class depending on url,
        pass parsed url parameters to object
        """
        if cls != Store:
            return super(Store, cls).__new__(cls, url, **kw)
        (scheme, netloc, path, params, query, fragment) \
            = urlparse.urlparse(url)

        try:
            cumulus = __import__('cumulus.store.%s' % scheme, globals())
            subcls = getattr (cumulus.store, scheme).Store
            obj = super(Store, cls).__new__(subcls, url, **kw)
            obj.scheme = scheme
            obj.netloc = netloc
            obj.path = path
            obj.params = params
            obj.query = query
            obj.fragment = fragment
            return obj
        except ImportError:
            raise NotImplementedError, "Scheme %s not implemented" % scheme

    def list(self, type):
        raise NotImplementedError

    def get(self, type, name):
        raise NotImplementedError

    def put(self, type, name, fp):
        raise NotImplementedError

    def delete(self, type, name):
        raise NotImplementedError

    def stat(self, type, name):
        raise NotImplementedError

    def scan(self):
        """Cache file information stored in this backend.

        This might make subsequent list or stat calls more efficient, but this
        function is intended purely as a performance optimization."""

        pass

def open(url):
    return Store(url)
