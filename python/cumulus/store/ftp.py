
from __future__ import print_function
import sys
from ftplib        import FTP, all_errors, error_temp, error_perm
from netrc         import netrc, NetrcParseError
import cumulus.store

def throw_notfound(method):
    """Decorator to convert a FTP 450 error into a
       cumulus.store.NoutFoundError.
    """
    def f(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except error_perm as e:
	    raise cumulus.store.NotFoundError(e)
    return f

class Store (cumulus.store.Store):
    """ Storage backend that accesses a remote FTP server."""
    def __init__ (self, url, **kw):
        super(Store, self).__init__(url)
        self.url = url
        self.synced = True
        upw = url.password
        port = url.port
        if port is None:
            port = '21'
        port = int (port, 10)
        host = url.hostname
        user = url.username
        pw = url.password

        if pw is None or user is None:
            try:
                n = netrc ()
                try:
                    user, acct, pw = n.authenticators (host)
                except ValueError:
                    pass
            except (IOError, NetrcParseError):
                pass
        self.host   = host
        self.port   = port
        self.user   = user
        self.passwd = pw
        self.prefix = url.path [1:] # skip *only* first '/'
        self.ftp    = FTP ()
        self.connect ()

    def _get_dir (self, path):
        # We put all files in directories starting with the first 3
        # characters of the filename
        # This works around limitations of some ftp servers that return
        # only the first 10000 files when listing directories.
        # Note that we only have files with hex characters in the first
        # 3 charactars plus files starting with 'sna' (for snapshot)
        components = path.split ('/')
        n = path.rsplit ('/', 1) [-1][:3]
        components.insert (-1, n)
        return '/'.join (components [:-1])

    def _get_path (self, path):
        n = path.rsplit ('/', 1) [-1]
        return '/'.join ((self._get_dir (path), n))

    def _mkdir (self, path):
        prefix = []
        components = path.split ('/')
        for c in components:
            prefix.append (c)
            try:
                self.ftp.mkd ('/'.join (prefix))
            except error_perm:
                pass

    def connect (self):
        self.ftp.connect (self.host, self.port)
        self.ftp.login (self.user, self.passwd)
        try:
            self.ftp.mkd (self.prefix)
        except error_perm:
            pass
        self.ftp.cwd (self.prefix)
    # end def connect

    @throw_notfound
    def list (self, path):
        self.sync ()
        dirs = self.ftp.nlst (path)
        #print ("Path:", path, file=sys.stderr)
        #print ("DIRS:", dirs, file=sys.stderr)
        files = []
        for d in dirs:
            files.extend (f.split ('/') [-1] for f in self.ftp.nlst (d))
	#print ("Files:", files, file=sys.stderr)
        return files

    @throw_notfound
    def get (self, path):
	#print ("GET:", path, file=sys.stderr)
        self.sync ()
        self.ftp.sendcmd ('TYPE I')
        sock = self.ftp.transfercmd ('RETR %s' % self._get_path (path))
        self.synced = False
        return sock.makefile ()

    @throw_notfound
    def put (self, path, fp):
        self.sync ()
        try:
            self._mkdir (self._get_dir (path))
        except error_perm:
            pass
        self.sync ()
        self.ftp.storbinary ("STOR %s" % self._get_path (path), fp)

    @throw_notfound
    def delete (self, path):
        self.sync ()
        self.ftp.delete (self._get_path (path))

    def stat (self, path):
        """ Note that the size-command is non-standard but supported by
        most ftp servers today. If size returns an error condition we
        try nlst to detect if the file exists and return an bogus length
        """
        self.sync ()
        fn = self._get_path (path)
        size = None
	#print ("STAT:", path, file=sys.stderr)
        try:
            # my server doesn't accept size in ascii-mode
            self.ftp.sendcmd ('TYPE I')
            size = self.ftp.size (fn)
            self.ftp.sendcmd ('TYPE A')
	except error_perm as err:
	    #print (dir(err), err.args, file=sys.stderr)
	    raise cumulus.store.NotFoundError(err.message)
        except all_errors as err:
            print ("ERROR:", err, file=sys.stderr)
            raise
        if size is not None:
            return {'size': size}
        #print ("nlst: %s" % fn, size)
        l = self.ftp.nlst (fn)
        if l:
            return {'size': 42}
        raise cumulus.store.NotFoundError(path)

    def sync (self):
        """ After a get command at end of transfer a 2XX reply is still
        in the input-queue, we have to get rid of that.
        We also test here that the connection is still alive. If we get
        a temporary error 421 ("error_temp") we reconnect: It was
        probably a timeout.
        """
        try:
            if not self.synced:
                self.ftp.voidresp()
            self.ftp.sendcmd ('TYPE A')
        except error_temp as err:
            if not err.message.startswith ('421'):
                raise
            self.connect ()
        self.synced = True
