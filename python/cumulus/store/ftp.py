
from ftplib        import FTP, all_errors
from netrc         import netrc, NetrcParseError
from cumulus.store import Store, type_patterns, NotFoundError

class FtpStore (Store):
    def __init__ (self, url, **kw):
        self.synced = True
        try:
            upw, hp = self.netloc.split ('@')
        except ValueError:
            hp = self.netloc
            upw = 'anonymous'
        try:
            host, port = hp.split (':')
            port = int (port, 10)
        except ValueError:
            host = hp
            port = 21
        try:
            user, passwd = upw.split (':')
        except ValueError:
            user = upw
            passwd = None
            try:
                n = netrc ()
                try:
                    user, acct, passwd = n.authenticators (host)
                except ValueError:
                    pass
            except (IOError, NetrcParseError):
                pass
        self.ftp = FTP ()
        self.ftp.connect (host, port)
        self.ftp.login (user, passwd)
        self.prefix = self.path [1:] # skip *only* first '/'
        self.ftp.cwd (self.prefix)

    def _get_path (self, type, name):
        # we are in right directory
        return name

    def list (self, type):
        self.sync ()
        files = self.ftp.nlst ()
        return (f for f in files if type_patterns[type].match (f))

    def get (self, type, name):
        self.sync ()
        sock = self.ftp.transfercmd ('RETR %s' % self._get_path (type, name))
        self.synced = False
        return sock.makefile ()

    def put (self, type, name, fp):
        self.sync ()
        self.ftp.storbinary ("STOR %s" % self._get_path (type, name), fp)

    def delete (self, type, name):
        self.sync ()
        self.ftp.delete (self._get_path (type, name))

    def stat (self, type, name):
        """ Note that the size-command is non-standard but supported by
        most ftp servers today. If size returns an error condition we
        try nlst to detect if the file exists and return an bogus length
        """
        self.sync ()
        fn = self._get_path (type, name)
        size = None
        try:
            # my client doesn't accept size in ascii-mode
            self.ftp.sendcmd ('TYPE I')
            size = self.ftp.size (fn)
            self.ftp.sendcmd ('TYPE A')
        except all_errors, err:
            print err
            pass
        if size is not None:
            return {'size': size}
        print "nlst: %s" % fn, size
        l = self.ftp.nlst (fn)
        if l:
            return {'size': 42}
        raise NotFoundError, (type, name)

    def sync (self):
        """ After a get command at end of transfer a 2XX reply is still
        in the input-queue, we have to get rid of that
        """
        if not self.synced:
            self.ftp.voidresp()
        self.synced = True

Store = FtpStore
