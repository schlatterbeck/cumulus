# Cumulus: Efficient Filesystem Backup to the Cloud
# Copyright (C) 2009 The Cumulus Developers
# See the AUTHORS file for a list of contributors.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

from __future__ import division, print_function, unicode_literals

from ftplib        import FTP, all_errors, error_temp, error_perm
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
        self.host   = host
        self.port   = port
        self.user   = user
        self.passwd = passwd
        self.prefix = self.path [1:] # skip *only* first '/'
        self.ftp    = FTP ()
        self.connect ()

    def _get_dir (self, type, name):
        # We put all files in directories starting with the first 3
        # characters of the filename
        # This works around limitations of some ftp servers that return
        # only the first 10000 files when listing directories.
        # Note that we only have files with hex characters in the first
        # 3 charactars plus files starting with 'sna' (for snapshot)
        return name [:3]

    def _get_path (self, type, name):
        return '/'.join ((self._get_dir (type, name), name))

    def connect (self) :
        self.ftp.connect (self.host, self.port)
        self.ftp.login (self.user, self.passwd)
        self.ftp.cwd (self.prefix)
    # end def connect

    def list (self, type):
        self.sync ()
        dirs = self.ftp.nlst ()
        files = []
        for d in dirs :
            files.extend (f.split ('/') [-1] for f in self.ftp.nlst (d))
        return (f for f in files if type_patterns[type].match (f))

    def get (self, type, name):
        self.sync ()
        self.ftp.sendcmd ('TYPE I')
        sock = self.ftp.transfercmd ('RETR %s' % self._get_path (type, name))
        self.synced = False
        return sock.makefile ()

    def put (self, type, name, fp):
        self.sync ()
        try :
            self.ftp.mkd (self._get_dir (type, name))
        except error_perm :
            pass
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
        except all_errors as err:
            print(err)
            pass
        if size is not None:
            return {'size': size}
        print("nlst: %s" % fn, size)
        l = self.ftp.nlst (fn)
        if l:
            return {'size': 42}
        raise NotFoundError(type, name)

    def sync (self):
        """ After a get command at end of transfer a 2XX reply is still
        in the input-queue, we have to get rid of that.
        We also test here that the connection is still alive. If we get
        a temporary error 421 ("error_temp") we reconnect: It was
        probably a timeout.
        """
        try :
            if not self.synced:
                self.ftp.voidresp()
            self.ftp.sendcmd ('TYPE A')
        except error_temp as err :
            if not err.message.startswith ('421') :
                raise
            self.connect ()
        self.synced = True

Store = FtpStore
