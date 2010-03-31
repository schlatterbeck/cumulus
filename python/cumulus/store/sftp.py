# vim: ai ts=4 sts=4 et sw=4
#needed for python 2.5
from __future__ import with_statement

from paramiko import Transport, SFTPClient, RSAKey, DSSKey
from paramiko.config import SSHConfig
import paramiko.util
from cumulus.store import Store, type_patterns, NotFoundError
import os, os.path
import getpass
import re
import sys


class SSHHostConfig(dict):
    def __init__(self, hostname, user = None, filename = None):
        #set defaults
        if filename == None:
            filename = os.path.expanduser('~/.ssh/config')

        #read config file
        ssh_config = SSHConfig()
        with open(filename) as config_file:
            ssh_config.parse(config_file)

        self.update(ssh_config.lookup(hostname))

        self.defaults={'port': 22, 'user': getpass.getuser(), 'hostname': hostname, 'hostkeyalias': hostname}

        if user != None:
            self['user'] = user

    def __getitem__(self, key):
        if key in self:
            return dict.__getitem__(self,key)
        elif key == 'hostkeyalias' and 'hostname' in self:
            return dict.__getitem__(self,'hostname')
        else:
            return self.defaults[key]


class SFTPStore(Store):
    """implements the sftp:// storage backend

        configuration via openssh/sftp style urls and
        .ssh/config files

        does not support password authentication or password
        protected authentication keys"""
    def __init__(self, url, **kw):
        if self.netloc.find('@') != -1:
            user, self.netloc = self.netloc.split('@')
        else:
            user = None

        self.config = SSHHostConfig(self.netloc, user)

        host_keys = paramiko.util.load_host_keys(os.path.expanduser('~/.ssh/known_hosts'))
        try:
            self.hostkey = host_keys[self.config['hostkeyalias']].values()[0]
        except:
            print str(self.config)
            raise


        if(self.config.has_key('identityfile')):
            key_file = os.path.expanduser(self.config['identityfile'])
            #not really nice but i don't see a cleaner way atm...
            try:
                self.auth_key = RSAKey (key_file)
            except SSHException, e:
                if e.message == 'Unable to parse file':
                    self.auth_key = DSAKey (key_file)
                else:
                    raise
        else:
            filename = os.path.expanduser('~/.ssh/id_rsa')
            if os.path.exists(filename):
                self.auth_key = RSAKey(filename)
            else:
                filename = os.path.expanduser('~/.ssh/id_dsa')
                if (os.path.exists(filename)):
                    self.auth_key = DSSKey (filename)

        self.__connect()

    def __connect(self):
        self.t = Transport((self.config['hostname'], self.config['port']))
        self.t.connect(username = self.config['user'], pkey = self.auth_key)
        self.client = SFTPClient.from_transport(self.t)
        self.client.chdir(self.path)

    def __build_fn(self, name):
        return "%s/%s" % (self.path,  name)

    def list(self, type):
        return filter(type_patterns[type].match, self.client.listdir(self.path))

    def get(self, type, name):
        return self.client.open(self.__build_fn(name), mode = 'rb')

    def put(self, type, name, fp):
        remote_file = self.client.open(self.__build_fn(name), mode = 'wb')
        buf = fp.read(4096)
        while (len(buf) > 0):
            remote_file.write(buf)
            buf = fp.read(4096)
        remote_file.close()

    def delete(self, type, name):
        self.client.remove(self.__build_fn(name))

    def stat(self, type, name):
        try:
            stat = self.client.stat(self.__build_fn(name))
            return {'size': stat.st_size}
        except IOError:
            raise NotFoundError

    def close(self):
        """connection has to be explicitly closed, otherwise
            it will hold the process running idefinitly"""
        self.client.close()
        self.t.close()

Store = SFTPStore
