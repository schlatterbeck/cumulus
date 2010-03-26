# vim: ai ts=4 sts=4 et sw=4
from paramiko import Transport, SFTPClient, RSAKey, DSSKey
from paramiko.config import SSHConfig
import paramiko.util
from cumulus.store import Store, type_patterns, NotFoundError
import os, os.path
import getpass
import re
import sys
#needed for python 2.5
try:
    from __future__ import with_statement
except ImportError, e:
    raise ImportError('We need the "with" statement, avalible with python >=2.5')


class SSHHostConfig(dict):
    def __init__(self, hostname, user = None, filename = None):
        dict.__init__()
        #set defaults
        if filename == None:
            filename = os.path.expanduser('~/.ssh/config')
        self['port'] = 22
        self['user'] = getpass.getuser()
        self['hostname'] = hostname
        self['hostkeyalias'] = hostname

        #read config file
        ssh_config = SSHConfig()
        with open(filename) as config_file:
            ssh_config.parse(config_file)
        self.update(ssh_config.lookup(hostname))

        if user != None:
            self['user'] = user

class SFTPStore(Store):
    """implements the sftp:// storage backend

        configuration via openssh/sftp style urls and
        .ssh/config files

        does not support password authentication or password
        protected authentication keys"""
    def __init__(self, url, **kw):
        if self.path.find('@') != -1:
            user, self.path = self.path.split('@')
        else:
            user = None

        if self.path.find(':') != -1:
            host, self.path = selfpath.split(':')
        else:
            host, self.path = self.path.split('/', 1)

        self.config = SSHHostConfig(host, user)


        host_keys = paramiko.util.load_host_keys(os.path.expanduser('~/.ssh/known_hosts'))
        self.hostkey = host_keys[config['hostkeyalias']].values()[0]

        if(config.has_key('identityfile')):
            key_file = os.path.expanduser(host_config['identityfile'])
            #not really nice but i don't see a cleaner way atm...
            try:
                self.auth_key = RSAKey (filename = key_file)
            except SSHException, e:
                if e.message == 'Unable to parse file':
                    self.auth_key = DSAKey (filename = key_file)
                else:
                    raise
        else:
            filename = os.path.expanduser('~/.ssh/id_rsa')
            if os.path.exists(filename):
                self.auth_key = RSAKey(filename = filename)
            else:
                filename = os.path.expanduser('~/.ssh/id_dsa')
                if (os.path.exists(filename)):
                    self.auth_key = DSSKey (filename = filename)

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
        return self.client.open(filename = self.__build_fn(name), mode = 'rb')

    def put(self, type, name, fp):
        remote_file = self.client.open(filename = self.__build_fn(name), mode = 'wb')
        buf = fp.read(4096)
        while (len(buf) > 0):
            remote_file.write(buf)
            buf = fp.read(4096)
        remote_file.close()

    def delete(self, type, name):
        self.client.remove(filename = self.__build_fn(name))

    def stat(self, type, name):
        stat = self.client.stat(filename = self.__build_fn(name))
        return {'size': stat.st_size}

    def close(self):
        """connection has to be explicitly closed, otherwise
            it will hold the process running idefinitly"""
        self.client.close()
        self.t.close()

Store = SFTPStore
