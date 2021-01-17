# -*- coding: utf-8 -*-
from __future__ import absolute_import
import os.path as osp
import logging, subprocess, os, glob, shutil, time, datetime
from contextlib import contextmanager

DEFAULT_LOGGING_LEVEL = logging.WARNING

def setup_logger(name='seutils'):
    if name in logging.Logger.manager.loggerDict:
        logger = logging.getLogger(name)
        logger.info('Logger %s is already defined', name)
    else:
        fmt = logging.Formatter(
            fmt = (
                '\033[33m%(levelname)7s:%(asctime)s:%(module)s:%(lineno)s\033[0m'
                + ' %(message)s'
                ),
            datefmt='%Y-%m-%d %H:%M:%S'
            )
        handler = logging.StreamHandler()
        handler.setFormatter(fmt)
        logger = logging.getLogger(name)
        logger.setLevel(DEFAULT_LOGGING_LEVEL)
        logger.addHandler(handler)
    return logger
logger = setup_logger()

def debug(flag=True):
    """Sets the logger level to debug (for True) or warning (for False)"""
    logger.setLevel(logging.DEBUG if flag else DEFAULT_LOGGING_LEVEL)

DRYMODE = False
def drymode(flag=True):
    global DRYMODE
    DRYMODE = flag

@contextmanager
def drymode_context(flag=True):
    global DRYMODE
    _saved_DRYMODE = DRYMODE
    DRYMODE = flag
    try:
        yield DRYMODE
    finally:
        DRYMODE = _saved_DRYMODE

def is_string(string):
    """
    Checks strictly whether `string` is a string
    Python 2/3 compatibility (https://stackoverflow.com/a/22679982/9209944)
    """
    try:
        basestring
    except NameError:
        basestring = str
    return isinstance(string, basestring)

N_SECONDS_SLEEP = 10

def run_command(cmd, env=None, dry=None, nonzero_exitcode_ok=False, n_retries=0, return_output_on_nonzero_exitcode=False):
    """
    Runs a command and captures output. Raises an exception on non-zero exit code,
    except if nonzero_exitcode_ok is set to True.
    """
    i_attempt = 0
    if dry is None: dry = DRYMODE
    if env is None: env = ENV
    while True:
        logger.info('%sIssuing command (attempt %s: %s)', '(dry) ' if dry else '', i_attempt, ' '.join(cmd))
        if dry: return ''
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            universal_newlines=True,
            )
        # Start running command and capturing output
        output = []
        for stdout_line in iter(process.stdout.readline, ''):
            logger.debug('CMD: ' + stdout_line.strip('\n'))
            output.append(stdout_line)
        process.stdout.close()
        process.wait()
        returncode = process.returncode
        # Return output only if command succeeded
        if returncode == 0:
            logger.info('Command exited with status 0 - all good')
        else:
            if nonzero_exitcode_ok:
                logger.info('Command exited with status %s', returncode)
                return output if return_output_on_nonzero_exitcode else returncode
            else:
                logger.error('Exit status {0} for command: {1}'.format(returncode, cmd))
                logger.error('Output:\n%s', '\n'.join(output))
                if i_attempt < n_retries:
                    i_attempt += 1
                    logger.error('Retrying attempt %s/%s in %s seconds...', i_attempt, n_retries, N_SECONDS_SLEEP)
                    time.sleep(N_SECONDS_SLEEP)
                    continue
                else:
                    raise subprocess.CalledProcessError(cmd, returncode)
        return output

def get_exitcode(cmd, env=None):
    """
    Runs a command and returns the exit code.
    """
    if env is None: env = ENV
    if is_string(cmd): cmd = [cmd]
    logger.debug('Getting exit code for "%s"', ' '.join(cmd))
    if DRYMODE:
        returncode = 0
    else:
        FNULL = open(os.devnull, 'w')
        process = subprocess.Popen(cmd, stdout=FNULL, stderr=subprocess.STDOUT, env=env)
        process.communicate()[0]
        returncode = process.returncode
    logger.debug('Got exit code %s', returncode)
    return returncode

def bytes_to_human_readable(num, suffix='B'):
    """
    Convert number of bytes to a human readable string
    """
    for unit in ['','k','M','G','T','P','E','Z']:
        if abs(num) < 1024.0:
            return '{0:3.1f} {1}b'.format(num, unit)
        num /= 1024.0
    return '{0:3.1f} {1}b'.format(num, 'Y')

def is_macos():
    """
    Checks if the platform is Mac OS
    """
    return os.uname()[0] == 'Darwin'

# _______________________________________________________
# Path management

DEFAULT_MGM = None
MGM_ENV_KEY = 'SEU_DEFAULT_MGM'

def set_default_mgm(mgm):
    """
    Sets the default mgm
    """
    global DEFAULT_MGM
    DEFAULT_MGM = mgm
    logger.info('Default mgm set to %s', mgm)

def read_default_mgm_from_env():
    if MGM_ENV_KEY in os.environ: set_default_mgm(os.environ[MGM_ENV_KEY])
# Set the default once at import time
read_default_mgm_from_env()

def get_default_mgm():
    if DEFAULT_MGM is None:
        raise RuntimeError(
            'A request relied on the default mgm to be set. '
            'Either use `seutils.set_default_mgm` or '
            'pass use the full path (starting with "root:") '
            'in your request.'
            )
    return DEFAULT_MGM

PROTOCOLS = [ 'root', 'srm', 'gsiftp', 'dcap' ] # Probably to be expanded

def has_protocol(filename):
    """
    Checks whether the filename contains a protocol.
    Currently a very basic string check, which so far has been enough
    """
    return ('://' in filename)

def is_ssh(path):
    return (':/' in path and not('://' in path))

def split_protocol_pfn(filename):
    """
    Splits protocol, server and logical file name from a physical file name.
    Throws an exception of format-ensuring checks fail.
    """
    if not has_protocol(filename):
        raise ValueError(
            'Attempted to get protocol from {0}, but there'
            ' does not seem to be any.'
            .format(filename)
            )
    protocol, rest = filename.split('://',1)
    if not '//' in rest:
        raise ValueError(
            'Could not determine server and logical file name from {0}'
            .format(filename)
            )
    server, lfn = rest.split('//',1)
    lfn = '/' + lfn # Restore the opening slash that was dropped in the split
    return protocol, server, lfn

def _split_mgm_pfn(filename):
    """
    Splits mgm and logical file name from a physical file name.
    Throws an exception of format-ensuring checks fail.
    """
    protocol, server, lfn = split_protocol_pfn(filename)
    return protocol + '://' + server, lfn

def _join_protocol_server_lfn(protocol, server, lfn):
    """
    Joins protocol, server and lfn into a physical filename.
    Ensures formatting to some extent.
    """
    protocol = protocol.replace(':', '') # Remove any ':' from the protocol
    server = server.strip('/') # Strip trailing or opening slashes
    if not lfn.startswith('/'):
        raise ValueError(
            'Logical file name {0} does not seem to be formatted correctly'
            .format(lfn)
            )
    return protocol + '://' + server + '/' + lfn

def split_mgm(path, mgm=None):
    """
    Returns the mgm and lfn that the user most likely intended to
    if path has a protocol string (e.g. 'root://...'), the mgm is taken from the path
    if mgm is passed, it is used as is
    if mgm is passed AND the path starts with 'root://' AND the mgm's don't agree,
      an exception is thrown
    if mgm is None and path has no mgm, the default variable DEFAULT_MGM is taken
    """
    if has_protocol(path):
        mgm_from_path, lfn = _split_mgm_pfn(path)
        if not(mgm is None) and not mgm_from_path == mgm:
            raise ValueError(
                'Conflicting mgms determined from path and passed argument: '
                'From path {0}: {1}, from argument: {2}'
                .format(path, mgm_from_path, mgm)
                )
        mgm = mgm_from_path
    elif mgm is None:
        mgm = get_default_mgm()
        lfn = path
    else:
        lfn = path
    # Sanity check
    if not lfn.startswith('/'):
        raise ValueError(
            'LFN {0} does not start with \'/\'; something is wrong'
            .format(lfn)
            )
    return mgm, lfn

def _join_mgm_lfn(mgm, lfn):
    """
    Joins mgm and lfn, ensures correct formatting.
    Will throw an exception of the lfn does not start with '/'
    """
    if not lfn.startswith('/'):
        raise ValueError(
            'This function expects filenames that start with \'/\''
            )
    if not mgm.endswith('/'): mgm += '/'
    # logger.error('mgm=%s lfn=%s', mgm, lfn)
    return mgm + lfn

def format(path, mgm=None):
    """
    Formats a path to ensure it is a path on the SE.
    Can take:
    - Just path starting with 'root:' - nothing really happens
    - Just path starting with '/' - the default mgm is used
    - Path starting with 'root:' and an mgm - an exception is thrown in case of conflict
    - Path starting with '/' and an mgm - mgm and path are joined
    """
    if is_ssh(path): return path
    mgm, lfn = split_mgm(path, mgm=mgm)
    return _join_mgm_lfn(mgm, lfn)

def get_protocol(path, mgm=None):
    """
    Returns the protocol contained in the path string
    """
    path = format(path, mgm)
    return path.split('://')[0]

def cmd_exists(executable):
    """
    Checks if a command can be found on the system path.
    Not a very smart implementation but does the job usually.
    See https://stackoverflow.com/a/28909933/9209944 .
    """
    return any(os.access(os.path.join(path, executable), os.X_OK) for path in os.environ["PATH"].split(os.pathsep))

class Inode(object):
    """
    Basic container of information representing an inode on a
    storage element: isdir/isfile, modification time, size, and path.
    """
    @classmethod
    def from_path(cls, path, mgm=None):
        path = format(path, mgm)
        return stat(path)

    def __init__(self, path, modtime, isdir, size):
        self.path = path
        self.modtime = modtime
        self.isdir = isdir
        self.size = size
        # Some derived properties
        self.isfile = not(self.isdir)
        self.size_human = bytes_to_human_readable(float(self.size))
        self.basename = osp.basename(path)

    def __repr__(self):
        if len(self.path) > 40:
            shortpath = self.path[:10] + '...' + self.path[-15:]
        else:
            shortpath = self.path
        return super(Inode, self).__repr__().replace('object', shortpath)

# _______________________________________________________
# Cache

USE_CACHE = False
CACHEDIR = osp.abspath('.seutils-cache')
CACHES = {}

def use_cache(flag=True):
    """
    Convenience function to turn on and off caching
    """
    global USE_CACHE
    USE_CACHE = flag

def make_cache(subcache_name, make_if_not_exist=True):
    """
    Returns a FileCache object. Will be created if it doesn't exist already
    """
    if not USE_CACHE: return
    global CACHES
    if not subcache_name in CACHES:
        from .cache import FileCache
        cache = FileCache(subcache_name, app_cache_dir=CACHEDIR)
        CACHES[subcache_name] = cache
    return CACHES[subcache_name]

def read_cache(subcache_name, key):
    """
    Attempts to get a value from a cache. Returns None if it was not found
    """
    if not USE_CACHE: return None
    val = make_cache(subcache_name).get(key, None)
    if not(val is None): logger.debug('Using cached result for %s from cache %s', key, subcache_name)
    return val

_LAST_CACHE_WRITE = None
def write_cache(subcache_name, key, value):
    """
    Writes a value to a cache
    """
    if USE_CACHE:
        logger.debug('Writing key %s to cache %s', key, subcache_name)
        subcache = make_cache(subcache_name)
        subcache[key] = value
        subcache.sync()
        global _LAST_CACHE_WRITE
        _LAST_CACHE_WRITE = datetime.datetime.now()

def cache(fn):
    """
    Function decorator to cache output of certain commands
    """
    cache_name = 'seutils-cache.' + fn.__name__
    def wrapper(path, *args, **kwargs):
        # Cache is basically a key-value dump; determine the key to use
        # For most purposes, just the path works well enough
        cache_key = fn.keygen(path, *args, **kwargs) if hasattr(fn, 'keygen') else path.strip()
        # Try to read the cache; if not possible, evaluate cmd and store the result
        val = read_cache(cache_name, cache_key)
        if val is None:
            val = fn(path, *args, **kwargs)
            write_cache(cache_name, cache_key, val)
        return val
    return wrapper

_LAST_TARBALL_CACHE = None
_LAST_TARBALL_PATH = None
def tarball_cache(dst='seutils-cache.tar.gz', only_if_updated=False):
    """
    Dumps the cache to a tarball.
    If only_if_updated is True, an additional check is made to see whether
    the last call to tarball_cache() was made after the last call to write_cache();
    if so, the last created tarball presumably still reflects the current state of
    the cache, and no new tarball is created. This will only work within the same python
    session (timestamps are not saved to files).
    """
    global _LAST_TARBALL_CACHE, _LAST_TARBALL_PATH
    if not USE_CACHE: raise Exception('No active cache to save to a file')
    if not dst.endswith('.tar.gz'): dst += '.tar.gz'
    dst = osp.abspath(dst)
    if only_if_updated:
        if _LAST_TARBALL_CACHE:
            if _LAST_CACHE_WRITE is None or _LAST_CACHE_WRITE < _LAST_TARBALL_CACHE:
                # Either no write has taken place or it was before the last tarball creation;
                # use the last created tarball and don't run again
                logger.info('Detected no change w.r.t. last tarball %s; using it instead', _LAST_TARBALL_PATH)
                return _LAST_TARBALL_PATH
    try:
        _return_dir = os.getcwd()
        if not osp.isdir(CACHEDIR): os.makedirs(CACHEDIR) # Empty dir can be tarballed too for consistency
        os.chdir(CACHEDIR)
        cmd = ['tar', '-zcvf', dst, '.']
        logger.info('Dumping %s --> %s', CACHEDIR, dst)
        run_command(cmd)
        _LAST_TARBALL_CACHE = datetime.datetime.now()
        _LAST_TARBALL_PATH = dst
        return dst
    finally:
        os.chdir(_return_dir)
    return dst

def load_tarball_cache(tarball, dst=None):
    """
    Extracts a cache tarball to cachedir and activates that cache
    """
    global USE_CACHE, CACHEDIR
    if dst is None: dst = CACHEDIR
    dst = osp.abspath(dst)
    logger.info('Extracting %s --> %s', tarball, dst)
    if not osp.isdir(dst): os.makedirs(dst)
    cmd = [
        'tar', '-xvf', tarball,
        '-C', dst
        ]
    run_command(cmd)
    # Activate it
    USE_CACHE = True
    CACHEDIR = dst
    logger.info('Activated cache for path %s', CACHEDIR)

# _______________________________________________________
# Helpers for interactions with SE

N_COPY_RETRIES = 0

# Import implementations
from . import xrd
from . import pyxrd
from . import gfal
from . import eos
from . import ssh
_implementations = [ xrd, pyxrd, gfal, eos, ssh ]

def get_implementation(name):
    """
    Takes the name of a submodule that contains implementations. Returns that module object.
    """
    for implementation in _implementations:
        if implementation.__name__.rsplit('.',1)[-1] == name:
            break
    else:
        raise Exception('No such implementation: {0}'.format(name))
    return implementation

_commands = [
    'mkdir', 'rm', 'stat', 'exists', 'isdir',
    'isfile', 'is_file_or_dir', 'listdir', 'cp',
    ]

def get_command(cmd_name, implementation=None, path=None):
    """
    Returns an implementation of a command.
    Parameter `implementation` is a module object; it can also be a name of a module.
    If it's None, a heuristic is ran to guess the best implementation.
    The `path` parameter is optional and only serves to make a better guess for the implementation.
    """
    if not cmd_name in _commands:
        raise Exception('Invalid command: {0}'.format(cmd_name))
    if implementation is None:
        implementation = best_implementation_heuristic(cmd_name, path)[0]
    elif is_string(implementation):
        implementation = get_implementation(implementation)
    if not hasattr(implementation, cmd_name):
        raise Exception(
            'Implementation {0} has no function {1}'
            .format(implementation.__name__, cmd_name)
            )
    return getattr(implementation, cmd_name)

def best_implementation_heuristic(cmd_name, path=None):
    """
    Determines the best implementation for a path.
    The order is loosely as follows:
    - If the python bindings of xrootd are importable, those are probably the most reliable
    - If the path does not start with root://, it's safer to use gfal
    - For root://-starting paths, use xrootd
    """
    # Shortcut for ssh
    if path and is_ssh(path):
        logger.debug('Path is ssh-like')
        return [ssh]
    # Grid protocols
    preferred_order = []
    def check(module):
        if module.is_installed() and hasattr(module, cmd_name) and not(module in preferred_order):
            preferred_order.append(module)
    check(pyxrd)
    # If this concerns removal, eos is the only tool that can do recursive directory removal:
    if cmd_name == 'rm': check(eos)
    # If path starts with root://, prefer xrd over gfal, otherwise the other way around
    if not(path is None) and get_protocol(path) != 'root':
        check(gfal)
        check(xrd)
    else:
        check(xrd)
        check(gfal)
    # Prefer eos last
    check(eos)
    # Check if at least one method is found
    if not len(preferred_order):
        raise Exception(
            'No good implementation could be found for cmd {0} (path {1})'
            .format(cmd_name, path)
            )
    logger.info('Using module %s to execute \'%s\' (path: %s)', preferred_order[0].__name__, cmd_name, path)
    return preferred_order

ENV = None
def set_env(env):
    """
    Sets the env in which command line arguments are ran by default
    """
    global ENV
    ENV = env

@contextmanager
def env_context(env):
    """
    Temporarily sets an environment, and then reverts to the old environment
    """
    global ENV
    old_ENV = ENV
    ENV = env
    try:
        yield None
    finally:
        ENV = old_ENV

def add_env_kwarg(fn):
    """
    Function decorator that gives the function the `env` keyword argument
    """
    def wrapper(*args, **kwargs):
        if 'env' in kwargs:
            with env_context(kwargs.pop('env')):
                return fn(*args, **kwargs)
        else:
            return fn(*args, **kwargs)
    return wrapper

# _______________________________________________________
# Actual interactions with SE
# The functions below are just wrappers for the actual implementations in
# separate modules. All functions have an `implementation` keyword; If set
# to None, the 'best' implementation is guessed.

@add_env_kwarg
def mkdir(path, implementation=None):
    """
    Creates a directory on the SE
    Does not check if directory already exists
    """
    path = format(path) # Ensures format
    logger.warning('Creating directory on SE: {0}'.format(path))
    cmd = get_command('mkdir', implementation=implementation, path=path)
    cmd(path)

@add_env_kwarg
def rm(path, recursive=False, implementation=None):
    """
    Creates a path on the SE
    Does not check if path already exists
    """
    path = format(path) # Ensures format
    logger.warning('Removing path on SE: {0}'.format(path))
    cmd = get_command('rm', implementation=implementation, path=path)
    cmd(path, recursive=recursive)

@add_env_kwarg
@cache
def stat(path, not_exist_ok=False, implementation=None):
    """
    Returns an Inode object for path.
    If not_exist_ok is True and the path doesn't exist, it returns None
    without raising an exception
    """
    path = format(path) # Ensures format
    cmd = get_command('stat', implementation=implementation, path=path)
    return cmd(path, not_exist_ok=not_exist_ok)

@add_env_kwarg
def stat_function(*args, **kwargs):
    """
    Alternative name for the stat function, since stat is also an often used keyword in functions
    """
    return stat(*args, **kwargs)

@add_env_kwarg
@cache
def exists(path, implementation=None):
    """
    Returns a boolean indicating whether the path exists.
    """
    return get_command('exists', implementation=implementation, path=path)(path)

@add_env_kwarg
@cache
def isdir(path, implementation=None):
    """
    Returns a boolean indicating whether the directory exists.
    Also returns False if the passed path is a file.
    """
    return get_command('isdir', implementation=implementation, path=path)(path)

@add_env_kwarg
@cache
def isfile(path, implementation=None):
    """
    Returns a boolean indicating whether the file exists.
    Also returns False if the passed path is a directory.
    """
    return get_command('isfile', implementation=implementation, path=path)(path)

@add_env_kwarg
@cache
def is_file_or_dir(path, implementation=None):
    """
    Returns 0 if path does not exist
    Returns 1 if it's a directory
    Returns 2 if it's a file
    """
    return get_command('is_file_or_dir', implementation=implementation, path=path)(path)

@add_env_kwarg
def listdir(path, stat=False, assume_directory=False, implementation=None):
    """
    Returns the contents of a directory
    If 'assume_directory' is True, it is assumed the user took
    care to pass a path to a valid directory, and no check is performed
    """
    if not(assume_directory) and not isdir(path):
        raise Exception('{0} is not a directory'.format(path))
    cmd = get_command('listdir', implementation=implementation, path=path)
    return cmd(path, stat)

# Custom cache key generator dependent on whether stat was True or False
def _listdir_keygen(path, stat=False, *args, **kwargs):
    return path.strip() + '___stat{}'.format(stat)
listdir.keygen = _listdir_keygen
listdir = cache(listdir)

@add_env_kwarg
def cp(src, dst, implementation=None, **kwargs):
    """
    Copies a file `src` to the storage element.
    Does not format `src` or `dst`; user is responsible for formatting.

    The method can be 'auto', 'xrdcp', or 'gfal-copy'. If 'auto', a heuristic
    will be applied to determine whether to best use xrdcp or gfal-copy.
    """
    logger.warning('Copying %s --> %s', src, dst)    
    cmd = get_command('cp', implementation=implementation, path=dst if has_protocol(dst) else src)
    cmd(src, dst, **kwargs)

# _______________________________________________________
# Algorithms that use the SE interaction implementation, but are agnostic to the underlying tool

def cp_to_se(src, dst, **kwargs):
    """
    Like cp, but assumes dst is a location on a storage element and src is local
    """
    cp(src, format(dst), **kwargs)

def cp_from_se(src, dst, **kwargs):
    """
    Like cp, but assumes src is a location on a storage element and dst is local
    """
    cp(format(src), dst, **kwargs)

MAX_RECURSION_DEPTH = 20

def ls(path, stat=False, assume_directory=False, no_expand_directory=False):
    """
    Lists all files and directories in a directory on the SE.
    It first checks whether the path exists and is a file or a directory.
    If it does not exist, it raises an exception.
    If it is a file, it just returns a formatted path to the file as a 1-element list
    If it is a directory, it returns a list of the directory contents (formatted)

    If stat is True, it returns Inode objects which contain more information beyond just the path

    If assume_directory is True, the first check is not performed and the algorithm assumes
    the user took care to pass a path to a directory. This saves a request to the SE, which might
    matter in the walk algorithm. For singular use, assume_directory should be set to False.

    If no_expand_directory is True, the contents of the directory are not listed, and instead
    a formatted path to the directory is returned (similar to unix's ls -d)
    """
    path = format(path)
    if assume_directory:
        status = 1
    else:
        status = is_file_or_dir(path)
    # Depending on status, return formatted path to file, directory contents, or raise
    if status == 0:
        raise RuntimeError('Path \'{0}\' does not exist'.format(path))
    elif status == 1:
        # It's a directory
        if no_expand_directory:
            # If not expanding, just return a formatted path to the directory
            return [stat_function(path) if stat else path]
        else:
            # List the contents of the directory
            return listdir(path, assume_directory=True, stat=stat) # No need to re-check whether it's a directory
    elif status == 2:
        # It's a file; just return the path to the file
        return [stat_function(path) if stat else path]

class Counter:
    """
    Class to basically mimic a pointer to an int
    This is very clumsy in python
    """
    def __init__(self):
        self.i = 0
    def plus_one(self):
        self.i += 1

def walk(path, stat=False):
    """
    Entry point for walk algorithm.
    Performs a check whether the starting path is a directory,
    then yields _walk.
    A counter object is passed to count the number of requests
    made to the storage element, so that 'accidents' are limited
    """
    path = format(path)
    status = is_file_or_dir(path)
    if not status == 1:
        raise RuntimeError(
            '{0} is not a directory'
            .format(path)
            )
    counter = Counter()
    for i in _walk(path, stat, counter):
        yield i

def _walk(path, stat, counter):
    """
    Recursively calls ls on traversed directories.
    The yielded directories list can be modified in place
    as in os.walk.
    """
    if counter.i >= MAX_RECURSION_DEPTH:
        raise RuntimeError(
            'walk reached the maximum recursion depth of {0} requests.'
            ' If you are very sure that you really need this many requests,'
            ' set seutils.MAX_RECURSION_DEPTH to a larger number.'
            .format(MAX_RECURSION_DEPTH)
            )
    contents = ls(path, stat=True, assume_directory=True)
    counter.plus_one()
    files = [ c for c in contents if c.isfile ]
    files.sort(key=lambda f: f.basename)
    directories = [ c for c in contents if c.isdir ]
    directories.sort(key=lambda d: d.basename)
    if stat:
        yield path, directories, files
    else:
        dirnames = [ d.path for d in directories ]
        yield path, dirnames, [ f.path for f in files ]
        # Filter directories again based on dirnames, in case the user modified
        # dirnames after yield
        directories = [ d for d in directories if d.path in dirnames ]
    for directory in directories:
        for i in _walk(directory.path, stat, counter):
            yield i

def ls_root(paths):
    """
    Flexible function that attempts to return a list of root files based on what
    the user most likely wanted to query.
    Takes a list of paths as input. If input as a string, it will be turned into a len-1 list.
    Firstly it is checked whether the path exists locally.
      If it's a root file, it's appended to the output,
      If it's a directory, it will be globbed for *.root.
    Secondly it's attempted to reach the path remotely.
    Returns a list of .root files.
    """
    if is_string(paths): paths = [paths]
    root_files = []
    for path in paths:
        if osp.exists(path):
            # Treat as a local path
            if osp.isfile(path):
                if path.endswith('.root'):
                    root_files.append(path)
            elif osp.isdir(path):
                root_files.extend(glob.glob(osp.join(path, '*.root')))
        else:
            # Treat path as a SE path
            try:
                stat = is_file_or_dir(path)
                if stat == 1:
                    # It's a directory
                    root_files.extend([ f for f in ls(path) if f.endswith('.root') ])
                elif stat == 2:
                    # It's a file
                    root_files.append(format(path))
                elif stat == 0:
                    logger.warning('Path %s does not exist locally or remotely', path)
            except RuntimeError:
                logger.warning(
                    'Path %s does not exist locally and could not be treated as a remote path',
                    path
                    )
    root_files.sort()
    return root_files

def ls_wildcard(pattern, stat=False):
    """
    Like ls, but accepts wildcards * .
    Directories are *not* expanded.

    The algorithm is like `walk`, but discards directories that don't fit the pattern
    early.
    Still the number of requests can grow quickly; a limited number of wildcards is advised.
    """
    pattern = format(pattern)
    if not '*' in pattern:
        return ls(pattern, stat=stat, no_expand_directory=True)
    import re
    if not stat and not '*' in pattern.rsplit('/',1)[0]:
        # If there is no star in any part but the last one and we don't need to stat, it is
        # much faster to do a simple listing once and do regex matching here.
        # This only saves time for the specific case of 'no need for stat' and 'pattern
        # only for the very last part'
        logger.info('Detected * only in very last part of pattern and stat=False; using shortcut')
        directory, pattern = pattern.rsplit('/',1)
        contents = ls(directory)
        if pattern == '*':
            # Skip the regex matching if set to 'match all'
            return contents
        regex = re.compile(pattern.replace('*', '.*'))
        contents = [ c for c in contents if regex.match(osp.basename(c)) ]
        return contents
    # 
    pattern_level = pattern.count('/')
    logger.debug('Level is %s for path %s', pattern_level, pattern)
    # Get the base pattern before any wild cards
    base = pattern.split('*',1)[0].rsplit('/',1)[0]
    logger.debug('Found base pattern %s from pattern %s', base, pattern)
    matches = []
    for path, directories, files in walk(base, stat=stat):
        level = path.count('/')
        logger.debug('Level is %s for path %s', level, path)
        trimmed_pattern = '/'.join(pattern.split('/')[:level+2]).replace('*', '.*')
        logger.debug('Comparing directories in %s with pattern %s', path, trimmed_pattern)
        regex = re.compile(trimmed_pattern)
        if stat:
            directories[:] = [ d for d in directories if regex.match(d.path) ]
        else:
            directories[:] = [ d for d in directories if regex.match(d) ]
        if level+1 == pattern_level:
            # Reached the depth of the pattern - save matches
            matches.extend(directories[:])
            if stat:
                matches.extend([f for f in files if regex.match(f.path)])
            else:
                matches.extend([f for f in files if regex.match(f)])
            # Stop iterating in this part of the tree
            directories[:] = []
    return matches

# _______________________________________________________
# Command line helpers

def cli_detect_fnal():
    if DEFAULT_MGM is None and os.uname()[1].endswith('.fnal.gov'):
        mgm = 'root://cmseos.fnal.gov'
        logger.warning('Detected fnal.gov host; using mgm %s as default if necessary', mgm)
        set_default_mgm(mgm)

def cli_flexible_format(lfn, mgm=None):
    if is_ssh(lfn): return lfn
    cli_detect_fnal()
    if not has_protocol(lfn) and not lfn.startswith('/'):
        try:
            prefix = '/store/user/' + os.environ['USER']
            logger.warning('Pre-fixing %s', prefix)
            lfn = os.path.join(prefix, lfn)
        except KeyError:
            pass
    if has_protocol(lfn):
        return format(lfn)
    else:
        return format(lfn, mgm)

# _______________________________________________________
# root utils extension

from . import rootutils as root
from . import pyxrd
