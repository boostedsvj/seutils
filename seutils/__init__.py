# -*- coding: utf-8 -*-
from __future__ import absolute_import
import os.path as osp
import logging, subprocess, os, glob, shutil

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

def executable_exists(executable):
    """
    Takes a string that is the executable, and returns True if the executable
    is one the path
    """
    import distutils.spawn
    return not(distutils.spawn.find_executable(executable) is None)

def run_command(cmd, dry=False, non_zero_exitcode_ok=False):
    """
    Runs a command and captures output. Raises an exception on non-zero exit code,
    except if non_zero_exitcode_ok is set to True.
    """
    logger.info('Issuing command: {0}'.format(' '.join(cmd)))
    if dry: return
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
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
        if non_zero_exitcode_ok:
            logger.info('Command exited with status %s', return_code)
            return returncode
        else:
            logger.error('Exit status {0} for command: {1}'.format(returncode, cmd))
            logger.error('Output:\n%s', '\n'.join(output))
            raise subprocess.CalledProcessError(cmd, returncode)
    return output

def get_exitcode(cmd):
    """
    Runs a command and returns the exit code.
    """
    if is_string(cmd): cmd = [cmd]
    logger.debug('Getting exit code for "%s"', ' '.join(cmd))
    FNULL = open(os.devnull, 'w')
    process = subprocess.Popen(cmd, stdout=FNULL, stderr=subprocess.STDOUT)
    process.communicate()[0]
    logger.debug('Got exit code %s', process.returncode)
    return process.returncode

def bytes_to_human_readable(num, suffix='B'):
    """
    Convert number of bytes to a human readable string
    """
    for unit in ['','k','M','G','T','P','E','Z']:
        if abs(num) < 1024.0:
            return '{0:3.1f} {1}b'.format(num, unit)
        num /= 1024.0
    return '{0:3.1f} {1}b'.format(num, 'Y')

# _______________________________________________________
# Path management

DEFAULT_MGM = None

def set_default_mgm(mgm):
    """
    Sets the default mgm
    """
    DEFAULT_MGM = mgm
    logger.info('Default mgm set to %s', mgm)

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
    mgm, lfn = split_mgm(path, mgm=mgm)
    return _join_mgm_lfn(mgm, lfn)

def get_protocol(path, mgm=None):
    """
    Returns the protocol contained in the path string
    """
    path = format(path, mgm)
    return path.split('://')[0]

def use_xrootd(protocol):
    """
    Based on the protocol, returns True if xrootd tools should be used, or gfal tools
    """
    # Not sure if this simple check will be enough in the futute
    return (protocol == 'root')

def use_xrootd_path(path):
    """
    Determines if xrootd should be used based on the passed path
    """
    return use_xrootd(get_protocol(path))

# _______________________________________________________
# Interactions with SE

class Inode(object):
    """
    Basic container of information representing an inode on a
    storage element: isdir/isfile, modification time, size, and path
    """
    @classmethod
    def from_path(cls, path, mgm=None):
        path = format(path, mgm)
        return stat(path)

    @classmethod
    def from_statline_gfal(cls, statline, directory):
        """
        `gfal-ls -l` returns only basenames, so the directory from which the
        statline originated is needed as an argument.
        """
        import datetime
        components = statline.strip().split()
        if not len(components) >= 9:
            raise RuntimeError(
                'Expected at least 9 components for stat line:\n{0}'
                .format(statline)
                )
        try:
            isdir = components[0].startswith('d')
            timestamp = ' '.join(components[5:8])
            modtime = datetime.datetime.strptime(timestamp, '%b %d %H:%M')
            size = int(components[4])
            path = osp.join(directory, components[8])
            return cls(path, modtime, isdir, size)
        except:
            logger.error('Error parsing statline: %s', statline)
            raise

    @classmethod
    def from_statline_xrootd(cls, statline, mgm):
        import datetime
        components = statline.strip().split()
        if not len(components) == 5:
            raise RuntimeError(
                'Expected 5 components for stat line:\n{0}'
                .format(statline)
                )
        isdir = components[0].startswith('d')
        modtime = datetime.datetime.strptime(components[1] + ' ' + components[2], '%Y-%m-%d %H:%M:%S')
        size = int(components[3])
        path = format(components[4], mgm)
        return cls(path, modtime, isdir, size)

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

def mkdir(directory):
    """
    Creates a directory on the SE
    Does not check if directory already exists
    """
    directory = format(directory) # Ensures format
    logger.warning('Creating directory on SE: {0}'.format(directory))
    _mkdir_xrootd(directory) if use_xrootd_path(directory) else _mkdir_gfal(directory)

def _mkdir_gfal(directory):
    run_command([ 'gfal-mkdir', '-p', directory ])

def _mkdir_xrootd(directory):
    mgm, directory = split_mgm(directory)
    run_command([ 'xrdfs', mgm, 'mkdir', '-p', directory ])

def stat(path, not_exist_ok=False):
    """
    Returns an Inode object for path.
    If not_exist_ok is True and the path doesn't exist, it returns None
    without raising an exception
    """
    return _stat_xrootd(path, not_exist_ok) if use_xrootd_path(path) else _stat_gfal(path, not_exist_ok)

def stat_function(*args, **kwargs):
    """
    Alternative name for the stat function, since stat is also an often used keyword in functions
    """
    return stat(*args, **kwargs)

def _stat_gfal(path, not_exist_ok=False):
    import datetime
    output = run_command(['gfal-stat', path], non_zero_exitcode_ok=not_exist_ok)
    if isinstance(output, int):
        # The command failed; if output is 2 the path did not exist,
        # which might be okay if not_exist_ok is True, but other codes
        # should raise an exception
        if not_exist_ok and output == 2:
            logger.info('Stat %s: no such file', path)
            return None
        else:
            raise RuntimeError(
                'cmd {0} returned exit code {1}'
                .format(' '.join(cmd), output)
                )
    # Interpret the output to create an Inode object
    size = None
    modtime = None
    isdir = None
    for line in output:
        line = line.strip()
        if len(line) == 0:
            continue
        elif line.startswith('Size:'):
            isdir = ('directory' in line)
            size = int(line.replace('Size:','').strip().split()[0])
        elif line.startswith('Modify:'):
            timestamp = line.replace('Modify:','').strip()
            # Strip off microseconds if they're there
            if '.' in timestamp: timestamp = timestamp.split('.')[0]
            modtime = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    if size is None: raise RuntimeError('Could not extract size from stat:\n{0}'.format(output))
    if modtime is None: raise RuntimeError('Could not extract modtime from stat:\n{0}'.format(output))
    if isdir is None: raise RuntimeError('Could not extract isdir from stat:\n{0}'.format(output))
    return Inode(path, modtime, isdir, size)

def _stat_xrootd(path, not_exist_ok=False):
    import datetime
    mgm, path = split_mgm(path)
    cmd = [ 'xrdfs', mgm, 'stat', path ]
    output = run_command(cmd, non_zero_exitcode_ok=not_exist_ok)
    if isinstance(output, int):
        # The command failed; if output is 54 the path did not exist,
        # which might be okay if not_exist_ok is True, but other codes
        # should raise an exception
        if not_exist_ok and output == 54:
            logger.info('Stat %s: no such file', path)
            return None
        else:
            raise RuntimeError(
                'cmd {0} returned exit code {1}'
                .format(' '.join(cmd), output)
                )
    # Parse output to an Inode instance
    size = None
    modtime = None
    isdir = None
    for l in output:
        l = l.strip()
        if l.startswith('Size:'):
            size = int(l.split()[1])
        elif l.startswith('MTime:'):
            timestamp = l.replace('MTime:', '').strip()
            modtime = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        elif l.startswith('Flags:'):
            isdir = 'IsDir' in l
    if size is None: raise RuntimeError('Could not extract size from stat:\n{0}'.format(output))
    if modtime is None: raise RuntimeError('Could not extract modtime from stat:\n{0}'.format(output))
    if isdir is None: raise RuntimeError('Could not extract isdir from stat:\n{0}'.format(output))
    return Inode(path, modtime, isdir, size)

def exists(path):
    """
    Returns a boolean indicating whether the path exists.
    """
    return _exists_xrootd(path) if use_xrootd_path(path) else _exists_gfal(path)

def _exists_gfal(path):
    return get_exitcode(['gfal-stat', path]) == 0

def _exists_xrootd(path):
    mgm, path = split_mgm(path)
    cmd = [ 'xrdfs', mgm, 'stat', path ]
    return get_exitcode(cmd) == 0

def isdir(directory):
    """
    Returns a boolean indicating whether the directory exists.
    Also returns False if the passed path is a file.
    """
    return _isdir_xrootd(directory) if use_xrootd_path(directory) else _isdir_gfal(directory)

def _isdir_gfal(directory):
    statinfo = _stat_gfal(directory, not_exist_ok=True)
    if statinfo is None: return False
    return statinfo.isdir

def _isdir_xrootd(directory):
    mgm, directory = split_mgm(directory)
    cmd = [ 'xrdfs', mgm, 'stat', '-q', 'IsDir', directory ]
    return get_exitcode(cmd) == 0

def isfile(path):
    """
    Returns a boolean indicating whether the file exists.
    Also returns False if the passed path is a directory.
    """
    return _isfile_xrootd(path) if use_xrootd_path(path) else _isfile_gfal(path)

def _isfile_xrootd(path):
    mgm, path = split_mgm(path)
    status = get_exitcode([ 'xrdfs', mgm, 'stat', '-q', 'IsDir', path ])
    # Error code 55 means path exists, but is not a directory
    return (status == 55)

def _isfile_gfal(path):
    statinfo = _stat_gfal(path, not_exist_ok=True)
    if statinfo is None: return False
    return statinfo.isfile

def is_file_or_dir(path):
    """
    Returns 0 if path does not exist
    Returns 1 if it's a directory
    Returns 2 if it's a file
    """
    return _is_file_or_dir_xrootd(path) if use_xrootd_path(path) else _is_file_or_dir_gfal(path)

def _is_file_or_dir_gfal(path):
    statinfo = _stat_gfal(path, not_exist_ok=True)
    if statinfo is None:
        return 0
    elif statinfo.isdir:
        return 1
    elif statinfo.isfile:
        return 2

def _is_file_or_dir_xrootd(path):
    mgm, path = split_mgm(path)
    cmd = [ 'xrdfs', mgm, 'stat', '-q', 'IsDir', path ]
    status = get_exitcode(cmd)
    if status == 0:
        # Path is a directory
        return 1
    elif status == 54:
        # Path does not exist
        return 0
    elif status == 55:
        # Path is a file
        return 2
    else:
        raise RuntimeError(
            'Command {0} exitted with code {1}; unknown case'
            .format(' '.join(cmd), status)
            )

def listdir(directory, stat=False, assume_directory=False):
    """
    Returns the contents of a directory
    If 'assume_directory' is True, it is assumed the user took
    care to pass a path to a valid directory, and no check is performed
    """
    if not assume_directory:
        if not isdir(directory):
            raise RuntimeError(
                '{0} is not a valid directory'
                .format(directory)
                )
    return _listdir_xrootd(directory, stat) if use_xrootd_path(directory) else _listdir_gfal(directory, stat)

def _listdir_xrootd(directory, stat=False):
    mgm, path = split_mgm(directory)
    cmd = [ 'xrdfs', mgm, 'ls', path ]
    if stat: cmd.append('-l')
    output = run_command(cmd)
    contents = []
    for l in output:
        l = l.strip()
        if not len(l): continue
        if stat:
            contents.append(Inode.from_statline_xrootd(l, mgm))
        else:
            contents.append(format(l, mgm))
    return contents

def _listdir_gfal(directory, stat=False):
    cmd = [ 'gfal-ls', format(directory) ]
    if stat: cmd.append('-l')
    output = run_command(cmd)
    contents = []
    for l in output:
        l = l.strip()
        if not len(l): continue
        if stat:
            contents.append(Inode.from_statline_gfal(l, directory))
        else:
            contents.append(format(osp.join(directory, l)))
    return contents

def cp(src, dst, method='auto', **kwargs):
    """
    Copies a file `src` to the storage element.
    Does not format `src` or `dst`; user is responsible for formatting.

    The method can be 'auto', 'xrdcp', or 'gfal-copy'. If 'auto', a heuristic
    will be applied to determine whether to best use xrdcp or gfal-copy.
    """
    logger.warning('Copying %s --> %s', src, dst)
    methods = {
        'xrdcp' : _cp_xrdcp,
        'gfal-copy' : _cp_gfal,
        }
    # Heuristic to determine what copy method to use
    if method == 'auto':
        for file in [src, dst]:
            if has_protocol(file):
                if use_xrootd_path(file):
                    method = 'xrdcp'
                else:
                    method = 'gfal-copy'
                break
        else:
            logger.debug(
                'No protocols specified in either src ({0}) or dst ({1}); using xrdcp'
                .format(src, dst)
                )
            method = 'xrdcp'
    # Execute the copy method
    try:
        methods[method](src, dst, **kwargs)
    except KeyError:
        logger.error('Method %s is not a valid copying method!', method)
        raise

def _cp_xrdcp(src, dst, create_parent_directory=True, verbose=True):
    cmd = [ 'xrdcp', src, dst ]
    if not verbose: cmd.insert(1, '-s')
    if create_parent_directory: cmd.insert(1, '-p')
    run_command(cmd)

def _cp_gfal(src, dst, create_parent_directory=True, verbose=True):
    cmd = [ 'gfal-copy', '-t', '180', src, dst ]
    if create_parent_directory: cmd.insert(1, '-p')
    if verbose: cmd.insert(1, '-v')
    run_command(cmd)

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


# _______________________________________________________
# Algorithms that use the SE interactions

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
    protocol = get_protocol(path)
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

MGM_ENV_KEY = 'SEU_DEFAULT_MGM'

def cli_update_default_mgm(mgm):
    if MGM_ENV_KEY in os.environ:
        logger.warning(
            'Setting default mgm to %s (previously: %s)',
            mgm, os.environ[MGM_ENV_KEY]
            )
    else:
        logger.warning('Setting default mgm to %s', mgm)
    os.environ[MGM_ENV_KEY] = mgm

def cli_detect_fnal():
    mgm = None
    if os.uname()[1].endswith('.fnal.gov'):
        mgm = 'root://cmseos.fnal.gov'
        logger.warning('Detected fnal.gov host; using mgm %s as default if necessary', mgm)
    return mgm

def cli_flexible_format(lfn, mgm=None):
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
