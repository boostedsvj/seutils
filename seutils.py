# -*- coding: utf-8 -*-
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

def run_command(cmd, dry=False):
    """
    Runs a command and captures output. Raises an exception on non-zero exit code.
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

def _unsafe_split_mgm(filename):
    """
    Takes a properly formatted path starting with 'root:' and containing '/store'
    """
    if not filename.startswith('root://'):
        raise ValueError(
            'Cannot split mgm; passed filename: {0}'
            .format(filename)
            )
    elif not '/store' in filename:
        raise ValueError(
            'No substring \'/store\' in filename {0}'
            .format(filename)
            )
    i = filename.index('/store')
    mgm = filename[:i]
    lfn = filename[i:]
    return mgm, lfn

def split_mgm(path, mgm=None):
    """
    Returns the mgm and lfn that the user most likely intended to
    if path starts with 'root://', the mgm is taken from the path
    if mgm is passed, it is used as is
    if mgm is passed AND the path starts with 'root://' AND the mgm's don't agree,
      an exception is thrown
    if mgm is None and path has no mgm, the default variable DEFAULT_MGM is taken
    """
    if path.startswith('root://'):
        _mgm, lfn = _unsafe_split_mgm(path)
        if not(mgm is None) and not _mgm == mgm:
            raise ValueError(
                'Conflicting mgms determined from path and passed argument: '
                'From path {0}: {1}, from argument: {2}'
                .format(path, _mgm, mgm)
                )
        mgm = _mgm
    elif mgm is None:
        mgm = get_default_mgm()
        lfn = path
    else:
        lfn = path
    # Sanity check
    if not lfn.startswith('/store'):
        raise ValueError(
            'LFN {0} does not start with \'/store\'; something is wrong'
            .format(lfn)
            )
    return mgm, lfn

def _join_mgm_lfn(mgm, lfn):
    """
    Joins mgm and lfn, ensures correct formatting.
    Will throw an exception of the lfn does not start with '/store'
    """
    if not lfn.startswith('/store'):
        raise ValueError(
            'This function expects filenames that start with \'/store\''
            )
    if not mgm.endswith('/'): mgm += '/'
    return mgm + lfn

def format(path, mgm=None):
    """
    Formats a path to ensure it is a path on the SE.
    Can take:
    - Just path starting with 'root:' - nothing really happens
    - Just path starting with '/store' - the default mgm is used
    - Path starting with 'root:' and an mgm - an exception is thrown in case of conflict
    - Path starting with '/store' and an mgm - mgm and path are joined
    """
    mgm, lfn = split_mgm(path, mgm=mgm)
    return _join_mgm_lfn(mgm, lfn)

# _______________________________________________________
# Interactions with SE

def mkdir(directory):
    """
    Creates a directory on the SE
    Does not check if directory already exists
    """
    mgm, directory = split_mgm(directory)
    logger.warning('Creating directory on SE: {0}'.format(_join_mgm_lfn(mgm, directory)))
    cmd = [ 'xrdfs', mgm, 'mkdir', '-p', directory ]
    run_command(cmd)

def isdir(directory):
    """
    Returns a boolean indicating whether the directory exists.
    Also returns False if the passed path is a file.
    """
    mgm, directory = split_mgm(directory)
    cmd = [ 'xrdfs', mgm, 'stat', '-q', 'IsDir', directory ]
    return get_exitcode(cmd) == 0

def exists(path):
    """
    Returns a boolean indicating whether the path exists.
    """
    mgm, path = split_mgm(path)
    cmd = [ 'xrdfs', mgm, 'stat', path ]
    return get_exitcode(cmd) == 0

def isfile(path):
    """
    Returns a boolean indicating whether the file exists.
    Also returns False if the passed path is a directory.
    """
    mgm, path = split_mgm(path)
    status = get_exitcode([ 'xrdfs', mgm, 'stat', '-q', 'IsDir', path ])
    # Error code 55 means path exists, but is not a directory
    return (status == 55)

def is_file_or_dir(path):
    """
    Returns 0 if path does not exist
    Returns 1 if it's a directory
    Returns 2 if it's a file
    """
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

def cp(src, dst, create_parent_directory=True):
    """
    Copies a file `src` to the storage element.
    Does not format `src` or `dst`; user is responsible for formatting.
    """
    logger.warning('Copying %s --> %s', src, dst)
    if create_parent_directory:
        cmd = [ 'xrdcp', '-s', '-p', src, dst ]
    else:
        cmd = [ 'xrdcp', '-s', src, dst ]
    run_command(cmd)

def cp_to_se(src, dst, create_parent_directory=True):
    """
    Like cp, but assumes dst is a location on a storage element and src is local
    """
    cp(src, format(dst))

def cp_from_se(src, dst, create_parent_directory=True):
    """
    Like cp, but assumes src is a location on a storage element and dst is local
    """
    cp(format(src), dst)

class Inode(object):
    """
    Basic container of information returned by xrdfs MGM ls -l PATH:
    permission string, modification time, size, and path
    """
    def __init__(self, statline, mgm=None):
        components = statline.strip().split()
        if not len(components) == 5:
            raise RuntimeError(
                'Expected 5 components for stat line:\n{0}'
                .format(statline)
                )
        self.permissions = components[0]
        self.isdir = self.permissions.startswith('d')
        self.isfile = not(self.isdir)
        import datetime
        self.modtime = datetime.datetime.strptime(components[1] + ' ' + components[2], '%Y-%m-%d %H:%M:%S')
        self.size = components[3]
        self.size_human = bytes_to_human_readable(float(self.size))
        self.localpath = components[4]
        if mgm:
            self.mgm = mgm
            self.path = format(self.localpath, mgm)

    def __repr__(self):
        if len(self.path) > 40:
            shortpath = self.path[:10] + '...' + self.path[-15:]
        else:
            shortpath = self.path
        return super(Inode, self).__repr__().replace('object', shortpath)

def ls(path, stat=False, assume_directory=False):
    """
    Lists all files and directories in a directory on the SE.
    It first checks whether the path exists and is a file or a directory.
    If it does not exist, it raises an exception.
    If it is a file, it just returns a formatted path to the file as a 1-element list
    If it is a directory, it returns a list of the directory contents (formatted)

    If stat is True, it returns Inode objects which contain more information beyond just the path
    If assume_directory is True, the first check is not performed and the algorithm assumes
    the user took care to pass a path to a directory.
    """
    mgm, path = split_mgm(path)
    if assume_directory:
        status = 1
    else:
        status = is_file_or_dir(format(path, mgm))
    # Depending on status, return formatted path to file, directory contents, or raise
    if status == 0:
        raise RuntimeError('Path \'{0}\' does not exist'.format(path))
    elif status == 1:
        # It's a directory; return contents
        cmd = [ 'xrdfs', mgm, 'ls', path ]
        if stat: cmd.append('-l')
        output = run_command(cmd)
        contents = []
        for l in output:
            l = l.strip()
            if not len(l): continue
            if stat:
                contents.append(Inode(l, mgm))
            else:
                contents.append(format(l, mgm))
        return contents
    elif status == 2:
        # It's a file; just return the path to the file
        return [_join_mgm_lfn(mgm, path)]

MAX_RECURSION_DEPTH = 20

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
    files.sort(key=lambda f: f.localpath)
    directories = [ c for c in contents if c.isdir ]
    directories.sort(key=lambda d: d.localpath)
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

    The algorithm is like `walk`, but discards directories that don't fit the pattern
    early.
    Still the number of requests can grow quickly; a limited number of wildcards is advised.
    """
    pattern = format(pattern)
    if not '*' in pattern: return ls(pattern, stat=stat)
    pattern_level = pattern.count('/')
    logger.debug('Level is %s for path %s', pattern_level, pattern)
    import re
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

def hadd(src, dst, dry=False):
    """
    Calls `ls_root` on `src` in order to be able to pass directories, then hadds.
    Needs ROOT env to be callable.
    """
    root_files = ls_root(src)
    if not len(root_files):
        raise RuntimeError('src {0} yielded 0 root files'.format(src))
    cmd = ['hadd', '-f', dst] + root_files
    if dry:
        logger.warning('hadd command: ' + ' '.join(cmd))
        return
    try:
        debug(True)
        run_command(cmd)
    except OSError as e:
        if e.errno == 2:
            logger.error('It looks like hadd is not on the path.')
        else:
            # Something else went wrong while trying to run `hadd`
            raise
    finally:
        debug(False)

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
        logger.warning('Detected fnal.gov host; using mgm %s', mgm)
    return mgm

def cli_flexible_format(lfn, mgm=None):
    if not lfn.startswith('root:') and not lfn.startswith('/'):
        try:
            prefix = '/store/user/' + os.environ['USER']
            logger.warning('Pre-fixing %s', prefix)
            lfn = os.path.join(prefix, lfn)
        except KeyError:
            pass
    if lfn.startswith('root:'):
        return format(lfn)
    else:
        return format(lfn, mgm)
