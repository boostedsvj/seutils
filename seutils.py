# -*- coding: utf-8 -*-
import os.path as osp
import logging, subprocess, os, glob, shutil

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
        logger.setLevel(logging.WARNING)
        logger.addHandler(handler)
    return logger
logger = setup_logger()

def debug(flag=True):
    """Sets the logger level to debug (for True) or warning (for False)"""
    logger.setLevel(logging.DEBUG if flag else logging.WARNING)

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

def ls(path):
    """
    Lists all files and directories in a directory on the se
    """
    mgm, path = split_mgm(path)
    status = get_exitcode([ 'xrdfs', mgm, 'stat', '-q', 'IsDir', path ])
    if status == 55:
        # It's a file; just return the path to the file
        return [_join_mgm_lfn(mgm, path)]
    elif status == 0:
        # It's a directory; return contents
        contents = run_command([ 'xrdfs', mgm, 'ls', path ])
        return [ format(l.strip(), mgm=mgm) for l in contents if not len(l.strip()) == 0 ]
    else:
        raise RuntimeError('Path \'{0}\' does not exist'.format(path))

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


def hadd(src, dst):
    """
    Calls `ls_root` on `src` in order to be able to pass directories, then hadds.
    Needs ROOT env to be callable.
    """
    root_files = ls_root(src)
    if not len(root_files):
        raise RuntimeError('src {0} yielded 0 root files'.format(src))
    cmd = ['hadd', dst] + root_files
    try:
        run_command(cmd)
    except OSError as e:
        if e.errno == 2:
            logger.error('It looks like hadd is not on the path.')
        else:
            # Something else went wrong while trying to run `hadd`
            raise


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
