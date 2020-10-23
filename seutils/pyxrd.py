import math, datetime, os.path as osp
import seutils
from seutils import logger, debug, run_command, is_string, split_mgm

IS_INSTALLED = None
def is_installed():
    """
    Checks whether the python bindings of XRootD are importable
    """
    global IS_INSTALLED
    if IS_INSTALLED is None:
        try:
            import XRootD
            IS_INSTALLED = True
        except ImportError:
            IS_INSTALLED = False
    return IS_INSTALLED

_FILESYSTEMCACHE = {}
def get_client(mgm):
    global _FILESYSTEMCACHE
    mgm = mgm.strip('/')
    if not mgm in _FILESYSTEMCACHE:
        logger.info('Starting new client for %s', mgm)
        from XRootD import client
        filesystem = client.FileSystem(mgm)
        status, _ = filesystem.ping()
        logger.info('Filesystem %s status: %s', mgm, status)
        if not status.ok:
            raise ValueError(
                'client {0} is not responsive: {1}'
                .format(mgm, status)
                )
        _FILESYSTEMCACHE[mgm] = filesystem
    else:
        filesystem = _FILESYSTEMCACHE[mgm]
    return filesystem

FLAGVALS = list(range(7))
_FLAGVALS_PREPARED = False
def read_statinfoflagenum():
    """
    StatInfoFlags is an enum with 2^n values; transform it to an ordinary
    python list
    """
    global FLAGVALS, _FLAGVALS_PREPARED
    if not _FLAGVALS_PREPARED:
        _FLAGVALS_PREPARED = True
        from XRootD import client
        log = lambda val: int(math.log(val, 2.0))
        FLAGVALS[log(client.flags.StatInfoFlags.X_BIT_SET)] = 'X_BIT_SET'
        FLAGVALS[log(client.flags.StatInfoFlags.IS_DIR)] = 'IS_DIR'
        FLAGVALS[log(client.flags.StatInfoFlags.OTHER)] = 'OTHER'
        FLAGVALS[log(client.flags.StatInfoFlags.OFFLINE)] = 'OFFLINE'
        FLAGVALS[log(client.flags.StatInfoFlags.POSC_PENDING)] = 'POSC_PENDING'
        FLAGVALS[log(client.flags.StatInfoFlags.IS_READABLE)] = 'IS_READABLE'
        FLAGVALS[log(client.flags.StatInfoFlags.IS_WRITABLE)] = 'IS_WRITABLE'

def statinfoflag_to_flags(flag):
    """
    Takes an int and returns a list of flags
    """
    read_statinfoflagenum()
    binary = bin(flag)[2:][::-1]
    flags = []
    for i, val in enumerate(binary):
        if i > len(FLAGVALS):
            logger.error('binary %s exceeded expected length', binary)
            break
        if val == '1':
            flags.append(FLAGVALS[i])
    return flags

def statinfo_to_inode(path, statinfo):
    """
    Converts a path and a statinfo object to an Inode object
    """
    return seutils.Inode(
        path,
        datetime.datetime.strptime(statinfo.modtimestr, '%Y-%m-%d %H:%M:%S'),
        'IS_DIR' in statinfoflag_to_flags(statinfo.flags),
        statinfo.size
        )

def mkdir(path):
    """
    Creates a directory on the SE
    Does not check if directory already exists
    """
    from XRootD import client
    mgm, directory = seutils.split_mgm(path)
    logger.warning('Creating directory on SE: {0}'.format(path))
    filesystem = get_client(mgm)
    status, _ = filesystem.mkdir(directory, client.flags.MkDirFlags.MAKEPATH)
    if not status.ok:
        raise ValueError(
            'Directory {0} on {1} could not be created: {2}'
            .format(directory, mgm, status)
            )
    logger.info('Created directory %s: %s', directory, status)

def stat(path, not_exist_ok=False):
    """
    """
    import XRootD
    mgm, path = split_mgm(path)
    filesystem = get_client(mgm)
    status, statinfo = filesystem.stat(path)
    if not status.ok:
        msg = 'stat: Could not access {0}: status {1}'.format(path, status)
        if not_exist_ok:
            logger.info(msg)
            return None
        else:
            raise Exception(msg)
    return statinfo_to_inode(path, statinfo)

def exists(path):
    inode = stat(path, not_exist_ok=True)
    return not(inode is None)

def is_file_or_dir(path):
    inode = stat(path, not_exist_ok=True)
    if inode is None:
        return 0
    elif inode.isdir:
        return 1
    else:
        return 2

def isdir(directory):
    return stat(directory).isdir

def isfile(directory):
    return stat(directory).isfile

def listdir(path, stat=False, assume_directory=False):
    from XRootD import client
    # Check whether path is actually a directory
    if not assume_directory and not isdir(path):
        raise Exception('Path {0} is not a directory'.format(path))
    # Retrieve listobj
    mgm, directory = split_mgm(path)
    filesystem = get_client(mgm)
    status, listobj = filesystem.dirlist(directory, client.flags.DirListFlags.STAT if stat else 0)
    if not status.ok:
        raise ValueError(
            'Could not list {0}: {1}'
            .format(directory, status)
            )
    # Transform to desired list of contents
    contents = []
    for item in listobj:
        itempath = osp.join(path, item.name)
        if stat:
            contents.append(statinfo_to_inode(itempath, item.statinfo))
        else:
            contents.append(itempath)
    return contents
