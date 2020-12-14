import seutils
import os.path as osp
from seutils import run_command, get_exitcode, Inode, split_mgm, N_COPY_RETRIES
logger = seutils.logger

IS_INSTALLED = None
def is_installed():
    """
    Checks whether gfal-ls is on the path
    """
    global IS_INSTALLED
    if IS_INSTALLED is None: IS_INSTALLED = seutils.cmd_exists('gfal-ls')
    return IS_INSTALLED

def gfalstatline_to_inode(statline, parent_directory):
    """
    Converts a plain line as outputted by `gfal-ls -l` into an Inode object.
    `gfal-ls -l` returns only basenames, so the parent_directory from which the
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
        path = osp.join(parent_directory, components[8])
        return Inode(path, modtime, isdir, size)
    except:
        logger.error('Error parsing statline: %s', statline)
        raise

def mkdir(directory):
    run_command([ 'gfal-mkdir', '-p', directory ])

def stat(path, not_exist_ok=False):
    import datetime
    cmd = ['gfal-stat', path]
    output = run_command(cmd, nonzero_exitcode_ok=not_exist_ok)
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

def exists(path):
    return get_exitcode(['gfal-stat', path]) == 0

def isdir(directory):
    statinfo = stat(directory, not_exist_ok=True)
    if statinfo is None: return False
    return statinfo.isdir

def isfile(path):
    statinfo = stat(path, not_exist_ok=True)
    if statinfo is None: return False
    return statinfo.isfile

def is_file_or_dir(path):
    statinfo = stat(path, not_exist_ok=True)
    if statinfo is None:
        return 0
    elif statinfo.isdir:
        return 1
    elif statinfo.isfile:
        return 2

def listdir(directory, stat=False):
    cmd = [ 'gfal-ls', format(directory) ]
    if stat: cmd.append('-l')
    output = run_command(cmd)
    contents = []
    for l in output:
        l = l.strip()
        if not len(l): continue
        if stat:
            contents.append(gfalstatline_to_inode(l, directory))
        else:
            contents.append(format(osp.join(directory, l)))
    return contents

def cp(src, dst, n_retries=N_COPY_RETRIES, create_parent_directory=True, verbose=True, force=False):
    cmd = [ 'gfal-copy', '-t', '180', src, dst ]
    if create_parent_directory: cmd.insert(1, '-p')
    if verbose: cmd.insert(1, '-v')
    if force: cmd.insert(1, '-f')
    run_command(cmd, n_retries=n_retries)

def rm(path, recursive):
    cmd = [ 'gfal-rm', path ]
    if recursive: cmd.insert(-1, '-r')
    run_command(cmd)
