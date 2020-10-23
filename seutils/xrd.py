import seutils
from seutils import run_command, get_exitcode, Inode, split_mgm, N_COPY_RETRIES
logger = seutils.logger

IS_INSTALLED = None
def is_installed():
    """
    Checks whether xrdfs is on the path
    """
    global IS_INSTALLED
    if IS_INSTALLED is None: IS_INSTALLED = seutils.cmd_exists('xrdfs')
    return IS_INSTALLED

def xrdstatline_to_inode(statline, mgm):
    """
    Converts a plain line as outputted by `xrdfs <mgm> ls -l <path>` into an Inode object
    """
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
    path = seutils.format(components[4], mgm)
    return Inode(path, modtime, isdir, size)

def cp(src, dst, n_retries=N_COPY_RETRIES, create_parent_directory=True, verbose=True, force=False):
    cmd = [ 'xrdcp', src, dst ]
    if not verbose: cmd.insert(1, '-s')
    if create_parent_directory: cmd.insert(1, '-p')
    if force: cmd.insert(1, '-f')
    run_command(cmd, n_retries=n_retries)

def listdir(directory, stat=False):
    mgm, path = split_mgm(directory)
    cmd = [ 'xrdfs', mgm, 'ls', path ]
    if stat: cmd.append('-l')
    output = run_command(cmd)
    contents = []
    for l in output:
        l = l.strip()
        if not len(l): continue
        if stat:
            contents.append(xrdstatline_to_inode(l, mgm))
        else:
            contents.append(seutils.format(l, mgm))
    return contents

def is_file_or_dir(path):
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

def is_file_or_dir_xrdoutputbased(path):
    """
    Looks for the flags in the actual output rather than using exit codes
    """
    mgm, path = split_mgm(path)
    cmd = [ 'xrdfs', mgm, 'stat', '-q', 'IsDir', path ]
    output = run_command(cmd, nonzero_exitcode_ok=True, return_output_on_nonzero_exitcode=True)
    exists = False
    is_directory = False
    for line in output:
        if line.startswith('Flags:'):
            exists = True
            if 'IsDir' in line: is_directory = True
            break
    if not exists:
        return 0
    elif is_directory:
        return 1
    else:
        return 2

def isfile(path):
    mgm, path = split_mgm(path)
    status = get_exitcode([ 'xrdfs', mgm, 'stat', '-q', 'IsDir', path ])
    # Error code 55 means path exists, but is not a directory
    return (status == 55)

def isdir(directory):
    mgm, directory = split_mgm(directory)
    cmd = [ 'xrdfs', mgm, 'stat', '-q', 'IsDir', directory ]
    return get_exitcode(cmd) == 0

def exists(path):
    mgm, path = split_mgm(path)
    cmd = [ 'xrdfs', mgm, 'stat', path ]
    return get_exitcode(cmd) == 0

def stat(path, not_exist_ok=False):
    import datetime
    mgm, path = split_mgm(path)
    cmd = [ 'xrdfs', mgm, 'stat', path ]
    output = run_command(cmd, nonzero_exitcode_ok=not_exist_ok)
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

def rm(path, recursive):
    # NB: xrdfs cannot recursively delete directories, so this is not the preferred tool
    mgm, lfn = split_mgm(path)
    if isdir(path):
        if not recursive:
            raise RuntimeError('{} is a directory but rm instruction is not recursive'.format(path))
        rm = 'rmdir'
    else:
        rm = 'rm'
    cmd = [ 'xrdfs', mgm, rm, lfn ]
    run_command(cmd)

def mkdir(directory):
    mgm, directory = split_mgm(directory)
    run_command([ 'xrdfs', mgm, 'mkdir', '-p', directory ])
