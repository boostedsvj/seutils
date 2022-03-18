import seutils
import uuid
import os.path as osp
import random
import datetime
import copy

global_rd = random.Random()
global_rd.seed(1006)

def generate_fake_node(path=None, modtime=None, isdir=None, size=None, parent_dir='/', rd=None, content=None):
    if rd is None: rd = global_rd
    if isdir is None:
        isdir = rd.random() < .2
    if path is None:
        uuid_str = str(uuid.UUID(int=rd.getrandbits(128)))[:8]
        if isdir:
            path = osp.join(parent_dir, 'dir_' + uuid_str)
        else:
            path = osp.join(parent_dir, 'file_' + uuid_str + '.file')
    if modtime is None:
        modtime = datetime.datetime(
            2020,
            rd.randint(1,4),  # month
            rd.randint(1,28), # day
            rd.randint(0,23), # hour
            rd.randint(0,59), # minute
            rd.randint(0,59), # second
            )
    if size is None:
        size = rd.randint(100, 1e5) * 10**(int(7*rd.random()))
    node = seutils.Inode(path, modtime, isdir, size)
    if content:
        node._content = content
    return node

def generate_fake_file(*args, **kwargs):
    kwargs['isdir'] = False
    return generate_fake_node(*args, **kwargs)

def generate_fake_dir(*args, **kwargs):
    kwargs['isdir'] = True
    return generate_fake_node(*args, **kwargs)

def generate_fake_tree(rd=None):
    if rd is None: rd = global_rd
    nodes = []
    nodes.append(generate_fake_dir(path='/'))
    nodes.append(generate_fake_dir(path='/foo'))
    root = generate_fake_dir(path='/foo/bar/')
    nodes.append(root)
    def add_level(node, depth=0):
        n_subdirs = max(rd.randint(0,4)-depth, 0)
        for i_subdir in range(n_subdirs):
            subdir = generate_fake_dir(parent_dir=node.path)
            yield subdir
            for i_file in range(rd.randint(0, 7)):
                yield generate_fake_file(parent_dir=subdir.path)
            for subsubdir in add_level(subdir, depth=depth+1):
                yield subsubdir
    nodes.extend(list(add_level(root)))
    return nodes


class FakeFS(object):
    def __init__(self, nodes=None):
        self.nodes = [] if nodes is None else nodes

    @property
    def paths(self):
        return (node.path for node in self.nodes)

    def normalize_path(self, path):
        return osp.normpath(path)

    def __contains__(self, path):
        if isinstance(path, seutils.Inode):
            path = path.path
        path = self.normalize_path(path)
        for contained_path in self.paths:
            if path == contained_path:
                return True
        return False

    def get_node(self, path):
        path = seutils.path.normpath(path)
        for node in self.nodes:
            if path.rstrip('/') == node.path.rstrip('/'):
                return node
        raise seutils.NoSuchPath(path)

    def stat(self, path):
        return self.get_node(path)

    def isdir(self, path):
        try:
            node = self.stat(path)
            return node.isdir
        except seutils.NoSuchPath:
            return False

    def isfile(self, path):
        try:
            node = self.stat(path)
            return node.isfile
        except seutils.NoSuchPath:
            return False

    def is_file_or_dir(self, path):
        try:
            node = self.stat(path)
            return 1 if node.isdir else 2
        except seutils.NoSuchPath:
            return 0

    def rm(self, path, recursive=False):
        node = self.stat(path)
        if node.isdir and not(recursive):
            raise ValueError('Cannot rm: rm directory requires recursive')
        self.nodes.remove(node)

    def exists(self, path):
        try:
            self.stat(path)
            return True
        except seutils.NoSuchPath:
            return False

    def listdir(self, path, stat=False):
        path = seutils.path.normpath(path)
        node = self.stat(path)
        if not node.isdir:
            raise TypeError('{} is not a directory'.format(path))
        contents = []
        for node in self.nodes:
            if path.rstrip('/') == node.dirname.rstrip('/'):
                contents.append(node if stat else node.path)
        return contents

    def cp(self, src, dst):
        seutils.logger.debug('FakeFS cp: {0} -> {1}'.format(src, dst))
        if self.exists(dst):
            raise ValueError('Path {} exists'.format(dst))
        src_node = self.stat(src)
        dst_node = copy.deepcopy(src_node)
        dst_node.path = osp.normpath(dst)
        self.nodes.append(dst)

    def cat(self, path):
        node = self.stat(path)
        if node.isdir:
            raise TypeError('{} is a directory'.format(path))
        if not hasattr(node, '_content'):
            import string
            choices = string.ascii_letters + string.digits
            node._content = ''.join(global_rd.choice(choices) for n in range(global_rd.randint(4,200)))
        return node._content

    def mkdir(self, path):
        self.put(path, isdir=True)

    def put(self, path, *args, **kwargs):
        """Puts a node in this fake file system.
        Will also create all directories leading up to the file, if they don't exist yet.
        Not strictly a valid interaction with a storage element, but convenient to have."""
        path = self.normalize_path(path)
        if self.exists(path):
            raise ValueError('Path {0} already exists'.format(path))
        node = generate_fake_node(path, *args, **kwargs)
        self.nodes.append(node)
        # Also make sure any directories leading up to this node are created
        for parent_dir in seutils.path.iter_parent_dirs(path):
            if self.isfile(parent_dir):
                raise ValueError('Parent path {0} is also a file'.format(parent_dir))
            elif not self.isdir(parent_dir):
                seutils.logger.debug('Creating also {0}'.format(parent_dir))
                self.nodes.append(generate_fake_dir(path=parent_dir))
        return node

class FakeRemoteFS(FakeFS):
    def __init__(self, mgm):
        self.mgm = mgm.rstrip('/')
        super(FakeRemoteFS, self).__init__()

    def stat(self, path):
        path = seutils.path.normpath(path)
        for node in self.nodes:
            if path.rstrip('/') == node.path.rstrip('/'):
                return node
        raise seutils.NoSuchPath(path)

    def put(self, path, *args, **kwargs):
        """Puts a node in this fake file system.
        Will also create all directories leading up to the file, if they don't exist yet.
        Not strictly a valid interaction with a storage element, but convenient to have."""
        path = seutils.path.format_mgm(self.mgm, path)
        if self.exists(path):
            raise ValueError('Path {0} already exists'.format(path))
        seutils.logger.debug('Put: %s', path)
        node = generate_fake_node(path, *args, **kwargs)
        self.nodes.append(node)
        # Also make sure any directories leading up to this node are created
        for parent_dir in seutils.path.iter_parent_dirs(path):
            if self.isfile(parent_dir):
                raise ValueError('Parent path {0} is also a file'.format(parent_dir))
            elif not self.isdir(parent_dir):
                seutils.logger.debug('Creating also {0}'.format(parent_dir))
                self.nodes.append(generate_fake_dir(path=parent_dir))
        return node

    def mkdir(self, path):
        self.put(path, isdir=True)


class FakeFSTransaction:
    def __init__(self, cmd, *args, **kwargs):
        if not hasattr(FakeFS, cmd):
            raise AttributeError('{0} is not a valid transaction'.format(cmd))
        self.postprocess = kwargs.pop('postprocess', None)
        self.mgm = kwargs.pop('mgm', None)
        self.cmd = cmd
        self.args = args
        self.kwargs = kwargs

    def execute(self, fs):
        seutils.logger.debug(
            'Executing {0} with args={1}, kwargs={2}'
            .format(self.cmd, self.args, self.kwargs)
            )
        product = getattr(fs, self.cmd)(*self.args, **self.kwargs)
        if self.postprocess:
            product = self.postprocess(product)
        return product


class NotIntercepted(Exception):
    def __init__(self, cmd):
        super(Exception, self).__init__('Could not intercept command: {}'.format(cmd))


class FakeCommandInterceptor:

    RCODE_NOSUCHPATH = 1
    RCODE_UNREACHABLE = 99

    def intercept(self, cmd, flags):
        raise NotImplementedError

    def mkdir(self, path, flags):
        return FakeFSTransaction('mkdir', path)

    def rm(self, path, flags):
        recursive = '-r' in flags or '--recursive' in flags
        return FakeFSTransaction('rm', path, recursive=recursive)

    def ls(self, path, flags):
        return FakeFSTransaction('listdir', path, stat='-l' in flags)

    def cat(self, path, flags):
        return FakeFSTransaction('cat', path)


class FakeGfalInterceptor(FakeCommandInterceptor):

    RCODE_NOSUCHPATH = 2
    RCODE_UNREACHABLE = 113

    def intercept(self, cmd, flags):
        gfal_cmd = cmd[0]
        if gfal_cmd == 'gfal-copy':
            return FakeFSTransaction('cp', cmd[-2], cmd[-1])
        if not gfal_cmd.startswith('gfal-'):
            raise NotIntercepted(cmd + flags)
        # mgm, path = seutils.path.split_mgm(cmd[1])
        path = cmd[1]
        mgm, _ = seutils.path.split_mgm(path)
        try:
            transaction = getattr(self, gfal_cmd.replace('gfal-',''))(path, flags)
            transaction.mgm = mgm
            return transaction
        except AttributeError:
            raise NotIntercepted(cmd + flags)

    def stat(self, path, flags):
        # node = self.fs.stat(cmd[-1])
        def postprocess(node):
            return (
                "  File: '{}'\n"
                "  Size: {}	{}\n"
                "Access: (0664/-rw-rw-r--)	Uid: 5678	Gid: 0\n"
                "Access: {date}\n"
                "Modify: {date}\n"
                "Change: {date}"
                ).format(
                    node.path,
                    node.size,
                    'regular file' if node.isfile else 'directory',
                    date = node.modtime.strftime('%Y-%m-%d %H:%M:%S.%f')
                    ).split('\n')
        return FakeFSTransaction('stat', path, postprocess=postprocess)

    def ls(self, path, flags):
        stat = '-l' in flags
        # gfal-ls returns basenames
        if stat:
            def postprocess(contents):
                lines = []
                for node in contents:
                    permissions = 'drwxrwxr-x' if node.isdir else '-rw-rw-r--'
                    gfal_bits = '1782' if node.isdir else '0   '
                    size = '4096' if node.isdir else str(node.size)
                    date = node.modtime.strftime('%b %d %H:%M')
                    line = '{0}   1 {1}  0 {2:<13s} {3} {4}'.format(
                        permissions, gfal_bits, size, date, node.basename
                        )
                    lines.append(line)
                return lines
        else:
            def postprocess(contents):
                return [osp.basename(p) for p in contents]
        return FakeFSTransaction('listdir', path, stat=stat, postprocess=postprocess)


class FakeXrdInterceptor(FakeCommandInterceptor):

    RCODE_NOSUCHPATH = 54
    RCODE_UNREACHABLE = 51

    def intercept(self, cmd, flags):
        if cmd[0] == 'xrdcp':
            seutils.logger.debug('Intercepted: cmd={0}, flags={1}'.format(cmd, flags))
            return FakeFSTransaction('cp', cmd[1], cmd[2])
        if cmd[0] != 'xrdfs':
            raise NotIntercepted(cmd + flags)
        try:
            xrd_cmd = cmd[2]
            path = seutils.path.join_mgm(cmd[1], cmd[3])
            transaction = getattr(self, xrd_cmd)(path, flags)
            transaction.mgm = cmd[1]
            return transaction
        except (IndexError, AttributeError):
            raise NotIntercepted(cmd + flags)

    def stat(self, path, flags):
        def postprocess(node):
            return [
                'Path:   {0}'.format(node.path),
                'Id:     66571293008382719'.format(),
                'Size:   {0}'.format(node.size),
                'MTime:  {0}'.format(node.modtime.strftime('%Y-%m-%d %H:%M:%S')),
                'Flags:  {0}'.format('19 (XBitSet|IsDir|IsReadable)' if node.isdir else '16 (IsReadable)'),
                ]
        return FakeFSTransaction('stat', path, postprocess=postprocess)

    def rm(self, path, flags):
        return FakeFSTransaction('rm', path, recursive=False)

    def rmdir(self, path, flags):
        return FakeFSTransaction('rm', path, recursive=True)

    def ls(self, path, flags):
        stat = '-l' in flags
        if stat:
            def postprocess(contents):
                lines = []
                for node in contents:
                    permissions = 'dr-x' if node.isdir else '-r--'
                    date = node.modtime.strftime('%Y-%m-%d %H:%M:%S')
                    line = '{0} {1} {2:<10s} {3}'.format(
                        permissions, date, str(node.size), node.path_no_mgm
                        )
                    lines.append(line)
                return lines
        else:
            postprocess = None
        return FakeFSTransaction('listdir', path, stat=stat, postprocess=postprocess)



class FakeInternet:
    '''Collection of a few file systems, and using multiple implementation interceptors.

    Any other non-local mgm usage will throw an error.
    '''    

    def __init__(self):
        self.fs = {
            '<local>' : FakeFS(),
            'root://foo.bar.gov' : FakeFS(),
            'gsiftp://foo.bar.gov' : FakeFS(),
            'root://cmseos.fnal.gov' : FakeFS(),
            'gsiftp://hepcms-gridftp.umd.edu' : FakeFS(),
            }
        self.interceptors = [
            FakeGfalInterceptor(),
            FakeXrdInterceptor()
            ]

    @property
    def mgms(self):
        return self.fs.keys()

    def check_mgm_exists(self, mgm):
        exists = mgm in self.mgms
        if not exists:
            seutils.logger.error(
                'FakeInternet: mgm %s does not exist (available: %s)',
                mgm, ', '.join(self.mgms)
                )
        return exists

    def cp(self, transaction, interceptor):
        src = transaction.args[0]
        dst = transaction.args[1]
        src_mgm, src_path = seutils.path.split_mgm(src) if seutils.path.has_protocol(src) else ('<local>', src)
        dst_mgm, dst_path = seutils.path.split_mgm(dst) if seutils.path.has_protocol(dst) else ('<local>', dst)
        for mgm in [src_mgm, dst_mgm]:
            if not self.check_mgm_exists(mgm):
                return interceptor.RCODE_UNREACHABLE, ''
        try:
            src_node = self.fs[src_mgm].stat(src)
        except seutils.NoSuchPath:
            return interceptor.RCODE_NOSUCHPATH, ''
        dst_node = copy.deepcopy(src_node)
        dst_node.path = dst
        self.fs[dst_mgm].nodes.append(dst_node)
        return 0, ''

    def intercept(self, cmd):
        flags = [c.strip() for c in cmd if c.startswith('-')]
        cmd = [c.strip() for c in cmd if not c.startswith('-')]

        for interceptor in self.interceptors:
            try:
                transaction = interceptor.intercept(cmd, flags)
                break
            except NotIntercepted:
                continue
        else:
            # No interceptor managed to intercept this command
            raise NotIntercepted(cmd)

        if transaction.cmd == 'cp':
            return self.cp(transaction, interceptor)

        # If the extracted mgm does not exist on the internet, return error code
        if transaction.mgm not in self.mgms:
            seutils.logger.error(
                'FakeInternet: mgm %s does not exist (available: %s)',
                transaction.mgm, ', '.join(self.mgms)
                )
            return interceptor.RCODE_UNREACHABLE, ''

        try:
            output = transaction.execute(self.fs[transaction.mgm])
            if output is None: output = ''
            return 0, output
        except seutils.NoSuchPath:
            seutils.logger.error(
                'FakeInternet: Path does not exist; available:\n%s',
                '\n'.join([node.path for fs in self.fs.values() for node in fs.nodes])
                )
            return interceptor.RCODE_NOSUCHPATH, ''


def activate_command_interception(fake_internet=None):
    """
    Fakes the seutils.run_command_rcode_and_output function with the fake interceptors.
    """
    if fake_internet is None: fake_internet = FakeInternet()
    def fake_run_command_rcode_and_output(cmd, env=None, dry=None):
        return fake_internet.intercept(cmd)
    seutils.__backup__run_command_rcode_and_output = seutils.run_command_rcode_and_output
    seutils.run_command_rcode_and_output = fake_run_command_rcode_and_output
    seutils.active_fake_internet = fake_internet

def deactivate_command_interception():
    if hasattr(seutils, '__backup__run_command_rcode_and_output'):
        seutils.run_command_rcode_and_output = seutils.__backup__run_command_rcode_and_output
        del seutils.__backup__run_command_rcode_and_output