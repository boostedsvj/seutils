import os.path as osp


def has_protocol(filename):
    """
    Checks whether the filename contains a protocol.
    Currently a very basic string check, which so far has been enough
    """
    return ('://' in filename)


def is_ssh(path):
    return (':/' in path and not('://' in path))


def is_valid_path(filename):
    """
    Checks whether a filename has a protocol, a double slash, and does not
    start with a slash
    """
    return not(filename.startswith('/')) and has_protocol(filename) and '//' in filename.split('://',1)[1]


def has_mgm(filename):
    return is_valid_path(filename)


def assert_valid_path(filename):
    """
    Like is_valid_path but raises an exception if False
    """
    if not is_valid_path(filename):
        raise ValueError(
            'Filename {} is not a properly formatted physical file name'.format(filename)
            )


def get_lfn(filename):
    if is_ssh(filename): return '/' + filename.split(':/')[1]
    assert_valid_path(filename)
    return '/' + filename.split('://',1)[1].split('//',1)[1]


def get_depth(filename):
    """
    Returns the depth of the logical filename.
    Examples:
    >>> get_lfn('root://foo.bar.gov//')
    >>> 0
    >>> get_lfn('root://foo.bar.gov//aaa')
    >>> 1
    >>> get_lfn('root://foo.bar.gov//aaa/')
    >>> 2
    >>> get_lfn('root://foo.bar.gov//aaa/a')
    >>> 2
    """
    lfn = get_lfn(normpath(filename))
    if lfn == '/': return 0
    return lfn.count('/')


def split_protocol_server_lfn(filename):
    """
    Splits protocol, server and logical file name from a physical file name.
    Throws an exception if format-ensuring checks fail.
    """
    assert_valid_path(filename)
    protocol, rest = filename.split('://',1)
    server, lfn = rest.split('//',1)
    lfn = '/' + lfn # Restore the opening slash that was dropped in the split
    return protocol, server, lfn


def join_protocol_server_lfn(protocol, server, lfn):
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


def split_mgm(path):
    """
    Splits the path into an mgm and a lfn.
    Example:
    >>> split_mgm('root://foo.bar.gov//some/path')
    >>> ('root://foo.bar.gov/', '/some/path')
    """
    assert_valid_path(path)
    protocol, server, lfn = split_protocol_server_lfn(path)
    return protocol + '://' + server, lfn


def format_mgm(mgm, filename):
    """
    If `filename` does not have an mgm, returns `join_mgm(mgm, filename)`.
    If `filename` does have an mgm, raises an error if the passed mgm != the filename mgm
    """
    if not has_mgm(filename):
        return join_mgm(mgm, filename)
    else:
        mgm_from_filename, _ = split_mgm(filename)
        if mgm.rstrip('/') != mgm_from_filename.rstrip('/'):
            raise ValueError(
                'format_mgm encountered different mgms; from filename: {}; passed: {}'
                .format(mgm_from_filename, mgm)
                )
        return filename


def join_mgm(mgm, lfn):
    """
    Joins mgm and lfn, ensures correct formatting.
    Will throw an exception of the lfn does not start with '/'
    """
    if not lfn.startswith('/'):
        raise ValueError('join_mgm expects an absolute path (passed lfn: %s)' % lfn)
    if not mgm.endswith('/'): mgm += '/'
    return mgm + lfn


def dirname(path):
    """
    Like osp.dirname, but works with an mgm.
    """
    is_remote = has_protocol(path)
    if is_remote:
        mgm, path = split_mgm(path)
    path = osp.dirname(osp.normpath(path))
    return join_mgm(mgm, path) if is_remote else path


def normpath(path):
    """
    Like osp.normpath, but works with an mgm.
    """
    original_path = path
    is_remote = has_protocol(path)
    if is_remote:
        mgm, path = split_mgm(path)
    path = osp.normpath(path)
    if original_path.endswith('/') and path != '/': path += '/'
    return join_mgm(mgm, path) if is_remote else path


def relpath(path, start):
    """
    Like osp.relpath, but works with an mgm.
    """
    if has_protocol(path) != has_protocol(start):
        raise TypeError('{0} / {1}: either both or neither must have mgms'.format(path, start))
    mgm1 = ''
    mgm2 = ''
    if has_protocol(path): mgm1, path = split_mgm(path)
    if has_protocol(start): mgm2, start = split_mgm(start)
    if mgm1 != mgm2:
        raise TypeError('mgm mismatch: {0} vs. {1}'.format(mgm1, mgm2))
    path = osp.normpath(path)
    return osp.relpath(path, start)


def iter_parent_dirs(path):
    """
    Iterates through all the parent directories of a path
    E.g.:
    `'/foo/bar'` --> `['/foo', '/']`
    """
    dir = dirname(path)
    previous_dir = None
    while dir != previous_dir:
        yield dir
        previous_dir = dir
        dir = dirname(dir)


def get_protocol(path):
    """
    Returns the protocol contained in the path string
    """
    return path.split('://',1)[0]