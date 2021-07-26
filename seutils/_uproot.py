import seutils
import os.path as osp, sys
from contextlib import contextmanager
from seutils import run_command, get_exitcode, Inode, split_mgm, N_COPY_RETRIES
logger = seutils.logger

IS_INSTALLED = None
def is_installed():
    """
    Checks whether ROOT is on the python path
    """
    global IS_INSTALLED
    if IS_INSTALLED is None:
        try:
            import uproot
            IS_INSTALLED = True
        except ImportError:
            IS_INSTALLED = False
    return IS_INSTALLED

@contextmanager
def open_root(path, mode='READ'):
    '''
    Does nothing if an open uproot object is passed
    '''
    do_open = seutils.is_string(path)
    try:
        yieldable = path
        if do_open:
            import uproot
            logger.debug('Opening %s with uproot', path)
            yieldable = uproot.open(path)
        yield yieldable
    finally:
        if do_open:
            try:
                f.close()
            except Exception:
                pass

def trees(rootfile):
    with open_root(rootfile) as f:
        return [ k.rsplit(';',1)[0] for k, v in sorted(f.items()) if repr(v).startswith('<TTree') ]


def _iter_key_value_pairs(f, prefix=''):
    name = f.name
    if sys.version_info[0] > 2: name = name.decode()
    name = prefix + name.rsplit('/',1)[-1]
    print(name, f)
    classname = repr(f)
    if classname.startswith('<ROOTDirectory'):
        for value in f.values():
            yield from _iter_key_value_pairs(value, prefix=name + '/')
    elif classname.startswith('<TTree'):
        yield name, f

def _format_key_value_pair(treename, tree):
    treename = treename.rsplit(';',1)[0]
    try:
        numentries = tree.num_entries
        branches = [b.name for b in tree.branches]
    except AttributeError:
        numentries = tree.numentries
        branches = list(tree.keys())
        if sys.version_info[0] > 2: branches = [b.decode() for b in branches]
    return (treename, numentries, branches) if branches else (treename, numentries)

def trees_and_counts(rootfile, branches=False):
    r = []
    with open_root(rootfile) as f:
        for key, value in sorted(_iter_key_value_pairs(f)):
            r.append(_format_key_value_pair(key, value))
    return r


def branches(rootfile, treepath=None):
    with open_root(rootfile) as f:
        if treepath is None:
            treepath = seutils.root.select_most_likely_tree(trees(f))
        tree = f[treepath]
        for key in tree.keys(recursive=True):
            value = tree[key]
            yield (value, 1)

