import seutils
import os.path as osp
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

def trees_and_counts(rootfile, branches=False):
    r = []
    with open_root(rootfile) as f:
        for key, value in sorted(f.items()):
            if not repr(value).startswith('<TTree'): continue
            key = key.rsplit(';',1)[0]
            if branches:
                r.append((key, value.num_entries, [b.name for b in value.branches]))
            else:
                r.append((key, value.num_entries))
    return r

def branches(rootfile, treepath=None):
    with open_root(rootfile) as f:
        if treepath is None:
            treepath = seutils.root.select_most_likely_tree(trees(f))
        tree = f[treepath]
        for key in tree.keys(recursive=True):
            value = tree[key]
            yield (value, 1)

