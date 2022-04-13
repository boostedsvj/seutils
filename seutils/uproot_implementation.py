import seutils, os.path
import sys
from contextlib import contextmanager
from .root import Implementation


@contextmanager
def open_uproot(path, mode='READ'):
    '''
    Does nothing if an open uproot object is passed
    '''
    do_open = seutils.is_string(path)
    try:
        yieldable = path
        if do_open:
            try:
                import uproot
            except ImportError:
                import uproot3 as uproot
            seutils.logger.debug('Opening %s with uproot', path)
            yieldable = uproot.open(path)
        yield yieldable
    finally:
        if do_open:
            try:
                yieldable.close()
            except Exception:
                pass


def is_node(f):
    """
    Checks whether the type of f is Directory-like
    """
    # This is not very robust but the route via isinstance was not working out.
    cls_name = repr(f)
    return cls_name.startswith('<ROOTDirectory') or cls_name.startswith('<ReadOnlyDirectory')


def is_ttree(f):
    cls_name = repr(f)
    return cls_name.startswith('<TTree')


def decode(s):
    """
    In python 3, returns a `str` object; in python 2, does nothing
    """
    if sys.version_info[0] >= 3 and isinstance(s, bytes):
        return s.decode()
    return s


def iter_contents(f, prefix='', seen=None, depth=0):
    """
    Starting from a node-like `f`, iterates and yields all contents.
    """
    # Keep a memo of seen memory addresses to avoid double counting
    if seen is None: seen = set()
    if id(f) in seen: return
    seen.add(id(f))

    is_nodelike = is_node(f)

    # Get a name for this node; Can be either the path (if nodelike) or the treename
    try:
        name = (decode(f.path[-1]) if len(f.path) else '') if is_nodelike else decode(f.name)
    except AttributeError:
        # uproot3 compatibility 
        name = decode(f.name)
        name = name.split('.root')[-1]

    name = os.path.join(prefix, name)
    if name == '': name = '/'

    # Plug in the tree depth as an attribute
    f.____depth = depth
    yield name, f

    # If f is a node-like object, yield also all its children
    if is_nodelike:
        for value in f.values():
            # `yield from` is python 3 only
            for _ in iter_contents(value, prefix=name, seen=seen, depth=depth+1):
                yield _

        
class UprootImplementation(Implementation):

    def check_is_installed(self):
        try:
            import uproot
            return True
        except ImportError:
            return False

    def get(self, rootfile, key):
        with open_uproot(rootfile) as f:
            return f[key.strip('/')]

    def ls(self, rootfile):
        with open_uproot(rootfile) as f:
            return list(iter_contents(f))

    def trees(self, rootfile):
        with open_uproot(rootfile) as f:
            return [t for t in iter_contents(f) if is_ttree(t[1])]

    def branches(self, tree):
        return tree.items(recursive=True)

    def nentries(self, tree):
        try:
            return tree.num_entries
        except AttributeError:
            return tree.numentries

    def is_node(self, f):
        return is_node(f)

    def is_ttree(self, f):
        return is_ttree(f)


class Uproot3Implementation(UprootImplementation):

    def check_is_installed(self):
        try:
            import uproot3
            return True
        except ImportError:
            return False

    def branches(self, tree):
        items = tree.items(recursive=True)
        return [ (decode(k), b) for k, b in items ]
