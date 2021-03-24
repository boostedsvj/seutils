import seutils
import os
import os.path as osp
import json
import datetime
import tempfile

from seutils import add_env_kwarg

from . import pyroot
from . import uproot


# _______________________________________________________
# Get commands

_implementations = [ uproot, pyroot ]
_commands = [ 'trees', 'trees_and_counts', 'branches', 'iter_branches' ]

def get_implementation(name):
    """
    Takes the name of a submodule that contains implementations. Returns that module object.
    """
    for implementation in _implementations:
        if implementation.__name__.rsplit('.',1)[-1] == name:
            break
    else:
        raise Exception('No such implementation: {0}'.format(name))
    return implementation

def get_command(cmd_name, implementation=None):
    """
    Returns an implementation of a command.
    Parameter `implementation` is a module object; it can also be a name of a module.
    If it's None, a heuristic is ran to guess the best implementation.
    The `path` parameter is optional and only serves to make a better guess for the implementation.
    """
    if not cmd_name in _commands:
        raise Exception('Invalid command: {0}'.format(cmd_name))
    if implementation is None:
        implementation = best_implementation_heuristic(cmd_name)[0]
    elif seutils.is_string(implementation):
        implementation = get_implementation(implementation)
    if not hasattr(implementation, cmd_name):
        raise Exception(
            'Implementation {0} has no function {1}'
            .format(implementation.__name__, cmd_name)
            )
    return getattr(implementation, cmd_name)

def best_implementation_heuristic(cmd_name):
    """
    Determines the best implementation.
    """

    # Grid protocols
    preferred_order = []
    def check(module):
        if module.is_installed() and hasattr(module, cmd_name) and not(module in preferred_order):
            preferred_order.append(module)
    check(uproot)
    check(pyroot)
    if not len(preferred_order):
        raise Exception(
            'No good implementation could be found for cmd {0}'
            .format(cmd_name)
            )
    seutils.logger.info('Using module %s to execute \'%s\'', preferred_order[0].__name__, cmd_name)
    return preferred_order


# _______________________________________________________
# Central entry points for root commands

@add_env_kwarg
def trees(rootfile, implementation=None):
    return get_command('trees', implementation)(rootfile)

@add_env_kwarg
def trees_and_counts(rootfile, branches=False, implementation=None):
    return get_command('trees_and_counts', implementation)(rootfile, branches)

@add_env_kwarg
def branches(rootfile, treepath=None, implementation=None):
    return get_command('branches', implementation)(rootfile, treepath)

@add_env_kwarg
def iter_branches(tree, treepath=None, implementation=None):
    return get_command('iter_branches', implementation)(tree)


# _______________________________________________________
# Algo's using the basic root functions above

def write_count_cache(cache, dst, failure_ok=True, delete_first=False):
    '''
    Writes the cache to dst by first opening a tempfile, copying it, and deleting the tempfile.

    If `failure_ok` is True, any encountered exceptions are ignored (since typically writing
    to the cache isn't critical).

    If `delete_first` is True it is attempted to delete the file first.

    Returns True if cache is successfully written.
    '''
    try:
        try:
            fd, path = tempfile.mkstemp()
            with open(path, 'w') as f:
                json.dump(cache, f, indent=2, sort_keys=True)
            if delete_first:
                try:
                    seutils.rm(dst)
                except Exception:
                    # Not a big deal if this fails
                    pass
            seutils.cp(path, dst)
        finally:
            os.close(fd)
    except Exception:
        if failure_ok:
            seutils.logger.warning('Could not write cache to %s', dst)
            return False
        else:
            raise
    return True


def count_dataset(directory, read_cache=True, write_cache=True, rootfiles=None):
    '''
    Looks for all root files in `directory` and counts the number of entries in all trees.

    By default a cache is created in `{directory}/.seucounts.json`, so that subsequent calls
    are faster.

    If `read_cache` is False, the cache is not read and assumed empty.
    If `write_cache` is False, the (potentially updated) cache will not be written.

    If `rootfiles` is specified, only those root files will be attempted to be read
    from the cache, and only those will be updated in the cache
    '''
    cache_file = osp.join(directory, '.seucounts.json')
    dir_inode = seutils.stat(directory)
    if dir_inode.isfile:
        raise Exception('{} is a file, not a directory'.format(directory))

    cache_inode = seutils.stat(cache_file, not_exist_ok=True)
    if read_cache:
        if cache_inode is None:
            cache = {}
        else:
            cache = json.loads(seutils.cat(cache_file))
            if dir_inode.modtime <= cache_inode.modtime:
                # The cache is the last modified file - just return the cache
                seutils.logger.info('Cached result (%s)', cache_inode.modtime)
                return cache
    else:
        cache = {}

    is_updated = False

    if rootfiles is None:
        rootfiles = seutils.ls_wildcard(osp.join(directory, '*.root'), stat=True)
    else:
        rootfiles = [seutils.stat(r) for r in rootfiles]

    for inode in rootfiles:
        if osp.dirname(inode.path).rstrip('/') != directory.rstrip('/'):
            raise Exception('rootfile {} is not in {}'.format(inode.path, directory))
        entry = cache.get(inode.basename, None)
        if entry and datetime.strptime(entry['_mtime'], '%Y%m%d_%H%M%S') > inode.modtime:
            # Entry is up to date - do nothing
            continue
        else:
            # Have to update the cache
            entry = {'_mtime' : datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}
            for treepath, nentries in seutils.root.trees_and_counts(inode.path):
                entry[treepath] = nentries
            cache[inode.basename] = entry
            is_updated = True

    if write_cache and is_updated:
        write_count_cache(
            cache, cache_file,
            delete_first=cache_inode is not None
            )
    return cache

def sum_dataset(directory, treepath=None, **kwargs):
    '''
    Returns only the sums of all entries
    If treepath is specified it returns a single int,
    otherwise it returns a dict of { treepath : count }
    '''
    output = count_dataset(directory, **kwargs)
    # Compute the tree total counts
    tree_totals = {}
    for rootfile, values in output.items():
        for key, value in values.items():
            if key == '_mtime': continue
            tree_totals.setdefault(key, 0)
            tree_totals[key] += value
    return tree_totals if treepath is None else tree_totals[treepath]

def select_most_likely_tree(trees):
    """
    Selects the 'most likely' tree the user intended from a list of trees.
    Typically this is the first one, minus some default CMSSW trees.
    """
    # Prefer other trees over these standard CMSSW trees
    filtered_trees = [ t for t  in trees if not t in [
        'MetaData', 'ParameterSets', 'Parentage', 'LuminosityBlocks', 'Runs'
        ]]
    # Pick the most likely tree
    if len(filtered_trees) == 0 and len(trees) >= 1:
        tree = trees[0]
        ignored_trees = trees[1:]
    elif len(filtered_trees) >= 1:
        tree = filtered_trees[0]
        ignored_trees = [ t for t in trees if not t == tree ]
    seutils.logger.info(
        'Using tree %s%s',
        tree,
        ' (ignoring {0})'.format(', '.join(ignored_trees)) if len(ignored_trees) else ''
        )
    return tree


def nentries(rootfile, treepath=None, **kwargs):
    '''
    Like count_dataset, but for a single file.
    If treepath is specified, returns the counts for that tree only
    '''
    directory = osp.dirname(rootfile)
    kwargs['rootfiles'] = [rootfile]
    entry = count_dataset(osp.dirname(rootfile), **kwargs)
    return entry if treepath is None else entry[treepath]


