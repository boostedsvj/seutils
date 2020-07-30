# -*- coding: utf-8 -*-
from __future__ import absolute_import
import os.path as osp
import logging, subprocess, os, glob, shutil
from contextlib import contextmanager

import seutils
from seutils import logger, debug, run_command, is_string

# _______________________________________________________
# hadd utilities

def hadd(src, dst, dry=False):
    """
    Calls `ls_root` on `src` in order to be able to pass directories, then hadds.
    Needs ROOT env to be callable.
    """
    root_files = ls_root(src)
    if not len(root_files):
        raise RuntimeError('src {0} yielded 0 root files'.format(src))
    _hadd(root_files, dst, dry=dry)

def hadd_chunks(src, dst, n_threads=6, chunk_size=200, tmpdir='/tmp', dry=False):
    """
    Calls `ls_root` on `src` in order to be able to pass directories, then hadds.
    Needs ROOT env to be callable.
    """
    root_files = ls_root(src)
    if not len(root_files):
        raise RuntimeError('src {0} yielded 0 root files'.format(src))
    _hadd_chunks(root_files, dst, n_threads, chunk_size, tmpdir, dry)

def _hadd(root_files, dst, dry=False):
    """
    Compiles and runs the hadd command
    """
    cmd = ['hadd', '-f', dst] + root_files
    if dry:
        logger.warning('hadd command: ' + ' '.join(cmd))
        return
    try:
        debug(True)
        run_command(cmd)
    except OSError as e:
        if e.errno == 2:
            logger.error('It looks like hadd is not on the path.')
        else:
            # Something else went wrong while trying to run `hadd`
            raise
    finally:
        debug(False)

def _hadd_packed(tup):
    """
    Just unpacks an input tuple and calls _hadd.
    Needed to work with multiprocessing.
    """
    return _hadd(*tup)

def _hadd_chunks(root_files, dst, n_threads=6, chunk_size=200, tmpdir='/tmp', dry=False):
    """
    Like hadd, but hadds a chunk of root files in threads to temporary files first,
    then hadds the temporary files into the final root file.
    The algorithm is recursive; if there are too many temporary files still, another intermediate
    chunked hadd is performed.
    """
    if not len(root_files):
        raise RuntimeError('src {0} yielded 0 root files'.format(src))
    elif len(root_files) < chunk_size:
        # No need for chunking. This should also be the final step of the recursion
        _hadd(root_files, dst, dry=dry)
        return

    import math, uuid, shutil, multiprocessing as mp
    n_chunks = int(math.ceil(len(root_files) / float(chunk_size)))

    # Make a unique directory for temporary files
    tmpdir = osp.join(tmpdir, 'tmphadd', str(uuid.uuid4()))
    os.makedirs(tmpdir)

    try:
        debug(True)
        chunk_rootfiles = []
        # First compile list of function arguments
        func_args = []
        for i_chunk in range(n_chunks):
            chunk = root_files[ i_chunk*chunk_size : (i_chunk+1)*chunk_size ]
            chunk_dst = osp.join(tmpdir, 'chunk{0}.root'.format(i_chunk))
            func_args.append([chunk, chunk_dst, dry])
            chunk_rootfiles.append(chunk_dst)
            if dry: logger.debug('hadding %s --> %s', ' '.join(chunk), chunk_dst)
        # Submit to multiprocessing in one go:
        if not dry:
            p = mp.Pool(n_threads)
            p.map(_hadd_packed, func_args)
            p.close()
            p.join()
        # Merge the chunks into the final destination, potentially with another chunked merge
        _hadd_chunks(chunk_rootfiles, dst, n_threads, chunk_size, tmpdir, dry)

    finally:
        logger.warning('Removing %s', tmpdir)
        shutil.rmtree(tmpdir)
        debug(False)

# _______________________________________________________
# Root utilities

USE_CACHE=False
CACHE_NENTRIES=None
CACHE_TREESINFILE=None

def use_cache(flag=True):
    global CACHE_NENTRIES
    global CACHE_TREESINFILE
    global USE_CACHE
    USE_CACHE = flag
    if flag:
        from .cache import FileCache
        if CACHE_NENTRIES is None:        
            CACHE_NENTRIES = FileCache('seutils.nentries')
        if CACHE_TREESINFILE is None:        
            CACHE_TREESINFILE = FileCache('seutils.treesinfile')
    else:
        USE_CACHE=False
        CACHE_NENTRIES=None
        CACHE_TREESINFILE=None

@contextmanager
def nocache():
    """
    Context manager to temporarily disable the cache
    """
    global USE_CACHE
    global CACHE_NENTRIES
    global CACHE_TREESINFILE
    _saved_CACHE_NENTRIES = CACHE_NENTRIES
    _saved_CACHE_TREESINFILE = CACHE_TREESINFILE
    USE_CACHE = False
    CACHE_NENTRIES = None
    CACHE_TREESINFILE = None
    try:
        yield None
    finally:
        USE_CACHE = True
        CACHE_NENTRIES = _saved_CACHE_NENTRIES
        CACHE_TREESINFILE = _saved_CACHE_TREESINFILE

@contextmanager
def open_root(rootfile):
    """
    Context manager to open a root file with pyroot
    """
    import ROOT
    logger.debug('Opening %s with pyroot', rootfile)
    tfile = ROOT.TFile.Open(rootfile)
    try:
        yield tfile
    finally:
        # Attempt to close, but closing can fail if nothing opened in the first place,
        # so accept any exception.
        try:
            tfile.Close()
        except:
            pass

def _iter_trees_recursively_root(node, prefix=''):
    """
    Takes a ROOT TDirectory-like node, and traverses through
    possible sub-TDirectories to yield the names of all TTrees.
    Can take a TFile.
    """
    listofkeys = node.GetListOfKeys()
    n_keys = listofkeys.GetEntries()
    for i_key in range(n_keys):
        key = listofkeys[i_key]
        classname = key.GetClassName()
        # Recurse through TDirectories
        if classname == 'TDirectoryFile':
            dirname = key.GetName()
            lower_node = node.Get(dirname)
            for tree in iter_trees_recursively(lower_node, prefix=prefix+dirname+'/'):
                yield tree
        elif not classname == 'TTree':
            continue
        else:
            treename = key.GetName()
            yield prefix + treename

def _get_trees_recursively_root(node):
    return list(_iter_trees_recursively_root(node))

def _get_trees_cache(rootfile):
    """
    Queries the cache for trees in rootfile.
    Returns None if no cached result is found.
    """
    global CACHE_TREESINFILE
    global USE_CACHE
    # Check if cache should be used and whether it exists
    if not USE_CACHE:
        return None
    # If rootfile is TDirectory-like, use that
    try:
        rootfile = rootfile.GetPath()
    except AttributeError:
        pass
    if rootfile in CACHE_TREESINFILE:
        trees = CACHE_TREESINFILE[rootfile]
        logger.info('Using cached result trees in %s: %s', rootfile, trees)
        return trees
    return None        

def _select_most_likely_tree(trees):
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
    logger.info(
        'Using tree %s%s',
        tree,
        ' (ignoring {0})'.format(', '.join(ignored_trees)) if len(ignored_trees) else ''
        )
    return tree

def get_trees(node):
    """
    Returns a list of the available trees in `node`.
    The cache is queried first.
    If node is a string, it is opened to get a TFile pointer.
    """
    nodename = node if is_string(node) else node.GetPath()
    cached_result = _get_trees_cache(nodename)
    if cached_result:
        return cached_result
    logger.debug('No cached result for %s', nodename)
    if is_string(node):
        with open_root(node) as tfile:
            trees = _get_trees_recursively_root(tfile)
    else:
        trees = _get_trees_recursively_root(node)
    # Update the cache
    if USE_CACHE:
        CACHE_TREESINFILE[nodename] = trees
        CACHE_TREESINFILE.sync()
    return trees

def _count_entries_root(tfile, tree='auto'):
    if tree == 'auto':
        trees = _get_trees_recursively_root(tfile)
        if len(trees) == 0:
            logger.error('No TTrees found in %s', tfilename)
            return None
        tree = _select_most_likely_tree(trees)
    ttree = tfile.Get(tree)
    return ttree.GetEntries()

def _count_entries_cache(rootfile, tree):
    global CACHE_NENTRIES
    global USE_CACHE
    # Check if cache should be used and whether it exists
    if not (USE_CACHE and CACHE_NENTRIES):
        return None
    key = rootfile + '___' + tree
    if key in CACHE_NENTRIES:
        nentries = CACHE_NENTRIES[key]
        logger.info('Using cached nentries in %s: %s', key, nentries)
        return nentries
    return None

def count_entries(rootfile, tree='auto'):
    # Try to use the cache to resolve a potential auto tree early
    if tree == 'auto' and USE_CACHE:
        trees = _get_trees_cache(rootfile)
        if trees: tree = _select_most_likely_tree(trees)
    # If tree is not (or no longer) 'auto', try to use the cache for nentries
    if tree != 'auto' and USE_CACHE:
        nentries = _count_entries_cache(rootfile, tree)
        if nentries: return nentries
    # At least some part of the cached couldn't return, so open the root file
    with open_root(rootfile) as tfile:
        nentries = _count_entries_root(tfile, tree)
    # Cache the result
    if USE_CACHE:
        CACHE_NENTRIES[rootfile + '___' + tree] = nentries
        CACHE_NENTRIES.sync()
    return nentries
