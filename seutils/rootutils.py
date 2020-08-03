# -*- coding: utf-8 -*-
from __future__ import absolute_import
import os.path as osp
import logging, subprocess, os, glob, shutil, math, uuid
from contextlib import contextmanager

import seutils
from seutils import logger, debug, run_command, is_string

# _______________________________________________________
# hadd utilities

def hadd(src, dst, dry=False):
    """
    Calls `seutils.ls_root` on `src` in order to be able to pass directories, then hadds.
    Needs ROOT env to be callable.
    """
    root_files = seutils.ls_root(src)
    if not len(root_files):
        raise RuntimeError('src {0} yielded 0 root files'.format(src))
    _hadd(root_files, dst, dry=dry)

def hadd_chunks(src, dst, n_threads=6, chunk_size=200, tmpdir='/tmp', dry=False):
    """
    Calls `seutils.ls_root` on `src` in order to be able to pass directories, then hadds.
    Needs ROOT env to be callable.
    """
    root_files = seutils.ls_root(src)
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

    import multiprocessing as mp
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
def open_root(rootfile, mode='read'):
    """
    Context manager to open a root file with pyroot
    """
    import ROOT
    logger.debug('Opening %s with pyroot', rootfile)
    tfile = ROOT.TFile.Open(rootfile, mode)
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
            for tree in _iter_trees_recursively_root(lower_node, prefix=prefix+dirname+'/'):
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

def get_most_likely_tree(node):
    """
    Returns get_trees, passed through a small filtering step
    """
    trees = get_trees(node)
    return _select_most_likely_tree(trees)

def _count_entries_root(tfile, tree='auto'):
    if tree == 'auto':
        trees = _get_trees_recursively_root(tfile)
        if len(trees) == 0:
            logger.error('No TTrees found in %s', tfile)
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

def split_rootfile(rootfile, dst='.', n_chunks=None, chunk_size=None, tree='auto'):
    """
    Splits a rootfile into new rootfiles.
    Either n_chunks or chunk_size *must* be specified.
    chunk_size is treated as number of events.
    dst is treated as a directory, unless it contains a substring '%i',
    in which case it is treated as a path to a file and %i is increased
    per splitted part.
    """
    if n_chunks and chunk_size:
        raise ValueError('Specify *either* n_chunks *or* chunk_size, not both')
    # Root files will have to be opened for copying, so no point in using the cache
    with nocache():
        with open_root(rootfile) as src_tfile:
            if tree == 'auto': tree = get_most_likely_tree(src_tfile)
            src_tree = src_tfile.Get(tree)
            nentries = src_tree.GetEntries()
            # Some math to make sensible chunks
            if not n_chunks: n_chunks = int(math.ceil(float(nentries) / chunk_size))
            chunks = _get_chunkify_nentries(nentries, n_chunks)
            logger.debug(
                'Splitting %s into %s chunks (chunk_size ~%s)',
                rootfile, n_chunks, chunks[0][1]-chunks[0][0]
                )
            for i, (first, last) in enumerate(chunks):
                if '%i' in dst:
                    dst_chunk = dst.replace('%i', str(i))
                else:
                    base, ext = osp.splitext(rootfile)
                    dst_chunk = osp.join(dst, base + '_' + str(i) + ext)
                logger.info('Splitting %s --> %s,%s %s', rootfile, first, last, dst_chunk)
                # Perform actual copying
                with open_root(dst_chunk, 'recreate') as dst_tfile:
                    _take_chunk_of_rootfile(src_tree, dst_tfile, first, last)

def make_chunk_rootfile(rootfile, first, last, dst=None, tree='auto'):
    """
    Makes a copy of the events in rootfile between first and last
    in a new file dst.
    """
    if dst is None:
        base, ext = osp.splitext(rootfile)
        dst = '{base}_{first}-{last}{ext}'.format(
            base=base, first=first, last=last, ext=ext
            )
    with nocache():
        with open_root(rootfile) as src_tfile:
            if tree == 'auto': tree = get_most_likely_tree(src_tfile)
            src_tree = src_tfile.Get(tree)
            nentries = src_tree.GetEntries()
            if last > nentries:
                raise ValueError(
                    'Requested last entry {} for {} has {} entries'
                    .format(last, rootfile, nentries)
                    )
            logger.debug('Taking chunk %s-%s of %s into %s', first, last, rootfile, dst)
            with open_root(dst, 'recreate') as dst_tfile:
                _take_chunk_of_rootfile(src_tree, dst_tfile, first, last)

def _take_chunk_of_rootfile(src_tree, dst_tfile, first, last):
    """
    Given a src TTree and a dst TFile, copies events between first
    and last of src_tree into dst_tfile
    """
    n_copy = last - first + 1
    dst_tfile.cd()
    dst_tree = src_tree
    dst_tree = dst_tree.CopyTree('', '', n_copy, first)
    dst_tree.Write()

def _get_chunkify_nentries(nentries, n_chunks):
    """
    Makes n_chunks chunks out of a nentries.
    Chunk size may be variable sized if nentries is not divisable by n_chunks
    May yield empty lists (i.e. first==last) if n_chunks > nentries.
    Yields the first and last entry per chunk
    """
    chunk_size_float = float(nentries) / n_chunks
    return [ (math.floor(i*chunk_size_float), math.floor((i+1)*chunk_size_float)-1) for i in range(n_chunks) ]

def iter_chunkify_rootfiles_by_entries(rootfiles, chunk_size):
    """
    Function that yields lists of (path.root, n_take, first, last), so that the number
    of events in a list is equal to chunk_size
    """
    if not len(rootfiles): raise StopIteration
    # Set up iterator for rootfiles and initialize
    rootfilesiter = iter(rootfiles)
    rootfile = next(rootfilesiter)
    nentries = count_entries(rootfile)
    first = 0
    # Set up the chunk
    chunk = []
    n_still_needed = chunk_size
    while True:
        n_take = min(n_still_needed, nentries)
        nentries -= n_take
        logger.debug(
            'Taking %s entries from %s (%s - %s); %s remaining',
            n_take, rootfile, first, first+n_take-1, nentries
            )
        chunk.append((rootfile, first, first+n_take-1))
        n_still_needed -= n_take
        first += n_take
        if n_still_needed == 0:
            # Chunk is full, yield it and reset chunk and n_still_needed
            yield chunk
            chunk = []
            n_still_needed = chunk_size
        if nentries == 0:
            # Try to get the next rootfile, yield remaining chunk if out of root files
            try:
                rootfile = next(rootfilesiter)
                nentries = count_entries(rootfile)
                first = 0
            except StopIteration:
                if chunk: yield chunk
                break

def hadd_chunk_entries(chunk, dst, file_split_fn=make_chunk_rootfile, tree='auto'):
    """
    Takes a chunk as outputted from iter_chunkify_rootfiles_by_entries
    (list of (rootfile, first, last)), and merges it into
    one rootfile at path dst
    """
    tmpdir = 'merging-{0}'.format(uuid.uuid4())
    os.makedirs(tmpdir)
    try:
        # First created the splitted rootfiles
        splitted_rootfiles = []
        for i, (rootfile, first, last) in enumerate(chunk):
            splitted_rootfile = osp.join(tmpdir, 'part{}.root'.format(i))
            file_split_fn(
                rootfile, first, last,
                dst=splitted_rootfile, tree=tree
                )
            splitted_rootfiles.append(splitted_rootfile)
        # Hadd the splitted rootfiles
        hadd_chunks(splitted_rootfiles, dst, tmpdir=tmpdir)
    finally:
        shutil.rmtree(tmpdir)
