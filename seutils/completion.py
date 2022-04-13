#!/usr/bin/env python
from __future__ import print_function
import os, os.path as osp, fnmatch
import sys, seutils

COMPLETION_TEST_MODE = None
DEFAULT_MGMS = None


def all_equal_ivo(lst):
    """Fast way to check if a list has all the same elements"""
    return not lst or lst.count(lst[0]) == len(lst)


def find_longest_matching_start(strings):
    """Find the longest common starting substring"""
    i_column = 0
    for i_column, column in enumerate(zip(*strings)):
        if not all_equal_ivo(column):
            return strings[0][:i_column]
    else:
        return strings[0][:i_column+1]


def enable_logging(fn):
    """
    Function wrapper that enables writing debug output to `debug_completion.log`
    """
    def wrapper(*args, **kwargs):
        if COMPLETION_TEST_MODE:
            with open('debug_completion.log', 'w') as f:
                def log(message):
                    print(message, file=f)
                kwargs['log'] = log
                return fn(*args, **kwargs)
        else:
            return fn(*args, **kwargs)
    return wrapper


def log(message):
    pass


def seu_ls(cmd, curr_word, prev_word, line, log=log):
    if line.strip() == 'seu-ls':
        # Only the plain command is being expanded
        if len(DEFAULT_MGMS) == 1:
            log('Base command; expanding to the only default mgm %s' % DEFAULT_MGMS[0])
            return DEFAULT_MGMS[0]
        else:
            log('Base command; len(mgms)=%s, cannot expand' % len(DEFAULT_MGMS))
        return
    elif line.endswith(' '):
        log('Already expanded')
        return
    # Get the thing to expand
    path = line.strip().split()[-1]
    return expand_path(path, log=log)


def expand_path(path, log=log):
    log('Path to expand: %s' % path)

    # For debugging only:
    if COMPLETION_TEST_MODE:
        try:
            log('dirname={}'.format(seutils.path.dirname(path)))
            log('lfn={}'.format(seutils.path.get_lfn(path)))
        except Exception:
            pass

    # Do expansion to the registerd default mgms
    if not(seutils.path.is_valid_path(path)):
        log('%s is not valid' % path)
        matching_mgms = [ mgm for mgm in DEFAULT_MGMS if fnmatch.fnmatch(mgm, path+'*') ]
        return format_matches(path, matching_mgms, log=log)

    log('Applying normpath')
    path = seutils.path.normpath(path) #+ ('' if path.endswith('/') else '/')
    log('path=%s' % path)

    if seutils.path.get_depth(path) <= 1:
        log('path=%s has depth <= 1' % path)
        matching_mgms = [ mgm for mgm in DEFAULT_MGMS if fnmatch.fnmatch(mgm, path+'*') ]
        return format_matches(path, matching_mgms, log=log)

    # Do expansion
    log('%s is valid, fetching contents' % path)
    if path.endswith('/'):
        log('Doing seutils.listdir("%s")' % path)
        contents = seutils.listdir(path)
    else:
        log('Doing seutils.ls_wildcard("%s*")' % path)
        contents = seutils.ls_wildcard(path + ('' if path.endswith('*') else '*'))

    return format_matches(path, contents, add_trailing_slash=True, log=log)


def format_matches(curr_word, matches, add_trailing_slash=False, log=log):
    """
    Given the `matches` for the `curr_word`, determines what exactly should be printed.
    """
    if curr_word.strip() == '':
        raise Exception('Do not use function `format_matches` with an empty curr_word')
    elif len(matches) == 0:
        log('No matches to expand for curr_word %s' % curr_word)
        return None
    elif len(matches) == 1:
        match = matches[0]
        log('Single match for curr_word %s: %s ; expanding to it' % (curr_word, match))
        if add_trailing_slash and seutils.isdir(match) and not match.endswith('/'):
            match += '/'
        match = match.split(':',1)[1] if ':' in curr_word else match
        log('Final expansion: %s' % match)
        return match
    else:
        log('Processing the following matches for curr_word %s: %s' % (curr_word, ', '.join(matches)))
        expand_to = find_longest_matching_start(matches)
        log('Longest matching start: %s' % expand_to)
        if expand_to.strip() == curr_word.strip():
            log('curr_word %s already maximally expanded, printing contents' % curr_word)
            return ' ' + '\n'.join(matches)
        else:
            log('Expanding to longest matching start %s' % expand_to)
            expand_to = expand_to.split(':',1)[1] if ':' in curr_word else expand_to
            log(('Final expansion: %s' % expand_to))
            return expand_to


@enable_logging
def completion_hook(cmd, curr_word, prev_word, line, log=log):
    log('The following default mgms were found: %s' % ', '.join(DEFAULT_MGMS))
    result = None
    try:
        log('cmd={}, curr_word={}, prev_word={}, line={}'.format(cmd, curr_word, prev_word, line))
        if cmd == 'seu-ls':
            result = seu_ls(cmd, curr_word, prev_word, line, log=log)
    except Exception:
        log('Exception occured: ')
        import traceback
        log(traceback.format_exc())
        raise 
    log('Result: %s' % result)
    if result is not None: print(result)
            

def activate_fake_internet():
    sys.path.append(osp.join(osp.dirname(osp.dirname(osp.abspath(seutils.__file__))), 'test'))
    import fakefs
    fi = fakefs.FakeInternet()
    fs = fakefs.FakeRemoteFS('root://foo.bar.gov')
    fs.put('/store/user/test.file', isdir=False, content='testcontent')
    fs.put('/store/user/other.file', isdir=False, content='testcontent')
    fs.put('/store/user/testdir', isdir=True)
    fs.put('/store/user/testdir/file.file', isdir=False, content='testcontent')
    fi.fs = {fs.mgm : fs}
    fakefs.activate_command_interception(fi)
    seutils.gfal._is_installed = True


def main():
    seutils.silent()
    global COMPLETION_TEST_MODE
    global DEFAULT_MGMS

    COMPLETION_TEST_MODE = os.environ.get('COMPLETION_TEST_MODE', None) == '1'
    if COMPLETION_TEST_MODE: activate_fake_internet()

    DEFAULT_MGMS = os.environ.get('SEU_DEFAULT_MGM', '').split(',')
    if DEFAULT_MGMS == ['']: DEFAULT_MGMS = []
    DEFAULT_MGMS = [mgm.rstrip('/') + '//store/' for mgm in DEFAULT_MGMS]

    completion_hook(sys.argv[1], sys.argv[2], sys.argv[3], os.environ['COMP_LINE'])

if __name__ == "__main__":
    main()