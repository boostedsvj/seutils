import os, os.path as osp
import shlex
import subprocess

def bash_complete(comp_line, comp_exe, default_mgm=None):
    """
    This is an approximation of how bash's complete function generates matches.
    ``comp_line`` Is the line assumed to be at the terminal.
    ``comp_exe`` is the path to an executable file that will generate matches.
    Returns the stdout of comp_exe after running with parameters normally
    supplied by complete.
    """
    cmd_line_args = shlex.split(comp_line)
    cmd = cmd_line_args[0]
    comp_point = str(len(comp_line))
    curr_word = ""
    prev_word = cmd
    if comp_line.endswith(" "):
        curr_word = ""
        prev_word = cmd_line_args[-1]
    else:
        curr_word = cmd_line_args[-1]
        prev_word = cmd_line_args[-2] if len(cmd_line_args) >= 2 else ""
    os.environ.update({"COMP_LINE": comp_line, "COMP_POINT": comp_point, 'COMPLETION_TEST_MODE': '1'})
    if default_mgm is not None: os.environ['SEU_DEFAULT_MGM'] = default_mgm
    output = seutils.run_command(
        [comp_exe, cmd, curr_word, prev_word],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    return [l.strip() for l in output]


test_dir = osp.dirname(osp.abspath(__file__))

def test_heylook():
    py_completer_file = osp.join(test_dir, 'heylook_completer.py')
    assert bash_complete('heylook ', py_completer_file) == ["a", "bunch", "of", "potential", "matches"]
    assert bash_complete('heylook b', py_completer_file) == ['bunch']

import seutils
completion_file = seutils.__file__.replace('__init__', 'completion').replace('.pyc', '.py')

def test_longest_matching():
    from seutils.completion import find_longest_matching_start
    assert find_longest_matching_start(['aaa', 'aaa']) == 'aaa'
    assert find_longest_matching_start(['aab', 'aaa']) == 'aa'
    assert find_longest_matching_start(['abb', 'aaa']) == 'a'
    assert find_longest_matching_start(['bb', 'aaa']) == ''
    assert find_longest_matching_start(['', 'aaa']) == ''
    assert find_longest_matching_start(['aaa']) == 'aaa'


def bash_complete2(line, default_mgm='root://foo.bar.gov/'):
    """Thin wrapper around `bash_complete` that fills in some default info for testing"""
    return bash_complete(line, completion_file, default_mgm)


def test_basic_path_completion():
    assert bash_complete2('seu-ls root://foo.bar.gov//store/user/t') == ['//foo.bar.gov//store/user/test',]
    assert bash_complete2('seu-ls root://foo.bar.gov//store/user/test') == [
        'root://foo.bar.gov//store/user/test.file',
        'root://foo.bar.gov//store/user/testdir'
        ]
    assert bash_complete2('seu-ls root://foo.bar.gov//store/user/testd') == ['//foo.bar.gov//store/user/testdir/']


def test_completes_default_mgm():
    assert bash_complete2('seu-ls ') == ['root://foo.bar.gov//store/']
    assert bash_complete2('seu-ls r') == ['root://foo.bar.gov//store/']
    assert bash_complete2('seu-ls root://') == ['//foo.bar.gov//store/']
    assert bash_complete2('seu-ls root://foo.bar.gov/') == ['//foo.bar.gov//store/']
    assert bash_complete2('seu-ls root://foo.bar.gov//') == ['//foo.bar.gov//store/']
    assert bash_complete2('seu-ls root://foo.bar.gov//store') == ['//foo.bar.gov//store/']
    assert bash_complete2('seu-ls root://foo.bar.gov//store/') == ['//foo.bar.gov//store/user/']

    # Cases with . and .. in the path
    assert bash_complete2('seu-ls root://foo.bar.gov//store/.') == ['//foo.bar.gov//store/']
    assert bash_complete2('seu-ls root://foo.bar.gov//store/..') == ['//foo.bar.gov//store/']
    assert bash_complete2('seu-ls root://foo.bar.gov//store/../store/./') == ['//foo.bar.gov//store/user/']

    # Cases with no default mgms registered
    assert bash_complete2('seu-ls r', '') == []
    assert bash_complete2('seu-ls root://', '') == []

    default_mgms = 'root://foo.bar.gov,gfal://foo.bar.edu,gfal://foo.bob.edu'
    assert bash_complete2('seu-ls ', default_mgms) == []
    assert bash_complete2('seu-ls r', default_mgms) == ['root://foo.bar.gov//store/']
    assert bash_complete2('seu-ls g', default_mgms) == ['gfal://foo.b']
    assert bash_complete2('seu-ls gfal://f', default_mgms) == ['//foo.b']
    assert bash_complete2('seu-ls gfal://foo.b', default_mgms) == [
        'gfal://foo.bar.edu//store/', 'gfal://foo.bob.edu//store/'
        ]