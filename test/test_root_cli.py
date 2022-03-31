import pytest, seutils
from test_cli import capturing, sys_argv
from test_root import test_mz250, test_cmssw_sim

implementations = ['uproot']

def capture(argv):
    command = argv[0].replace('seu-', '').replace('-', '_')
    with sys_argv(argv):
        with capturing() as output:
            getattr(seutils.root_cli, command)()    
    return output


@pytest.mark.parametrize('implementation', implementations)
def test_root_ls(implementation):
    output = capture(['seu-root-ls', test_mz250, '-i', implementation])
    print(output)
    assert output == ['/', '  TreeMaker2', '    PreSelection (tree, 2 entries)']