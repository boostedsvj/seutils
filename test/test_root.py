import seutils, pytest, os.path as osp
from pprint import pprint
from seutils.uproot_implementation import UprootImplementation, Uproot3Implementation

test_mz250 = osp.join(osp.dirname(__file__), 'test_mz250.root')
test_cmssw_sim = osp.join(osp.dirname(__file__), 'test_cmssw_sim.root')

@pytest.fixture
def impl(request):
    yield request.getfixturevalue(request.param)

@pytest.fixture
def uproot_impl():
    return UprootImplementation()

@pytest.fixture
def uproot3_impl():
    return Uproot3Implementation()

@pytest.fixture
def globalscope_impl():
    return seutils.root


implementations = ['uproot_impl', 'globalscope_impl']
# implementations = ['uproot3_impl']


@pytest.mark.parametrize('impl', implementations, indirect=True)
def test_ls(impl):
    names = [n[0] for n in impl.ls(test_mz250)]
    assert names == ['/', '/TreeMaker2', '/TreeMaker2/PreSelection']
    names = [n[0] for n in impl.ls(test_cmssw_sim)]
    assert names == ['/', '/MetaData', '/ParameterSets', '/Parentage', '/Events', '/LuminosityBlocks', '/Runs']
    names = [n[0] for n in impl.trees(test_mz250)]
    assert names == ['/TreeMaker2/PreSelection']

@pytest.mark.parametrize('impl', implementations, indirect=True)
def test_branches(impl):
    tree = impl.get(test_mz250, '/TreeMaker2/PreSelection')
    branches_names = [b[0] for b in impl.branches(tree)]
    assert branches_names[0] == 'RunNum'
    assert impl.nentries(tree) == 2
    # cmssw tree
    tree = impl.get(test_cmssw_sim, '/Events')
    branches_names = [b[0] for b in impl.branches(tree)]
    assert branches_names[0] == 'EventAuxiliary'
    assert impl.nentries(tree) == 10
