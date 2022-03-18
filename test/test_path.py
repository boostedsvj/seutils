import seutils, pytest
import seutils.path as seup

def test_normpath():
    assert seup.normpath('/foo/bar') == '/foo/bar'
    assert seup.normpath('/foo/bar/') == '/foo/bar/'
    assert seup.normpath('root://foo.bar.gov//foo/bar') == 'root://foo.bar.gov//foo/bar'
    assert seup.normpath('root://foo.bar.gov//foo/bar/') == 'root://foo.bar.gov//foo/bar/'
    assert seup.normpath('root://foo.bar.gov//') == 'root://foo.bar.gov//'
    assert seup.normpath('root://foo.bar.gov//./foo/../') == 'root://foo.bar.gov//'
    assert seup.normpath('/foo/.//bar/../.') == '/foo'
    assert seup.normpath('root://foo.bar.gov//foo/.//bar/../.') == 'root://foo.bar.gov//foo'
    
def test_split_mgm():
    path = 'root://foo.bar.gov//foo/bar'
    assert seup.split_mgm(path) == ('root://foo.bar.gov', '/foo/bar')
    assert seup.split_mgm('root://foo.bar.gov//') == ('root://foo.bar.gov', '/')
    with pytest.raises(ValueError):
        seup.split_mgm('root://foo.bar.gov/')

def test_dirname():
    assert seup.dirname('/foo/bar') == '/foo'
    assert seup.dirname('root://foo.bar.gov//foo/bar') == 'root://foo.bar.gov//foo'
    assert seup.dirname('root://foo.bar.gov//foo/.//bar//.') == 'root://foo.bar.gov//foo'
    assert seup.dirname('root://foo.bar.gov//foo') == 'root://foo.bar.gov//'
    assert seup.dirname('root://foo.bar.gov//') == 'root://foo.bar.gov//'
    with pytest.raises(ValueError):
        seup.split_mgm('root://foo.bar.gov/')

def test_iter_parent_dirs():
    assert list(seup.iter_parent_dirs('/foo/bar')) == ['/foo', '/']
    assert list(seup.iter_parent_dirs('root://foo.bar.gov//foo/bar')) == ['root://foo.bar.gov//foo', 'root://foo.bar.gov//']
    assert list(seup.iter_parent_dirs('root://foo.bar.gov//foo/.//bar//.')) == ['root://foo.bar.gov//foo', 'root://foo.bar.gov//']

def test_inode_equality():
    from datetime import datetime
    left = seutils.Inode('/bla', datetime(2019, 10, 10, 10, 10, 10), True, 1001)
    right = seutils.Inode('/bla', datetime(2019, 10, 10, 10, 10, 10), True, 1001)
    assert left == right
    right.size = 1002
    assert left != right

def test_relpath():
    assert seup.relpath('/foo/bar/bla.txt', '/foo/') == 'bar/bla.txt'
    assert seup.relpath('/foo/bar/bla.txt', '/foo') == 'bar/bla.txt'
    assert seup.relpath('root://foo.bar.gov//foo/bar/bla.txt', 'root://foo.bar.gov//foo/') == 'bar/bla.txt'
    assert seup.relpath('root://foo.bar.gov//foo/bar/bla.txt', 'root://foo.bar.gov//foo') == 'bar/bla.txt'
    with pytest.raises(TypeError):
        seup.relpath('root://foo.bar.gov//foo/bar', '/foo')
    with pytest.raises(TypeError):
        seup.relpath('root://foo.bar.gov//foo/bar', 'gsiftp://foo.bar.edu//foo')

def test_get_depth():
    assert seup.get_depth('root://foo.bar.gov//') == 0
    assert seup.get_depth('root://foo.bar.gov//aaa') == 1
    assert seup.get_depth('root://foo.bar.gov//aaa/') == 2
    assert seup.get_depth('root://foo.bar.gov//aaa/a') == 2
    assert seup.get_depth('root://foo.bar.gov//aaa/a/..') == 1
    assert seup.get_depth('root://foo.bar.gov//aaa/a/../') == 2