# seutils - Storage element utilities

`seutils` is a package that aims to make HEP storage element operations via Python easier. It relies on the availability of storage element command line tools such as [XRootD](https://xrootd.slac.stanford.edu/), [gfal2-util](https://github.com/cern-fts/gfal2-util), or [eos](https://github.com/cern-eos/eos). `seutils` has no python dependencies other than the standard library, and is compatible from Python 3.10 down to Python 2.7.5.


## Installation

For ordinary use:

```
pip install seutils
```

(or `python3 -m pip install seutils`).

For development/testing:

```
git clone git@github.com:tklijnsma/seutils.git
cd seutils
pip install -e .
pip install pytest
pytest test
# To run the integration tests (requires write access to root://cmseos.fnal.gov/):
pytest test --integration
```


## About this package

Most type of environments (e.g. CMSSW, most default interactive node environments) come with either `XRootD` or `gfal` pre-installed. The need to perform file operations within Python code often arises, and frequently we resort to something like `os.system('xrdcp some.file root://foo.bar.edu//store/')`. If everything works smoothly this does the trick, but if anything inside the `os.system` call fails, some problems arise:

- Typically failures of `os.system` are not detected, so no exception is thrown
- Even if a failure is detected, it is probably not clear what exactly went wrong
- Some storage elements do not support `xrdcp` but only work with `gfal-copy`; non-robust if-else code quickly grows around the `os.system` call
- Formatting the paths can be a pain

`seutils` aims to solve most of these problems with a simple interface, and adds a bunch of utilities.

The `XRootD` python bindings will be supported in a future release. On most interactive nodes, using these bindings typically requires hacking the `$PATH` variable, and is not very reliable.


## Examples

A more complete documentation is in development.


### Path manipulations

Splitting off and joining the protocol+server part and the filename part:

```
>>> seutils.path.split_mgm('root://foo.bar.gov//foo/bar/file.txt')
('root://foo.bar.gov', '/foo/bar/file.txt')
>>> 
>>> seutils.path.join_mgm('root://foo.bar.gov', '/foo/bar/file.txt')
'root://foo.bar.gov//foo/bar/file.txt'
```

Consistent treatment of paths with a protocol+server, aligned with `os.path`'s behavior:

```
>>> seutils.path.dirname('root://foo.bar.gov//foo/bar')
'root://foo.bar.gov//foo'
>>>
>>> seutils.path.dirname('root://foo.bar.gov//foo/')
'root://foo.bar.gov//'
>>>
>>> seutils.path.dirname('root://foo.bar.gov//')
'root://foo.bar.gov//'
```

Works with `.` and `..`:

```
>>> seutils.path.normpath('root://foo.bar.gov//foo/../foo/bar/./../bar/')
'root://foo.bar.gov//foo/bar/'
>>>
>>> seutils.path.dirname('root://foo.bar.gov//foo/../foo/bar/./../bar/')
'root://foo.bar.gov//foo'
```

And some other utilities:

```
>>> seutils.path.relpath('root://foo.bar.gov//foo/bar/test.file', 'root://foo.bar.gov//foo/')
'bar/test.file'
>>>
>>> list(seutils.path.iter_parent_dirs('root://foo.bar.gov//foo/bar/test.file'))
['root://foo.bar.gov//foo/bar', 'root://foo.bar.gov//foo', 'root://foo.bar.gov//']
>>>
>>> seutils.path.get_protocol('root://foo.bar.gov//foo/bar/test.file')
'root'
```


### Storage element operations

```
# Available paths:
# root://foo.bar.gov//store/user/test.file (contents: 'testcontent')
# root://foo.bar.gov//store/user/other.file
# root://foo.bar.gov//store/user/testdir
# root://foo.bar.gov//store/user/testdir/file.file

>>> node = seutils.stat('root://foo.bar.gov//store/user/test.file')
>>> node
<seutils.Inode root://foo.bar.gov//store/user/test.file at 0x7fdcc5ae75e0>
>>> node.path
'root://foo.bar.gov//store/user/test.file'
>>> node.size
26991000000
>>> node.size_human
'25.1 Gb'

>>> seutils.ls('root://foo.bar.gov//store/user/')
['root://foo.bar.gov//store/user/test.file', 'root://foo.bar.gov//store/user/other.file', 'root://foo.bar.gov//store/user/testdir']

>>> seutils.ls('root://foo.bar.gov//store/user/', stat=True)
[<seutils.Inode root://foo.bar.gov//store/user/test.file at 0x7fdcc5ae7340>, <seutils.Inode root://foo...user/other.file at 0x7fdcc5b1ef70>, <seutils.Inode root://foo.bar.gov//store/user/testdir at 0x7fdcc5b1edc0>]

>>> seutils.cat('root://foo.bar.gov//store/user/test.file')
'testcontent'

>>> seutils.isdir('root://foo.bar.gov//store/user/test.file')
False

>>> seutils.exists('root://foo.bar.gov//store/user/doesnotexist.file')
False

>>> seutils.put('root://foo.bar.gov//store/user/new.file', contents='some content')
>>> seutils.cat('root://foo.bar.gov//store/user/new.file')
'some content'
```

Wildcards are supported:

```
>>> seutils.ls_wildcard('root://foo.bar.gov//store/*/t*')
['root://foo.bar.gov//store/user/test.file', 'root://foo.bar.gov//store/user/testdir']
```

An `os.walk` equivalent is also available:

```
>>> for current_directory, directories, files in seutils.walk('root://foo.bar.gov//store'):
...   print(current_directory, directories, files)
...
root://foo.bar.gov//store ['root://foo.bar.gov//store/user'] []
root://foo.bar.gov//store/user ['root://foo.bar.gov//store/user/testdir'] ['root://foo.bar.gov//store/user/other.file', 'root://foo.bar.gov//store/user/test.file']
root://foo.bar.gov//store/user/testdir [] ['root://foo.bar.gov//store/user/testdir/file.file']
```

For wildcard and walk operations the number of storage element requests can quickly run out of hand. A default cut-off is set at a recursion depth of 20. This can be increased by setting `seutils.MAX_RECURSION_DEPTH` to higher number.

All utilities listed above take a keyword argument `implementation='...'`, where the value may be `None`, `'xrd'`, or `'gfal'`.


### Command line utilities

`seutils` comes with a number of command line utilities: `seu-install-completion`, `seu-version`, `seu-ls`, `seu-du`, `seu-rm`, `seu-mkdir`, and `seu-cat`. Please use the `--help` flag to print their usage instructions.

If the command line completion is installed (see `seu-install-completion`), some basic tab completion is available:

```
$ seu-ls root://foo.bar.gov//store/u
<tab>
$ seu-ls root://foo.bar.gov//store/user
<tab>
$ seu-ls root://foo.bar.gov//store/user/
<tab>
$ seu-ls root://foo.bar.gov//store/user/
 root://foo.bar.gov//store/user/test.file  root://foo.bar.gov//store/user/other.file  root://foo.bar.gov//store/user/testdir
$ seutils$ seu-ls root://foo.bar.gov//store/user/t
<tab>
$ seu-ls root://foo.bar.gov//store/user/test
<tab>
$ seu-ls root://foo.bar.gov//store/user/test
 root://foo.bar.gov//store/user/test.file  root://foo.bar.gov//store/user/testdir
```

Some commands also accept wildcards:

```
$ seu-ls root://foo.bar.gov//store/user/test*
root://foo.bar.gov//store/user/test.file
root://foo.bar.gov//store/user/testdir

$ seu-ls -l root://foo.bar.gov//store/user/test*
2020-12-08 03:23  1.6 Gb    root://foo.bar.gov//store/user/test.file
2020-08-17 15:38  305.0 b   root://foo.bar.gov//store/user/testdir
```


### ROOT file utilities

As high energy physicists frequently have to work with ROOT files, `seutils` includes some basic utilities to quickly get information about ROOT files. The following command lines tools require that `uproot` (version 3 or 4) is installed.

```
$ seu-root-ls -b root://foo.bar.gov//store/user/my/file/rootfile.root
/
  TreeMaker2
    PreSelection (tree, 2 entries)
      RunNum
      LumiBlockNum
      EvtNum
      BadChargedCandidateFilter
```

`seu-root-ls` works on both remote and local files.
