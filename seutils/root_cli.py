import seutils
from . import root
import argparse, os.path as osp

class Parser(object):
    """
    Very thin wrapper class for argparse.ArgumentParser with some options
    used for most root command line tools in seutils
    """
    def __init__(self, *args, **kwargs):
        self.parser = argparse.ArgumentParser(*args, **kwargs)
        self.add_argument('-v', '--verbose', action='store_true', help='Increases verbosity')
        self.use_implementation = kwargs.get('implementation', True)
        if self.use_implementation:
            choices = list(root.implementations.keys())
            choices.sort()
            choices.insert(0, 'auto')
            self.add_argument(
                '-i', '--implementation', type=str,
                help='Implementation to be used (choices: {0})'.format(', '.join(choices)),
                choices=choices
                )

    def add_argument(self, *args, **kwargs):
        self.parser.add_argument(*args, **kwargs)

    def parse_args(self, *args, **kwargs):
        parsed_args = self.parser.parse_args(*args, **kwargs)
        if parsed_args.verbose: seutils.debug()
        if self.use_implementation:
            parsed_args.implementation = root.get_implementation(parsed_args.implementation)
        else:
            parsed_args.implementation = None
        return parsed_args


class ParserSinglePath(Parser):
    def __init__(self, *args, **kwargs):
        super(ParserSinglePath, self).__init__(*args, **kwargs)
        self.add_argument('path', type=str, help='Path (local or remote)')

    def parse_args(self, *args, **kwargs):
        parsed_args = super(ParserSinglePath, self).parse_args(*args, **kwargs)
        # if not seutils.path.has_protocol(parsed_args.path):
        #     raise TypeError('Path {0} is not remote'.format(parsed_args.path))
        if '*' in parsed_args.path:
            raise TypeError('Path {0}: Wildcards not accepted'.format(parsed_args.path))
        return parsed_args


class ParserMultipleRemotePaths(Parser):
    def __init__(self, *args, **kwargs):
        super(ParserMultipleRemotePaths, self).__init__(*args, **kwargs)
        self.add_argument('paths', type=str, nargs='*', help='Paths (must be remote)')

    def parse_args(self, *args, **kwargs):
        expand_wildcards = kwargs.pop('expand_wildcards', True)
        allow_zero_paths = kwargs.pop('allow_zero_paths', False)
        disallow_wildcards = kwargs.pop('disallow_wildcards', False)
        parsed_args = super(ParserMultipleRemotePaths, self).parse_args(*args, **kwargs)
        paths = []
        for path in parsed_args.paths:
            if not seutils.path.has_protocol(path):
                raise TypeError('Path {0} is not remote'.format(path))
            if disallow_wildcards and '*' in path:
                raise TypeError('Wildcards not allowed')
            if expand_wildcards:
                paths.extend(seutils.ls_wildcard(path, implementation=parsed_args.implementation))
            else:
                paths.append(path)
        parsed_args.paths = paths
        if not allow_zero_paths and len(paths) == 0:
            raise TypeError('Pass at least one path')
        return parsed_args


def root_ls():
    parser = ParserSinglePath()
    parser.add_argument('-b', '--branches', action='store_true', help='Also print branches for TTree\'s')
    args = parser.parse_args()
    contents = root.ls(args.path, implementation=args.implementation)
    for name, node in contents:
        is_tree = root.is_ttree(node, implementation=args.implementation)
        s = '  '*node.____depth + (name if name=='/' else osp.basename(name))
        if is_tree: s += ' (tree, {} entries)'.format(root.nentries(node, implementation=args.implementation))
        print(s)
        if is_tree and args.branches:
            for branch_name, _ in root.branches(node, implementation=args.implementation):
                print('  '*(node.____depth+1) + branch_name)


def root_count():
    parser = ParserMultipleRemotePaths()
    parser.add_argument('-l', '--detailed', action='store_true', help='Print also counts per file')
    args = parser.parse_args()

    from collections import OrderedDict
    tree_counts = OrderedDict()
    tree_files = OrderedDict()

    # Collect the counts per tree
    for path in args.paths:
        for name, node in root.ls(path, implementation=args.implementation):
            if not root.is_ttree(node, implementation=args.implementation): continue
            tree_counts.setdefault(name, [])
            tree_counts[name].append(root.nentries(node, implementation=args.implementation))
            tree_files.setdefault(name, [])
            tree_files[name].append(path)

    # Print
    for name, counts in tree_counts.items():
        n_files = len(counts)
        total = sum(counts)
        mean = float(total) / n_files
        print('{3}: {0:.0f} ({1:.2f} per file, {2} files)'.format(total, mean, n_files, name))

        if args.detailed:
            for path, count in zip(tree_files[name], counts):
                print('{0:7.0f} {1}'.format(count, path))
