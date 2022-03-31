from __future__ import print_function
import seutils, os, argparse

class Parser(object):
    """
    Very thin wrapper class for argparse.ArgumentParser with some options
    used for every command line tool in seutils
    """
    def __init__(self, *args, **kwargs):
        self.parser = argparse.ArgumentParser(*args, **kwargs)
        self.add_argument('-v', '--verbose', action='store_true', help='Increases verbosity')
        self.add_argument('-d', '--dry', action='store_true', help='Does not actually run any commands')
        self.add_argument('-f', '--fake', action='store_true', help='Use a fake filesystem instead')
        self.use_implementation = kwargs.get('implementation', True)
        if self.use_implementation:
            choices = list(seutils.implementations.keys())
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
        if parsed_args.dry: seutils.drymode()
        if self.use_implementation:
            parsed_args.implementation = seutils.get_implementation(parsed_args.implementation)
        else:
            parsed_args.implementation = None
        return parsed_args


class ParserSingleRemotePath(Parser):
    def __init__(self, *args, **kwargs):
        super(ParserSingleRemotePath, self).__init__(*args, **kwargs)
        self.add_argument('path', type=str, help='Path (must be remote)')

    def parse_args(self, *args, **kwargs):
        parsed_args = super(ParserSingleRemotePath, self).parse_args(*args, **kwargs)
        if not seutils.path.has_protocol(parsed_args.path):
            raise TypeError('Path {0} is not remote'.format(parsed_args.path))
        elif '*' in parsed_args.path:
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


# ________________________________________________________
# Command line tool implementations

def version():
    print(seutils.version())

def cat():
    args = ParserMultipleRemotePaths().parse_args()
    for path in args.paths:
        print(seutils.cat(path, implementation=args.implementation))

def ls():
    parser = ParserMultipleRemotePaths()
    parser.add_argument('-l', '--long', action='store_true', help='Include mtime and size in the output')
    parser.add_argument(
        '-s', '--sort', type=str, choices=['name', 'date', 'size'], default='name',
        help='Only works if -l is passed as well. Possibility to sort the output by date or size as well'
        )
    args = parser.parse_args(expand_wildcards=False)

    if not args.long and args.sort != 'name':
        seutils.logger.warning('Option --sort ignored (use --long as well)')

    if not args.paths: args.paths = ['']
    for path in args.paths:
        if '*' in path:
            output = seutils.ls_wildcard(path, stat=args.long, implementation=args.implementation)
        else:
            output = seutils.ls(path, stat=args.long, implementation=args.implementation)
        if args.long:
            if args.sort == 'date':
                output.sort(key=lambda inode: inode.modtime, reverse=True)
            elif args.sort == 'size':
                output.sort(key=lambda inode: -inode.size)
            for inode in output:
                print(
                    '{}  {:<8}  {}'
                    .format(
                        inode.modtime.strftime('%Y-%m-%d %H:%M'),
                        inode.size_human,
                        inode.path
                        )
                    )
        else:
            print('\n'.join(output))

def du():
    parser = ParserMultipleRemotePaths()
    parser.add_argument('-s', '--sort', action='store_true', help='Sorts by size instead (default is by name)')
    args = parser.parse_args(expand_wildcards=False)
    for path in args.paths:
        inodes = seutils.ls_wildcard(path, stat=True, implementation=args.implementation)
        if args.sort: inodes.sort(key=lambda inode: -inode.size)
        for inode in inodes:
            print('{0:<8} {1}'.format(inode.size_human, inode.path))

# Bind python3 "raw_input" to input, for python 2/3 compatibility
try:
    input = raw_input
except NameError:
    pass

def rm():
    parser = ParserMultipleRemotePaths()
    parser.add_argument('-y', action='store_true', help='Skip user verification')
    parser.add_argument('-r', action='store_true', help='Recursive remove (required for directories)')
    args = parser.parse_args()

    for path in args.paths:
        if not args.y:
            do_continue = False
            while True:
                answer = input('rm {}{} [y/n]? '.format('-r ' if args.r else '', path)).lower()
                if answer == 'y':
                    break
                elif answer == 'n':
                    do_continue = True
                    break
            if do_continue: continue
        seutils.rm(path, recursive=args.r, implementation=args.implementation)

def mkdir():
    args = ParserMultipleRemotePaths().parse_args(disallow_wildcards=True, expand_wildcards=False)
    for path in args.paths:
        seutils.mkdir(path, implementation=args.implementation)
