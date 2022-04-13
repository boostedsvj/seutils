import seutils


class Implementation:

    def __init__(self):
        self._is_installed = None

    def is_installed(self):
        if self._is_installed is None:
            self._is_installed = self.check_is_installed()
        return self._is_installed

    def check_is_installed(self):
        raise NotImplementedError

    def get(self, rootfile, key):
        raise NotImplementedError

    def ls(self, rootfile):
        raise NotImplementedError

    def trees(self, rootfile):
        raise NotImplementedError

    def branches(self, tree):
        raise NotImplementedError

    def nentries(self, tree):
        raise NotImplementedError

    def is_node(self, f):
        raise NotImplementedError

    def is_ttree(self, f):
        raise NotImplementedError


from .uproot_implementation import UprootImplementation, Uproot3Implementation

uproot = UprootImplementation()
uproot3 = Uproot3Implementation()
implementations = dict(uproot=uproot, uproot3=uproot3)


def get_implementation(implementation_name):
    """
    Returns an implementation instance corresponding to the passed name.
    Returns None if `implementation_name` is 'auto' or None.
    """
    if implementation_name in ['auto', None]:
        return None
    return implementations[implementation_name]


def best_implementation(cmd_name, path=None):
    """
    Given a command name, returns an installed implementation that has this command
    """
    for implementation in implementations.values():
        if implementation.is_installed() and hasattr(implementation, cmd_name):
            seutils.logger.info(
                'Using implementation %s to execute \'%s\' (path: %s)',
                implementation.__class__.__name__, cmd_name, path
                )
            return implementation
    raise Exception('No installed implementation found for cmd {0}, path {1}'.format(cmd_name, path))


def make_global_scope_command(cmd_name):
    """
    Creates a global scope command in case the user does not care about the
    underlying implementation.
    """
    def wrapper(path, *args, **kwargs):
        implementation = kwargs.pop('implementation', None)
        if implementation is None:
            implementation = best_implementation(cmd_name, path)
        elif seutils.is_string(implementation):
            implementation = implementations[implementation]
        return getattr(implementation, cmd_name)(path, *args, **kwargs)
    return wrapper

get = make_global_scope_command('get')
ls = make_global_scope_command('ls')
trees = make_global_scope_command('trees')
branches = make_global_scope_command('branches')
nentries = make_global_scope_command('nentries')
is_node = make_global_scope_command('is_node')
is_ttree = make_global_scope_command('is_ttree')
