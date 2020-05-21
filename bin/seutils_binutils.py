import os, seutils

MGM_ENV_KEY = 'SEU_DEFAULT_MGM'

def update_default_mgm(mgm):
    if MGM_ENV_KEY in os.environ:
        seutils.logger.warning(
            'Setting default mgm to %s (previously: %s)',
            mgm, os.environ[MGM_ENV_KEY]
            )
    else:
        seutils.logger.warning('Setting default mgm to %s', mgm)
    os.environ[MGM_ENV_KEY] = mgm

def detect_fnal():
    mgm = None
    if os.uname()[1].endswith('.fnal.gov'):
        mgm = 'root://cmseos.fnal.gov'
        seutils.logger.warning('Detected fnal.gov host; using mgm %s', mgm)
    return mgm

def flexible_format(lfn, mgm=None):
    if not lfn.startswith('root:') and not lfn.startswith('/'):
        try:
            prefix = '/store/user/' + os.environ['USER']
            seutils.logger.warning('Pre-fixing %s', prefix)
            lfn = os.path.join(prefix, lfn)
        except KeyError:
            pass
    if lfn.startswith('root:'):
        return seutils.format(lfn)
    else:
        return seutils.format(lfn, mgm)
