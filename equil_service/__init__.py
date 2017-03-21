"""Equil Service module"""
import sys
import os
import contextlib
try:
    import matlab
    import matlab.engine
except ImportError as ex:
    print(ex)
    print('Cannot import MATLAB engine, using EquilStarter.exe')
    import subprocess
import settings as S

__path__ = os.path.dirname(os.path.realpath(__file__))

def run_equilibrium(matfile_in):
    """start equilibrium calculation"""
    if 'matlab' in sys.modules:
        with get_loader(S.EQUILIBRIUM_PATH):
            eng = matlab.engine.start_matlab()
            eng.cd(S.EQUILIBRIUM_PATH, nargout=0)
            if eng.fn_Run(2, matfile_in, nargout=1):
                raise Exception('Equilbrium error!')
            eng.fn_Run(3, nargout=1)
            eng.quit()
    else:
        with get_loader(os.getcwd()):
            equil_starter = os.path.join(__path__, 'EquilStarter.exe')
            subprocess.call([equil_starter, '2', matfile_in])
            subprocess.call([equil_starter, '3'])

@contextlib.contextmanager
def get_loader(pathname):
    """dynamically create loader_out.config"""
    template_path = os.path.join(__path__, 'loader_template.config')
    with open(template_path, 'r') as _fs:
        config = _fs.read()
    filename = os.path.join(pathname, 'loader_out.config')
    with open(filename, 'w') as _fs:
        _fs.write(config.format(login=S._login, pwd=S._password, db_name=S._ora_base))
    try:
        yield
    except Exception:
        raise
    finally:
        os.remove(filename)
