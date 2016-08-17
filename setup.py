#!/usr/bin/env python

"""
Standard python setup.py file for WMArchive
to build     : python setup.py build
to install   : python setup.py install --prefix=<some dir>
to clean     : python setup.py clean
to build doc : python setup.py doc
to run tests : python setup.py test
"""
from __future__ import print_function
__author__ = "Valentin Kuznetsov"

# system modules
import os
import sys
import shutil
import fnmatch
import subprocess
from unittest import TextTestRunner, TestLoader
from distutils.core import setup
from distutils.cmd import Command
from distutils.command.build_ext import build_ext
from distutils.errors import CCompilerError
from distutils.errors import DistutilsPlatformError, DistutilsExecError
from distutils.core import Extension
from distutils.command.install import INSTALL_SCHEMES

# general settings
sys.path.append(os.path.join(os.getcwd(), 'src/python'))
required_python_version = '2.7'
build_errors = (CCompilerError, DistutilsExecError, DistutilsPlatformError)
if sys.platform == 'win32' and sys.version_info > (2, 6):
   # 2.6's distutils.msvc9 compiler can raise an IOError
   build_errors += (IOError,)

class TestCommand(Command):
    """
    Class to handle unit tests
    """
    user_options = [ ]

    def initialize_options(self):
        """Init method"""
        self._dir = os.getcwd()
        print("PYTHONPATH", os.environ['PYTHONPATH'])

    def finalize_options(self):
        """Finalize method"""
        pass

    def run(self):
        """
        Finds all the tests modules in test/, and runs them.
        """
        tpath = os.path.join(self._dir, 'test/python')
        tests = TestLoader().discover(tpath, pattern='*_t.py')
        t = TextTestRunner(verbosity = 2)
        result = t.run(tests)
        # return a non-zero exit status on failure -- useful in CI
        if not result.wasSuccessful():
            sys.exit(1)

class CleanCommand(Command):
    """
    Class which clean-up all pyc files
    """
    user_options = [ ]

    def initialize_options(self):
        """Init method"""
        self._clean_me = [ ]
        for root, dirs, files in os.walk('.'):
            for fname in files:
                if  fname.endswith('.pyc') or fname. endswith('.py~') or \
                    fname.endswith('.rst~'):
                    self._clean_me.append(os.path.join(root, fname))

    def finalize_options(self):
        """Finalize method"""
        pass

    def run(self):
        """Run method"""
        for clean_me in self._clean_me:
            try:
                os.unlink(clean_me)
            except:
                pass
        # remove build area
        shutil.rmtree('build')

class DocCommand(Command):
    """
    Class which build documentation
    """
    user_options = [ ]

    def initialize_options(self):
        """Init method"""
        pass

    def finalize_options(self):
        """Finalize method"""
        pass

    def run(self):
        """Run method"""
        cdir = os.getcwd()
        os.chdir(os.path.join(cdir, 'doc'))
        if  'PYTHONPATH' in os.environ:
            os.environ['PYTHONPATH'] = os.path.join(cdir, 'src/python') \
                + ':' + os.environ['PYTHONPATH']
        else:
            os.environ['PYTHONPATH'] = os.path.join(cdir, 'src/python')
        subprocess.call('make html', shell=True)
        os.chdir(cdir)
        try:
            os.makedirs('build')
        except OSError:
            pass
        for name in os.listdir('doc/build'):
            if  os.path.exists('build/%s' % name):
                shutil.rmtree('build/%s' % name)
            shutil.move('doc/build/%s' % name, 'build')
        os.rmdir('doc/build')

def dirwalk(relativedir):
    """
    Walk a directory tree and look-up for __init__.py files.
    If found yield those dirs. Code based on
    http://code.activestate.com/recipes/105873-walk-a-directory-tree-using-a-generator/
    """
    dir = os.path.join(os.getcwd(), relativedir)
    for fname in os.listdir(dir):
        fullpath = os.path.join(dir, fname)
        if  os.path.isdir(fullpath) and not os.path.islink(fullpath):
            for subdir in dirwalk(fullpath):  # recurse into subdir
                yield subdir
        else:
            initdir, initfile = os.path.split(fullpath)
            if  initfile == '__init__.py':
                yield initdir

def find_packages(relativedir):
    packages = [] 
    for dir in dirwalk(relativedir):
        package = dir.replace(os.getcwd() + '/', '')
        package = package.replace(relativedir + '/', '')
        package = package.replace('/', '.')
        packages.append(package)
    return packages

def datafiles(dir, pattern=None):
    """Return list of data files in provided relative dir"""
    files = []
    for dirname, dirnames, filenames in os.walk(dir):
        for subdirname in dirnames:
            files.append(os.path.join(dirname, subdirname))
        for filename in filenames:
            if  filename[-1] == '~':
                continue
            # match file name pattern (e.g. *.css) if one given
            if pattern and not fnmatch.fnmatch(filename, pattern):
                continue
            files.append(os.path.join(dirname, filename))
    return files
    
version      = "development"
name         = "WMArchive"
description  = "CMS WMArchive Service"
readme       ="WMArchive <https://github.com/dmwm/WMArchive/wiki>"
author       = "Valentin Kuznetsov",
author_email = "vkuznet@gmail.com",
scriptfiles  = filter(os.path.isfile, ['etc/wmarch_config.py'])
url          = "https://github.com/dmwm/WMArchive/wiki"
keywords     = ["WMArchive", "Hadoop"]
package_dir  = {'WMArchive': 'src/python/WMArchive'}
packages     = find_packages('src/python')
data_files   = [
                ('WMArchive/etc', ['etc/wmarch_config.py']),
                ('WMArchive/data/js', datafiles('src/js')),
                ('WMArchive/data/css', datafiles('src/css')),
                ('WMArchive/data/maps', datafiles('src/maps')),
                ('WMArchive/data/sass', datafiles('src/sass')),
                ('WMArchive/data/images', datafiles('src/images')),
                ('WMArchive/data/templates', datafiles('src/templates')),
               ]
license      = "CMS experiment software"
classifiers  = [
    "Status :: Production/Beta",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: CMS/CERN Software License",
    "Operating System :: MacOS :: MacOS X",
    "Operating System :: Microsoft :: Windows",
    "Operating System :: POSIX",
    "Programming Language :: Python",
    "Topic :: Database"
]

def setupEnv():
    "Fix environment for our needs"
    path = os.path.join(os.getcwd(), 'test/python')
    if  'PYTHONPATH' in os.environ.keys():
        os.environ['PYTHONPATH'] += ':%s' % path
    else:
        os.environ['PYTHONPATH'] = ':%s' % path

def main():
    if sys.version < required_python_version:
        s = "I'm sorry, but %s %s requires Python %s or later."
        print(s % (name, version, required_python_version))
        sys.exit(1)

    setupEnv()

    dist = setup(
        name                 = name,
        version              = version,
        description          = description,
        long_description     = readme,
        keywords             = keywords,
        packages             = packages,
        package_dir          = package_dir,
        data_files           = data_files,
        scripts              = datafiles('bin'),
        requires             = ['python (>=2.7)', 'pymongo (>=3.0)', 'pydoop (>=1.1.0)', \
                                'bz2file (>=0.95)', 'sphinx (>=1.0.4)'],
        ext_modules          = [],
        classifiers          = classifiers,
        cmdclass             = {'test': TestCommand,
                                'doc': DocCommand,
                                'clean': CleanCommand},
        author               = author,
        author_email         = author_email,
        url                  = url,
        license              = license,
    )

if __name__ == "__main__":
    main()

