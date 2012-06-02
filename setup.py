import os
import sys
from setuptools import setup, find_packages

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()
    
setup(
    name = "pyfusiontables",
    version = "0.0.1.1",
    description='Use fusiontables to store column oriented data',
    long_description = read('README.rst'),
    url='https://github.com/shuggiefisher/python-fusiontables',
    license = 'BSD',
    author = 'Sam Vevang',
    author_email = 'sam.vevang@gmail.com',
    packages = find_packages(),
    classifiers = [
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
    dependency_links = [
        "http://bitbucket.org/svevang/python-fusion-tables-client/get/tip.zip#egg=python-fusion-tables-client",
    ],
    install_requires = ['python-fusion-tables-client', 'importlib', 'python-dateutil', 'simplejson'],
)
