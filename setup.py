import os
import sys

from setuptools import find_packages, setup

setup(
    name='Regnskaber',
    version=0.11,
    url='https://github.com/gronlud/regnskaber',
    description=('A financial statement fetching module from the Danish '
                 'Business Authority'),
    # author='Allan Grønlund, Jesper S. Nielsen',
    # author_email='jesper.sindahl.nielsen@gmail.com',
    license='MIT',
    packages=['regnskaber'],
    install_requires=[
        'certifi>=2017.7.27.1',
        'chardet>=3.0.4',
        'elasticsearch>=6.30.0',
        'elasticsearch-dsl>=6.1.00',
        'idna>=2.6',
        'isodate>=0.5.4',
        'langdetect>=1.0.7',
        'lxml>=4.0.0',
        'mysqlclient>=1.3.12',
        'numpy>=1.14.2',
        'python-dateutil>=2.6.1',
        'requests',
        'scipy>=1.0.1',
        'six>=1.11.0',
        'sklearn>=0.0',
        'SQLAlchemy>=1.1.14',
        'urllib3>=1.22',
        'xmljson==0.1.9',
        'xbrl_ai>=0.2',
    ],
    dependency_links=[
        'git+https://github.com/Niels-Peter/XBRL-AI.git@8a90c18ed495487797c6f82d0e6bc8618b5c0bce#egg=xbrl_ai-0.2',
    ],
    package_data={
        'regnskaber': ['resources/*.json', 'resources/aarl.zip']
    }
)
