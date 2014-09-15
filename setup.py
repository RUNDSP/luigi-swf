import encoding
import io
import os.path
from setuptools import setup, find_packages

import luigi_swf


setup(
    name='luigi_swf',
    version=luigi_swf.__version__,
    url='https://github.com/RUNDSP/luigi_swf',
    license='Apache Software License',
    author='Mike Placentra',
    install_requires=[
	],
    author_email='mplacentra@runads.com',
    description=("Spotify's Luigi + Amazon's Simple "
    			 "Workflow Service integration"),
    long_description='',
    packages=['luigi_swf'],
    platforms='any',
    classifiers = [
        'Programming Language :: Python',
        'Development Status :: 4 - Beta',
        'Natural Language :: English',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
