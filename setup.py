from setuptools import setup

import luigi_swf


setup(
    name='luigi-swf',
    version=luigi_swf.__version__,
    url='https://github.com/RUNDSP/luigi_swf',
    license='Apache Software License',
    author='Mike Placentra',
    install_requires=[
        'arrow>=0.4.2',
        'boto>=2.28.0',
        'luigi>=1.0.16',
        'pidfile>=0.1.0',
        'python-daemon>=1.6.1',
        'six>=1.6.1',
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
