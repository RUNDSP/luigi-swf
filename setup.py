from setuptools import setup


setup(
    name='luigi-swf',
    version='0.13.1',
    url='https://github.com/RUNDSP/luigi_swf',
    license='Apache Software License',
    install_requires=[
        'boto>=2.28.0',
        'luigi>=1.0.16',
        'pidfile>=0.1.0',
        'python-daemon>=1.6.1',
        'six>=1.6.1',
    ],
    author='Mike Placentra',
    author_email='someone@michaelplacentra2.net',
    description=("Spotify's Luigi + Amazon's Simple "
                 "Workflow Service integration"),
    long_description='',
    packages=['luigi_swf'],
    scripts=[
        'bin/swf_decider_server.py',
        'bin/swf_worker_server.py',
    ],
    platforms='any',
    classifiers = [
        'Programming Language :: Python',
        'Natural Language :: English',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
