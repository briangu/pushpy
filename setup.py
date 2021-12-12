from setuptools import setup
from push.version import VERSION

description='A library for enabling dynamic distributed python deployment'
try:
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
except(IOError, ImportError, RuntimeError):
    long_description = description

setup(
    name='push',
    packages=['push'],
    version=VERSION,
    description=description,
    long_description=long_description,
    author='Brian Guarraci',
    author_email='brian@ops5.com',
    license='MIT',
    url='https://github.com/briangu/push',
    download_url='https://github.com/bakwc/PySyncObj/tarball/' + VERSION,
    keywords=['network', 'replication', 'raft', 'synchronization'],
    classifiers=[
        'Topic :: System :: Networking',
        'Topic :: System :: Distributed Computing',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.6',
        'Operating System :: POSIX :: Linux',
        'Operating System :: MacOS :: MacOS X',
        'License :: OSI Approved :: MIT License',
    ],
    entry_points={
        'console_scripts': [
            'push_admin=push.push_admin:main',
        ],
    },
)
