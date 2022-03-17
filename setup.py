from setuptools import setup
import pushpy
import os

description = 'A library for enabling dynamic distributed python deployment'
try:
    import pypandoc

    long_description = pypandoc.convert('README.md', 'rst')
except(IOError, ImportError, RuntimeError):
    long_description = description

install_requires = [
    'wheel',
    'dill~=0.3.4',
    'setuptools',
    'requests~=2.26.0',
    'tornado~=6.1',
    'pysyncobj~=0.3.10',
    'psutil~=5.8.0',
    'GPUtil~=1.4.0',
    'PyYaml~=6.0',
    ]

setup(
    name='pushpy',
    packages=['pushpy'],
    version=pushpy.__version__,
    description=description,
    long_description=long_description,
    author='Brian Guarraci',
    author_email='brian@ops5.com',
    license='MIT',
    url='https://github.com/briangu/push',
    keywords=['network', 'replication', 'raft', 'synchronization', 'application'],
    install_requires=install_requires,
    setup_requires=["wheel"],
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
            'push_repl=pushpy.push_repl:main',
            'push_server=pushpy.push_server:main',
        ],
    },
)
