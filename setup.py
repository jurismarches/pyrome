from setuptools import setup

from pyrome import __version__

with open('README.rst', 'r') as f:
    long_description = f.read()


setup(
    name='pyrome',
    version=__version__,
    description='Load Pole Emploie ROME into a database and offers utilities',
    long_description=long_description,
    author='Jurismarches',
    author_email='contact@jurismarches.com',
    url='https://github.com/jurismarches/pyrome',
    packages=['pyrome'],
    install_requires=[
        'peewee>=2.9.1'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.4',
        'Natural Language :: French',
        ])
