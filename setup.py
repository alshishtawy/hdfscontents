import os
from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name="hdfscontents",
    version="0.1.0",
    author="Ahmad Al-Shishtawy",
    author_email="alshishtawy@gmail.com",
    description=("Jupyter content manager that uses the HDFS filesystem"),
    license="Apache License 2.0",
    keywords="Jupyter, HDFS",
    url="https://github.com/alshishtawy/hdfscontents",
    packages=['hdfscontents', 'tests'],
    long_description=read('README.rst'),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Framework :: IPython",
        "Topic :: Utilities",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
    ], install_requires=['traitlets', 'notebook', 'hdfs3', 'tornado', 'nbformat', 'ipython_genutils']
)
