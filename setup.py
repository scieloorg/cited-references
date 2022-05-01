#!/usr/bin/env python
from setuptools import setup, find_packages


install_requires = [
    'aiohttp',
    'articlemetaapi',
    'async-timeout',
    'beautifulsoup4',
    'bs4',
    'certifi',
    'chardet',
    'idna',
    'idna-ssl',
    'legendarium',
    'multidict',
    'ply',
    'pymongo',
    'requests',
    'selenium',
    'soupsieve',
    'thriftpy2',
    'urllib3',
    'xylose',
    'yarl',
    'scielo_scholarly_data',
]

setup(
    name="cited-references",
    version='0.5',
    description="The SciELO Citation Tools",
    author="SciELO",
    author_email="scielo-dev@googlegroups.com",
    license="BSD",
    url="https://github.com/scieloorg/cited-references",
    maintainer_email='rafael.pezzuto@gmail.com',
    packages=find_packages(),
    install_requires=install_requires,
    entry_points="""
    [console_scripts]
    clean-elsevier=core.cleaners.elsevier:main
    match=core.matchers.match:main
    scrap-latindex=core.scrappers.latindex:main
    scrap-scielo=core.scrappers.scielo:main
    ident-data-elsevier=core.utils.identify_data_to_collect:main
    """
)
