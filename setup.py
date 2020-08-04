#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup

setup(
    name          = 'seutils',
    version       = '0.25',
    license       = 'BSD 3-Clause License',
    description   = 'Description text',
    url           = 'https://github.com/tklijnsma/seutils.git',
    author        = 'Thomas Klijnsma',
    author_email  = 'tklijnsm@gmail.com',
    packages      = ['seutils'],
    zip_safe      = False,
    scripts       = [
        'bin/seu-format', 'bin/seu-ls', 'bin/seu-hadd', 'bin/seu-du',
        'bin/seu-nentries', 'bin/seu-takechunkroot'
        ],
    )
