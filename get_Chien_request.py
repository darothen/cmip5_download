#!/usr/bin/env python
"""
1. piControl
a) models: ACCESS1-0, ACCESS1-3, BNU-ESM, CSIRO-Mk3-6-0, MRI-CGCM3
missing variables: rlut, rlutcs, rsdt, rsut, rsutcs
b) for all models
missing variables: rsds, rsdscs

2. historical
a) GISS-E2-H
missing variable: rsut
b) for models: GISS-E2-H, GISS-E2-R
missing variables: rsdt

3. for historical, historicalGHG, and historicalNAT
for all models, missing rsdscs
"""
from CEDA_download import *
import logging

SAVE_PATH = "chien/{experiment}/{model}"
OVERWRITE = True

set_logger(filename="chien1.log")

username, password = get_credentials()

def find_and_download(variables, includes):
    all_var_paths = walk_ftp_tree(includes,
                                  username=username, password=password)
    all_var_datasets = [breakdown_var_path(p) for p in all_var_paths]
    datasets = [d for d in all_var_datasets
                if d['variable'] in variables]
    download_batch(datasets, SAVE_PATH,
                   username, password, overwrite=OVERWRITE)
"""
logging.info("--- BEGIN Set 1a")
# Set 1a - piControl, specific models/fields
variables = ['rlut', 'rlutcs', 'rsdt', 'rsut', 'rsutcs']
includes = { 'model': ['ACCESS1-0', 'ACCESS1-3', 'BNU-ESM',
                       'CSIRO-Mk3-6-0', 'MRI-CGCM3'],
             'experiment': 'piControl',
             'freq': 'mon',
             'realm': 'atmos',
             'ensemble': 'r1i1p1' }
find_and_download(variables, includes)
logging.info("--- END Set 1a")

# Set 1b - piControl, just some vars but for *all* available models
logging.info("--- BEGIN Set 1b")
variables = ['rsds', 'rsdscs']
includes = { 'experiment': 'piControl',
             'freq': 'mon',
             'realm': 'atmos',
             'ensemble': 'r1i1p1' }
find_and_download(variables, includes)
logging.info("--- END Set 1b")

# Set 2 - Historical emissions, rsut
logging.info("--- BEGIN Set 2")
variables = ['rsut', 'rsdt']
includes = { 'model': ['GISS-E2-H', 'GISS-E2-R'],
             'experiment': 'historical',
             'freq': 'mon',
             'realm': 'atmos',
             'ensemble': 'r1i1p1' }
find_and_download(variables, includes)
logging.info("--- END Set 2")
"""

# Set 3 - rsdcs for all models for historical, historicalGHG,
#         historicalNAT
logging.info("--- BEGIN Set 3")
variables = ['rsdscs', ]
#includes = { 'experiment': ['historical', 'historicalGHG',
#                            'historicalNAT', ],
includes = { 'experiment': ['historicalNat', ],
             'freq': 'mon',
             'realm': 'atmos',
             'ensemble': 'r1i1p1' }
find_and_download(variables, includes)
logging.info("--- END Set 3")
