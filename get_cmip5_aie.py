#!/usr/bin/env python
"""
Download the data to compute shortwave cloud forcing from the set of
10 CMIP5 models participating in the sstClim/sstClimAerosol experiments

"""
from CEDA_download import *
import logging

SAVE_PATH = "cmip5_aie/{experiment}/{model}"
OVERWRITE = True

set_logger(filename="cmip5_aie.log")

username, password = get_credentials()

group_models = [
    ("BCC", "bcc-csm1-1"),
    ("CCCma", "CanESM2"),
    ("CSIRO-QCCCE", "CSIRO-Mk3-6-0"),
    ("IPSL", "IPSL-CM5A-LR"),
    ("LASG-IAP", "FGOALS-s2"),
    ("MIROC", "MIROC5"),
    ("MOHC", "HadGEM2-A"),
    ("MRI", "MRI-CGCM3"),
    ("NCC", "NorESM1-M"),
    ("NOAA-GFDL", "GFDL-CM3"),
]

experiments = ["sstClim", "sstClimAerosol",]
freqs = ["mon", ]
ensembles = ["r1i1p1", ]

########################################################################

# Set 1 - atmosphere/radiation properties
logging.info("--- BEGIN Set 1")
realms = ["atmos", ]
cmor_tables = ["Amon", ]
variables = ["rlutcs", "rsutcs", "rlut", "rsut", ]

datasets = get_datasets(group_models, experiments, freqs, realms,
                        cmor_tables, ensembles, variables)
download_batch(datasets, SAVE_PATH,
               username, password, overwrite=OVERWRITE)
logging.info("--- END Set 1")

# Set 2 - atmospheric cloud/cloudwater properties
logging.info("--- BEGIN Set 2")
realms = ["atmos", ]
cmor_tables = ["Amon", ]
variables = ["clt", "clwvi", ]

datasets = get_datasets(group_models, experiments, freqs, realms,
                        cmor_tables, ensembles, variables)
download_batch(datasets, SAVE_PATH,
               username, password, overwrite=OVERWRITE)
logging.info("--- END Set 2")

# Set 3 - Time-invariant fields
#logging.info("--- BEGIN Set 3")
#
#realms = ["atmos", ]
#cmor_tables= ["fx", ]
#variables = ["orog", "areacella", "sftlf", ]
#datasets = get_datasets(group_models, ["sstClim", ], ["fx", ], realms,
#                        cmor_tables, ["r0i0p0", ], variables)
#download_batch(datasets, SAVE_PATH, username, password, overwrite=True)
#logging.info("--- END Set 3")

# Set 4 - CDNC/cloud properties
group_models = [
    ("MIROC", "MIROC5"),
    ("NCC", "NorESM1-M"),
]

logging.info("--- BEGIN Set 4")
realms = ["aerosol", ]
cmor_tables = ["aero", ]
variables = ["reffclws", "reffclwtop", "od550aer", "conccn", "cdnc", "cldnvi"]

datasets = get_datasets(group_models, experiments, freqs, realms,
                        cmor_tables, ensembles, variables)
download_batch(datasets, SAVE_PATH,
               username, password, overwrite=OVERWRITE)
logging.info("--- END Set 4")
