#!/usr/bin/env python
"""
Download cloud droplet and shortwave radiative properties for the small
set of models where these are available for the sstClim and sstClimAerosol
experiments.

MODELS:

1. NCC - NorESM1-M
2. MIROC - MIROC5
"""
from CEDA_download import *
import logging

username, password = get_credentials()

group_models = [
    ("MIROC", "MIROC5"),
    ("NCC", "NorESM1-M"),
]

experiments = ["sstClim", "sstClimAerosol",]
freqs = ["mon", ]
ensembles = ["r1i1p1", ]

# Set 1 - cloud properties
realms = ["aerosol", ]
out_realms = ["aero", ]
variables = ["reffclws", "reffclwtop", "od550aer", "conccn", "cdnc", "cldnvi"]

logging.info("Set 1 - Cloud Properties")
datasets = get_datasets(group_models, experiments, freqs, realms,
                        out_realms, ensembles, variables)
download_batch(datasets, "cdnc_swcf/{experiment}/{model}",
               username, password, overwrite=False)

########################################################################

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

# Set 2 - atmosphere/radiation properties
realms = ["atmos", ]
out_realms = ["Amon", ]
variables = ["rlutcs", "rsutcs", "rlut", "rsut", ]

datasets = get_datasets(group_models, experiments, freqs, realms,
                        out_realms, ensembles, variables)
download_batch(datasets, "cdnc_swcf/{experiment}/{model}",
               username, password, overwrite=False)
