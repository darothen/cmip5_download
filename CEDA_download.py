#!/usr/bin/env python
"""
CEDA_download.py - Download CMIP5 output from the [Centre for
environmental Data Archival][CEDA].

You'll need your own login (username/password) in order to use this. Once
that's been obtained, these tools should help make it very easy to 
download batches of data, even when you don't know exactly which models
archived particular datasets.

[CEDA]: http://www.ceda.ac.uk/myceda
"""
from __future__ import print_function

from ftplib import FTP
from getpass import getpass
from itertools import product
from pprint import pprint

import os, pickle, re, sys
import logging

#: CEDA FTP address
CEDA_FTP = "ftp.ceda.ac.uk"
#: Path of CMIP5 data on CEDA FTP server
CEDA_BASE_PATH = "badc/cmip5/data/cmip5/output1"

def set_logger(filename=None, level=logging.INFO):

    logger_kwargs = dict(level=level, format="%(message)s")
    if not (filename is None):
        logger_kwargs['filename'] = filename

    logging.basicConfig(**logger_kwargs)

def get_credentials():
    """ Retrieve a user's name and password safely from command
    line. """
    username = input(" Username: ")
    password = getpass(" Password: ")

    return username, password

def get_CEDA_path(group="MIROC", model="MIROC5",
                  experiment="sstClim", freq="mon",
                  realm="aerosol", cmor_table="aero",
                  ensemble="r1i1p1", variable="cldncl"):
    """ Construct the path to a datafile on CEDA's FTP server """
    CEDA_path = os.path.join(CEDA_BASE_PATH, group, model,
                             experiment, freq,
                             realm, cmor_table, ensemble,
                             "latest", variable)
    return CEDA_path

def get_datasets(group_models, experiments, freqs, realms,
                 cmor_tables, ensembles, variables):
    """ Generate dictionaries of arguments to pass to `get_CEDA_path`
    """
    all_products = product(group_models, experiments, freqs,
                           realms, cmor_tables, ensembles,
                           variables)
    for p in all_products:
        product_dict = { key: val for key, val in
                                  zip(["group_model", "experiment",
                                       "freq", "realm", "cmor_table",
                                       "ensemble", "variable"], p) }
        product_dict['group'] = product_dict['group_model'][0]
        product_dict['model'] = product_dict['group_model'][1]
        del product_dict['group_model']

        yield product_dict

def download_batch(datasets,
                   save_path_template="example/{experiment}/{model}/{variable}",
                   username=None, password=None,
                   overwrite=True):

    if (username is None) or (password is None):
        username, password = get_credentials()

    # Login to the FTP
    logging.info("Attempting to connect to %s as '%s'" %
                (CEDA_FTP, username))
    with FTP(CEDA_FTP) as ftp:
        ftp.login(user=username, passwd=password)
        logging.info("Success!")

        # Loop over the requested datasets
        logging.info("Iterating over requested datasets...")
        for i, dataset in enumerate(datasets):
            logging.info("   %d) %r" % (i+1, dataset))
            save_path = save_path_template.format(**dataset)

            # Make path to save data if it doesn't already exist
            if not os.path.exists(save_path):
                os.makedirs(save_path)

            # Download datasets
            path_on_ftp = get_CEDA_path(**dataset)
            logging.info("      Retrieving files in " + path_on_ftp)
            filelist = ftp.nlst(path_on_ftp + "/")
            for fn in filelist:
                base_fn = os.path.basename(fn)
                logging.info("      " + os.path.basename(base_fn))
                save_fn = os.path.join(save_path, base_fn)
                write_cb = open(save_fn, 'wb').write

                if os.path.exists(save_fn):
                    if overwrite:
                        logging.info("      Downloading/writing to " + save_fn)
                        ftp.retrbinary("RETR " + fn, write_cb)
                    else:
                        logging.info("      File exists; skipping " + save_fn)
                else:
                    logging.info("      Downloading/writing to " + save_fn)
                    ftp.retrbinary("RETR " + fn, write_cb)

        logging.info("Closing connection to %s" % CEDA_FTP)
    logging.info("... done.")


class CMIPFilter(object):
    """ Filter class for specifying slices/subsets through the
    CMIP5 directory.

    Assumes a CEDA-style directory structure, with paths like

        CMCC/CMCC-CESM/piControl/mon/atmos/Amon/r1i1p1/latest

    for parsing logic.

    Parameters
    ----------
    includes, excludes : dicts (optional)
        Dictionaries with keys corresponding to valid CMIP bit
        components (see CMIPFilter.BIT_NAMES) and associated values
        containing lists of values to include or exclude

    """

    BIT_PATTERN = r"([\w_-]+)/?"
    BIT_RE = re.compile(BIT_PATTERN)

    BIT_NAMES = ["group", "model", "experiment", "freq",
                 "realm", "cmor_table", "ensemble"]

    def __init__(self, includes={}, excludes={}):
        self.includes = includes
        self.excludes = excludes

    def __call__(self, path_name):
        """ Returns 'True' if this is an acceptable path """
        # Match against regex
        matches = CMIPFilter.BIT_RE.findall(path_name)
        if not matches:
            return True

        # Convert into dictionary for lookup
        bit_dict = { b:m for b, m in zip(CMIPFilter.BIT_NAMES, matches) }

        for bit_name in CMIPFilter.BIT_NAMES[::-1]:
            # Includes
            try:
                bit_val = bit_dict[bit_name]
                if not (bit_val in self.includes[bit_name]):
                    return False
            except KeyError:
                pass
            # Excludes
            try:
                bit_val = bit_dict[bit_name]
                if bit_val in self.excludes[bit_name]:
                    return False
            except KeyError:
                pass

        return True

    def __repr__(self):
        return "CMIPFilter(includes=%r, excludes=%r)" % (self.includes,
                                                         self.excludes)

default_filter = lambda d: True
def _walk_ftp_tree(ftp, start_dir="", cmip_filter=default_filter):
    """ Recursively traverse the `ftp` tree from a starting point, looking
    for full directory trees pointing to where variables (in netCDF format)
    are located. """

    logging.debug("[ walk_ftp_tree(%r, %s, %s) ]" % (ftp, start_dir, cmip_filter))

    found_vars = []
    dir_list = sorted(ftp.nlst(start_dir))
    if not dir_list:
        raise ValueError("Error querying %s" % start_dir)
    elif len(dir_list) == 1:
        # Check if the one file here is the name of the directory - that
        # would indicate it's a file, not a directory!
        if dir_list[0] == start_dir:
            logging.info("--- Found a file.")
            return None
        # Else, we should continue to iterate.

    logging.info("Files/dirs in %s:" % start_dir)
    for fn in dir_list:
        logging.info("    " + fn)

    # Are we at a leaf of the directory tree? That would be indicated if
    # there's a "latest" folder, which will contain the NetCDF files we care
    # about organized into subfolders by variables.
    if "latest" in [os.path.basename(d) for d in dir_list]:
        leaf_dir = start_dir + "/latest"
        logging.info("!!! Found a leaf directory - %s" % leaf_dir)

        var_list = sorted(ftp.nlst(leaf_dir))
        found_vars += var_list
        for i, var in enumerate(var_list, 1):
            # file_list = fnmatch.filter(sorted(ftp.nlst(leaf_dir)), pattern)
            # print(file_list)
            # input("continue...")
            # found_files += file_list
            # for i, fn in enumerate(file_list, 1):
            logging.info("    %d) %s" % (i, os.path.basename(var)))

    else:
        # If not, there must be additional directories to look through
        # However, we'll skip over some:
        filtered_dir_list = [d for d in dir_list if cmip_filter(d)]

        for d in filtered_dir_list:
            # next_dir = start_dir + "/" + d if start_dir else d
            next_dir = d
            dir_result = _walk_ftp_tree(ftp, next_dir, cmip_filter)
            if not (dir_result is None):
                found_vars += dir_result

    logging.debug(">>> Returning")
    for i, v in enumerate(found_vars, 1):
        logging.debug(">>> %d) %s" % (i, v))

    return found_vars

def walk_ftp_tree(includes={}, excludes={},
                  username=None, password=None):

    if (username is None) or (password is None):
        username, password = get_credentials()

    # Login to the FTP
    logging.info("Attempting to connect to %s as '%s'" %
                (CEDA_FTP, username))

    filter_inst = CMIPFilter(includes, excludes)
    logging.info("Filter settings - %r" % filter_inst)

    with FTP(CEDA_FTP) as ftp:
        ftp.login(user=username, passwd=password)
        logging.info("!!! Success!")

        # Go to starting point
        logging.info("Changing to %s" % CEDA_BASE_PATH)
        ftp.cwd(CEDA_BASE_PATH)

        all_var_paths = _walk_ftp_tree(ftp, cmip_filter=filter_inst)

    # Save list of variables to disk for analyzing
    # logging.info("Saving to CEDA_variables.p")
    # with open("CEDA_variables.p", "wb") as f:
    #     pickle.dump(all_vars, f)

    logging.info("... done!")

    return all_var_paths

def breakdown_var_path(var_path):
    """ Breakdown a path of the form
    'MRI/MRI-CGCM3/piControl/mon/atmos/cfMon/r1i1p1/latest/pctisccp' to
    a dictionary of its constituents. """

    bit_names = ["group", "model", "experiment", "freq",
                 "realm", "cmor_table", "ensemble", "_", "variable"]
    path_bits = var_path.split("/")
    bit_dict = { b:m for b, m in zip(bit_names, path_bits) }
    del bit_dict["_"]

    return bit_dict

if __name__ == "__main__":

    datasets = get_datasets([['MIROC', 'MIROC5'], ],
                            ['sstClim', 'sstClimAerosol'],
                            ['mon', ],
                            ['aerosol', ], ['aero', ],
                            ['r1i1p1', ],
                            ['cldncl', 'cldnci', ])
