# -*- coding: utf-8 -*-
"""
Created on Mon Sep 19 11:37:17 2022
Changed on Thu  Dec 26 12:50:00 2024
based on the following Taylor request:
    If we receive at least one eligible scan from the study eye, then mark eligible - do not wait until 3 scans are received to make eligibility decision
    If 3 scans are received from the study eye but no scans are eligible (e.g. the scans either failed or were ineligible), then mark Ineligible
    Otherwise, mark missing data.

@author: yael
%Changed by ilan
"""

import numpy as np
import statistics
import argparse
import os


def read_args():
    valid_arg = 1
    pt_ID = None
    eye = None
    output_path = None
    qualifier_version = None
    DB_ip = None

    parser = argparse.ArgumentParser(description="Qualifier arguments",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-p", "--Patient_ID", type=int)
    parser.add_argument("-e", "--Eye", type=str)
    parser.add_argument("-o", "--Output_path", type=str)
    parser.add_argument("-q", "--Qualifier_version", type=int)
    parser.add_argument("-d", "--DB_ip", type=str)

    args = parser.parse_args()
    config = vars(args)
    # print(config)

    if config["Patient_ID"] is None:
        print('Abort - Patient ID hasnt been defined')
        valid_arg = 0

    if config["Eye"] is None:
        print('Abort - Eye hasnt been defined')
        valid_arg = 0

    if config["Output_path"] is None:
        print('Abort - Output_path hasnt been defined')
        valid_arg = 0

    if config["Qualifier_version"] is None:
        print('Abort - qualifier_version hasnt been defined')
        valid_arg = 0

    if config["DB_ip"] is None:
        print('Abort - DB_ip hasnt been defined')
        valid_arg = 0

    try:
        if not os.path.isdir(config["Output_path"]):
            os.mkdir(config["Output_path"])
    except:
        print('Cant create output path')
        valid_arg = 0

    pt_ID = config["Patient_ID"]
    eye = config["Eye"]
    output_path = config["Output_path"]
    qualifier_version = config["Qualifier_version"]
    DB_ip = config["DB_ip"]

    if qualifier_version not in [1, 2]:
        print('Abort - qualifier version should be 1 or 2, while inserted value = %d' % qualifier_version)
        valid_arg = 0

    return pt_ID, eye, output_path, qualifier_version, DB_ip, valid_arg


def mean_list(list):
    list = [item.astype('float') for item in list if not np.isnan(item)]
    if list == []:
        return 0
    else:
        return round(statistics.mean(list), 3)


def median_list(list):
    list = [item.astype('float') for item in list if not np.isnan(item)]
    if list == []:
        return 0
    else:
        return round(statistics.median(list), 3)


def std_list(list):
    list = [item.astype('float') for item in list if not np.isnan(item)]
    if len(list) < 2:
        return 0
    else:
        return round(statistics.stdev(list), 3)


def min_list(list):
    list = [item.astype('float') for item in list if not np.isnan(item)]
    if list == []:
        return 0
    else:
        return round(min(list), 3)


def max_list(list):
    list = [item.astype('float') for item in list if not np.isnan(item)]
    if list == []:
        return 0
    else:
        return round(max(list), 3)