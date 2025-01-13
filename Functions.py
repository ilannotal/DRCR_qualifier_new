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

import argparse
import os


def read_args():
    valid_arg = 1
    pt_ID = None
    eye = None
    output_path = None
    DB_ip = None
    Just_study_eye = 1

    parser = argparse.ArgumentParser(description="Qualifier arguments",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-p", "--Patient_ID", type=int)
    parser.add_argument("-e", "--Eye", type=str)
    parser.add_argument("-o", "--Output_path", type=str)
    parser.add_argument("-d", "--DB_ip", type=str)
    parser.add_argument("-j", "--Just_study_eye", type=int)

    args = parser.parse_args()
    config = vars(args)
    # print(config)

    if config["Patient_ID"] is None:
        print('Abort - Patient ID has not been defined')
        valid_arg = 0

    if config["Eye"] is None:
        print('Abort - Eye has not been defined')
        valid_arg = 0

    if config["Output_path"] is None:
        print('Abort - Output_path hasnt been defined')
        valid_arg = 0

    if config["DB_ip"] is None:
        print('Abort - DB_ip has not been defined')
        valid_arg = 0

    if config["Just_study_eye"] is None:
        print('Abort - Just_study_eye has not been defined')
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
    DB_ip = config["DB_ip"]
    Just_study_eye = config["Just_study_eye"]

    if Just_study_eye not in [0, 1]:
        print('Abort - Just_study_eye should be 0 or 1, while inserted value = %d' % Just_study_eye)
        valid_arg = 0

    return pt_ID, eye, output_path, DB_ip, Just_study_eye, valid_arg
