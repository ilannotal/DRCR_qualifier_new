# -*- coding: utf-8 -*-
"""
Created on Sun Sep 18 14:00:17 2022
Changed on Thu  Dec 26 12:50:00 2024
based on the following Taylor request:
    If we receive at least one eligible scan from the study eye, then mark eligible - do not wait until 3 scans are received to make eligibility decision
    If 3 scans are received from the study eye but no scans are eligible (e.g. the scans either failed or were ineligible), then mark Ineligible
    Otherwise, mark missing data.

@author: yael
%Changed by ilan
"""
import pandas as pd
import os
import pyodbc
from datetime import datetime
import warnings
import argparse
import os

from Qualifier import My_Qualifier, Logs
from Functions import read_args

if __name__ == "__main__":
    local_testing = 0

    if local_testing == 0:
        pt_ID, eye, output_path, DB_ip, Just_study_eye, valid_arg = read_args()
    else:
        pt_ID = 4634
        eye = 'R'
        output_path = r'\\172.17.102.175\Algorithm\Production\DRCR_predictor\Testing'
        DB_ip = '172.30.2.246'
        Just_study_eye = 1
        valid_arg = 1


    if valid_arg:
        logs = Logs(output_path)
        logs.insert_log('Qualifier started successfully', logs.UnitExeRunStatusID, logs.DiagnosticsID, logs.SucceedID)

        Qualifier = My_Qualifier(pt_ID, eye, output_path, DB_ip, Just_study_eye, logs)


