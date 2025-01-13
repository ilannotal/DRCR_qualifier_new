# -*- coding: utf-8 -*-
"""
Created on Sun Sep 18 14:11:30 2022
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

warnings.filterwarnings('ignore')


class Logs:
    def __init__(self, output_path):
        self.logs_csv_name = 'QualifierLogDetails.csv'
        self.output_path = output_path

        # Module ID
        self.Qualifier_ID = 60
        self.UnitExeRunStatusID = 11
        # Log action types ID
        self.ErrorID = 0  # Error
        self.DiagnosticsID = 1  # General notification
        # Log status types ID
        self.FailedID = 0
        self.SucceedID = 1
        self.AbortID = 2

        self.log_df = pd.DataFrame(columns=['OperationID', 'Details', 'ActionTypeID', 'StatusTypeID', 'InsertionTime'])

    def insert_log(self, log_str, Module, ActionType, StatusType):
        n = len(self.log_df)
        print(log_str)
        self.log_df.loc[n, 'OperationID'] = Module
        self.log_df.loc[n, 'Details'] = log_str
        self.log_df.loc[n, 'ActionTypeID'] = ActionType
        self.log_df.loc[n, 'StatusTypeID'] = StatusType
        self.log_df.loc[n, 'InsertionTime'] = datetime.now()
        self.save_logs()

    def insert_abort_log(self):
        self.insert_log('Abort qualifier', self.UnitExeRunStatusID, self.ErrorID, self.AbortID)

    def insert_failure_log(self):
        self.insert_log('Qualifier failed', self.UnitExeRunStatusID, self.ErrorID, self.FailedID)

    def save_logs(self):
        self.log_df.to_csv(os.path.join(self.output_path, self.logs_csv_name), index=False)


class My_Qualifier:
    def __init__(self, pt_ID, eye, output_path, DB_ip, Just_study_eye, logs):
        self.pt_ID = pt_ID
        self.eye = eye
        self.output_path = output_path
        self.Just_study_eye = Just_study_eye
        self.logs = logs
        self.required_number_of_scans = 1
        self.max_required_number_of_scans = 3
        self.number_of_failed_cal = 3  # Abort if there are 3 failed calibrations
        self.actual_num_cal_failure = 0    #Actual number of failed calibrations
        self.save_result_csv = 'Qualifier_results.csv'
        self.save_scan_data_csv = 'Qualifer_scan_data.csv'
        self.missing_data_device_ID = 0
        self.not_study_eye_ID = 5
        self.qualified_ID = 2
        self.disqualified_ID = 3
        self.screen_failure_ID = 4

        self.result_df = pd.DataFrame(columns=['PatientID', 'Eye', 'ResultID', 'Message'])
        self.result_df.loc[0, 'PatientID'] = int(pt_ID)
        self.result_df.loc[0, 'Eye'] = eye
        self.DB_ip = DB_ip

        self.connect_to_DB()
        if self.connected_to_DB == 0: return
        self.cursor = self.conn.cursor()

        if Just_study_eye:
            self.check_if_study_eye()
            if self.not_study_eye == 1: return

        self.check_failed_calibration()
        if self.is_failed_cal == 1: return

        self.check_number_of_scans()
        if self.is_missing_scans == 1: return

        self.check_failed_scans()
        #if self.is_failed_scans == 1: return

        self.save_scan_data()

        # if self.is_failed_scans == 0:
        #     result_str = 'Eye is qualified for monitoring'
        #     self.result_df.loc[0, 'ResultID'] = int(self.qualified_ID)
        #     self.result_df.loc[0, 'Message'] = result_str
        # else:
        #     result_str = 'Eye is disqualified for monitoring'
        #     self.result_df.loc[0, 'ResultID'] = int(self.disqualified_ID)
        #     self.result_df.loc[0, 'Message'] = result_str
        #
        # self.logs.insert_log(result_str, self.logs.Qualifier_ID, self.logs.DiagnosticsID, self.logs.SucceedID)
        #
        # self.save_results()

        self.logs.insert_log('Qualifier suceess', self.logs.UnitExeRunStatusID, self.logs.DiagnosticsID,
                             self.logs.SucceedID)

    def connect_to_DB(self):
        try:
            drivers = [item for item in pyodbc.drivers()]
            sub_DB = 'OCTanalysis'
            self.conn = pyodbc.connect('Driver=%s;'
                                       'Server=%s;'
                                       'Database=%s;'
                                       'uid=sa;pwd=Vision321;TrustServerCertificate=yes' % (
                                       drivers[0], self.DB_ip, sub_DB))

            self.logs.insert_log('Connected to DB', self.logs.Qualifier_ID, self.logs.DiagnosticsID,
                                 self.logs.SucceedID)
            self.connected_to_DB = 1

        except Exception as e:
            print(e)
            self.logs.insert_log('Failed to connect to DB', self.logs.UnitExeRunStatusID, self.logs.ErrorID,
                                 self.logs.FailedID)
            self.connected_to_DB = 0
            self.logs.insert_failure_log()

    def check_if_study_eye(self):
        # In the case of Just_study_eye ==1, this function checks if Eye is study eye and return not_study_eye=1 if not.
        self.not_study_eye = 0

        study_eye_query = """
        select _PatientEyeEnrollmentData.Isincluded
            from _PatientEyeEnrollmentData
            where
            _PatientEyeEnrollmentData.PatientID = %d and _PatientEyeEnrollmentData.Eye = '%s'
            """ % (self.pt_ID, self.eye)
        df_study_eye = pd.io.sql.read_sql(study_eye_query, self.conn)
        self.not_study_eye = ~df_study_eye.Isincluded[0]
        message = 'Not study eye' if self.not_study_eye else 'Study eye'
        if self.not_study_eye:
            message = 'Not study eye'
            self.result_df.loc[0, 'Message'] = message
            self.result_df.loc[0, 'ResultID'] = int(self.not_study_eye)
            self.logs.insert_log(message, self.logs.Qualifier_ID, self.logs.DiagnosticsID, self.logs.FailedID)
            self.logs.insert_abort_log()
            self.save_results()
        else:
            self.logs.insert_log('Success in study eye check', self.logs.Qualifier_ID, self.logs.DiagnosticsID, self.logs.SucceedID)

    def check_failed_calibration(self):
        # The function checks if we have self.number_of_failed_cal failed calibrations
        self.is_failed_cal = 0

        sucess_status_type = [4]
        my_query_cal = """
        select scan.UniqueIdentifier,scan.ConfigurationTypeID,
        scan.TerminationStatusTypeID from scan
        join session on scan.SessionID = session.SessionID
        join _Patient on _Patient.PatientID = Session.PatientID
        where scan.ConfigurationTypeID = 0 and _Patient.patientID = %d and Scan.Eye = '%s'
        """ % (self.pt_ID, self.eye)

        df_calibration = pd.io.sql.read_sql(my_query_cal, self.conn)
        num_cal_failures = len(df_calibration[~df_calibration['TerminationStatusTypeID'].isin(sucess_status_type)])
        if num_cal_failures >= self.number_of_failed_cal:
            self.result_df.loc[0, 'ResultID'] = self.screen_failure_ID
            message = 'Screen failure - %d failed calibrations' % (self.number_of_failed_cal)
            self.result_df.loc[0, 'Message'] = message
            self.is_missing_scans = 1 #Failed calibrations
            self.logs.insert_log(message, self.logs.Qualifier_ID, self.logs.DiagnosticsID, self.logs.FailedID)
            self.logs.insert_abort_log()
            self.save_results()
            self.is_failed_cal = 1
        else:
            self.logs.insert_log('Success in calibration check', self.logs.Qualifier_ID, self.logs.DiagnosticsID, self.logs.SucceedID)
            self.actual_num_cal_failure = num_cal_failures
        return

    def check_number_of_scans(self):
        # The function checks if we have enough scans to evaluate qualification
        self.is_missing_scans = 0
        scans = []
        scans_ids = []
        number_of_raster_scans = 0
        number_of_cal_scans = 0

        self.cursor.execute('DECLARE @Patient int;set @Patient = ?; \
        DECLARE @Eye char;set @Eye = ?; \
        select DISTINCT scan.UniqueIdentifier,scan.scanid,  \
        scan.EndTime from scan \
        JOIN Session ON session.SessionID = scan.SessionID \
        JOIN _Patient ON _Patient.PatientID = Session.PatientID \
        where _patient.patientid = @Patient AND SCAN.Eye = @Eye \
        order by scan.EndTime asc', self.pt_ID, self.eye)
        records = self.cursor.fetchall()
        for row in records:
            scans.append(row[0])
            scans_ids.append(row[1])

        self.cal_scans = [scan for scan in scans if 'CAL' in scan]
        number_of_cal_scans = len(self.cal_scans)
        self.raster_scans = [scan for scan in scans if 'TST' in scan]
        self.raster_scans_ids = [scans_ids[n] for n in range(0, len(scans_ids)) if 'TST' in scans[n]]
        self.number_of_raster_scans = len(self.raster_scans)

        if ((number_of_cal_scans < self.number_of_failed_cal) & (self.actual_num_cal_failure == number_of_cal_scans)):
            self.is_missing_scans = 1
            self.result_df.loc[0, 'ResultID'] = self.missing_data_device_ID
            message = 'Missing data - this eye does not have a successful calibration scan'
            self.result_df.loc[0, 'Message'] = message
            self.is_missing_scans = 1 #No calibrations
            self.logs.insert_log(message, self.logs.Qualifier_ID, self.logs.DiagnosticsID, self.logs.FailedID)
            self.logs.insert_abort_log()
            self.save_results()

        elif self.number_of_raster_scans < self.required_number_of_scans:
            self.result_df.loc[0, 'ResultID'] = self.missing_data_device_ID
            message = 'Missing data - this eye has %s raster scans where %s is required' % (
            self.number_of_raster_scans, self.required_number_of_scans)
            self.result_df.loc[0, 'Message'] = message
            self.is_missing_scans = 1 #Less than required raster scans
            self.logs.insert_log(message, self.logs.Qualifier_ID, self.logs.DiagnosticsID, self.logs.FailedID)
            self.logs.insert_abort_log()
            self.save_results()
        else:
            message = 'This eye has %s raster scans where %s are required' % (self.number_of_raster_scans, self.required_number_of_scans)
            self.logs.insert_log(message, self.logs.Qualifier_ID, self.logs.DiagnosticsID , self.logs.SucceedID)
            self.actual_number_of_scans = max(self.required_number_of_scans, min(self.number_of_raster_scans, self.max_required_number_of_scans))
            self.raster_scans = self.raster_scans[0:self.actual_number_of_scans]
            self.raster_scans_ids = self.raster_scans_ids[0:self.actual_number_of_scans]

    def check_failed_scans(self):
        # The function checks if we have a case that all the scans failed. In this
        # case this eye is disqualified

        self.aup_df = pd.DataFrame(
            columns=['ScanID', 'VGAup', 'DNAup', 'RunModeTypeID', 'UpdateLongiPositions', 'EligibleQuant', 'StudyEye'])
        message = ""
        for index,scan in enumerate(self.raster_scans_ids):
            self.is_failed_scans = 0
            self.cursor.execute('DECLARE @Scan int; set @Scan = ?; \
            select Top 1 s.ScanID, s.Eye, VG_aup.ID as VG_aup, NOA_aup.ID as DN_aup, \
            VG_aup.RunModeTypeID as RunModeTypeID, VG_ScanOutput.UpdateLongiPositions as UpdateLongiPositions, \
            EligibleQuant, _PatientEyeEnrollmentData.Isincluded \
            from DN_ScanOutput \
            join AnalysisUnitProcess as NOA_aup on DN_ScanOutput.AnalysisUnitProcessID = NOA_aup.ID \
            join AnalysisUnitProcess as VG_aup on VG_aup.ScanID = NOA_aup.ScanID and VG_aup.CfgID = NOA_aup.CfgID and VG_aup.RunModeTypeID = NOA_aup.RunModeTypeID \
            join VG_ScanOutput on VG_ScanOutput.AnalysisUnitProcessID = VG_aup.ID \
            join Scan s on s.ScanID = NOA_aup.ScanID \
            join Session se on se.SessionID = s.SessionID \
            join _Patient on _Patient.PatientID = se.PatientID \
            join _PatientEyeEnrollmentData on _PatientEyeEnrollmentData.PatientID = _Patient.patientID and _PatientEyeEnrollmentData.Eye = s.Eye \
            where \
            s.ScanID = @Scan \
            order by  VG_aup.ID Desc', scan)
            #--and VG_aup.RunModeTypeID = 1 and VG_ScanOutput.UpdateLongiPositions = 1 \
            #--and DN_ScanOutput.EligibleQuant = 1'

            records = self.cursor.fetchall()
            new_row = {
                'ScanID': scan,
                'VG_aup': records[0].VG_aup,
                'DN_aup': records[0].DN_aup,
                'RunModeTypeID': records[0].RunModeTypeID,
                'UpdateLongiPositions': records[0].UpdateLongiPositions,
                'EligibleQuant': records[0].EligibleQuant,
                'StudyEye': records[0].Isincluded
            }
            # Append the new row to the DataFrame
            new_row_df = pd.DataFrame([new_row])
            self.aup_df = pd.concat([self.aup_df, new_row_df], ignore_index=True)

            new_row_df = new_row_df[((new_row_df.UpdateLongiPositions == 1) & (new_row_df.EligibleQuant == 1))]

            if len(new_row_df) > 0:
                result_str = 'Scan %d is eligible. Eye is qualified for monitoring' % scan
                self.logs.insert_log(result_str, self.logs.Qualifier_ID, self.logs.DiagnosticsID, self.logs.SucceedID)
                self.result_df.loc[0, 'ResultID'] = int(self.qualified_ID)
                self.result_df.loc[0, 'Message'] = result_str
                self.save_results()
                break

            self.is_failed_scans = 1
            #self.result_df.loc[0, 'ResultID'] = int(self.missing_data_analysis_ID)
            message = 'Missing data - scan %d is not eligible' % (scan)
            #self.result_df.loc[0, 'Message'] = message
            self.logs.insert_log(message, self.logs.Qualifier_ID, self.logs.DiagnosticsID, self.logs.FailedID)
            continue
        if self.is_failed_scans:
            if self.actual_number_of_scans == self.max_required_number_of_scans:
                message = 'All %d raster scans failed - eye is disqualified' % self.max_required_number_of_scans
                self.logs.insert_log(message, self.logs.Qualifier_ID, self.logs.DiagnosticsID, self.logs.FailedID)
                self.result_df.loc[0, 'ResultID'] = int(self.disqualified_ID)
                self.result_df.loc[0, 'Message'] = 'Eye is disqualified for monitoring'
                self.save_results()
            else:
                message = 'All %d raster scans failed - waiting for more scans...' % self.actual_number_of_scans
                self.logs.insert_log(message, self.logs.Qualifier_ID, self.logs.DiagnosticsID, self.logs.FailedID)
                self.result_df.loc[0, 'ResultID'] = int(self.missing_data_device_ID)
                self.result_df.loc[0, 'Message'] = message
                self.save_results()


    def save_scan_data(self):
        # The function saves a csv with detailes of the scans used to evaluate
        # the qualification - scan ID, VG aup and NOA aup
        self.scan_data_df = pd.DataFrame(columns=['ScanID', 'VGAup', 'DNAup', 'StudyEye'])
        self.scan_data_df['ScanID'] = self.aup_df['ScanID'][0:self.actual_number_of_scans].astype(int)
        self.scan_data_df['VGAup'] = self.aup_df['VG_aup'][0:self.actual_number_of_scans].astype(int)
        self.scan_data_df['DNAup'] = self.aup_df['DN_aup'][0:self.actual_number_of_scans].astype(int)
        self.scan_data_df['StudyEye'] = self.aup_df['StudyEye'][0:self.actual_number_of_scans].astype(int)
        self.scan_data_df.to_csv(os.path.join(self.output_path, self.save_scan_data_csv), index=False)


    def save_results(self):
        self.result_df.to_csv(os.path.join(self.output_path, self.save_result_csv), index=False)

