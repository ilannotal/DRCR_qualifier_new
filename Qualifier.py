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
import numpy as np
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
    def __init__(self, pt_ID, eye, output_path, qualifier_version, DB_ip, logs):
        self.pt_ID = pt_ID
        self.eye = eye
        self.output_path = output_path
        self.qualifier_version = qualifier_version
        self.logs = logs
        self.required_number_of_scans = 1
        self.max_required_number_of_scans = 3
        self.number_of_failed_cal = 3  # Abort if there are 3 failed calibrations
        self.save_result_csv = 'Qualifier_results.csv'
        self.save_scan_data_csv = 'Qualifer_scan_data.csv'
        self.missing_data_device_ID = 0
        self.missing_data_analysis_ID = 1
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

        self.check_failed_calibration()
        if self.is_failed_cal == 1: return

        self.check_number_of_scans()
        if self.is_missing_scans == 1: return

        self.check_failed_scans()
        if self.is_failed_scans == 1: return

        # self.check_aup_data()
        # if self.is_missing_aup == 1: return
        #
        # self.extract_parameters()
        self.save_scan_data()
        # # self.evaluate_features()
        #
        if self.is_failed_scans == 0:
            result_str = 'Eye is qualified for monitoring'
            self.result_df.loc[0, 'ResultID'] = int(self.qualified_ID)
            self.result_df.loc[0, 'Message'] = result_str
        else:
            result_str = 'Eye is disqualified for monitoring'
            self.result_df.loc[0, 'ResultID'] = int(self.disqualified_ID)
            self.result_df.loc[0, 'Message'] = result_str

        self.logs.insert_log(result_str, self.logs.Qualifier_ID, self.logs.DiagnosticsID, self.logs.SucceedID)
        #
        # # self.eval_model()
        self.save_results()

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
            self.logs.insert_log(message, self.logs.Qualifier_ID, self.logs.ErrorID, self.logs.FailedID)
            self.logs.insert_abort_log()
            self.save_results()
            self.is_failed_cal = 1
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
        number_of_raster_scans = len(self.raster_scans)

        if number_of_cal_scans < 1:
            self.result_df.loc[0, 'ResultID'] = self.missing_data_device_ID
            message = 'Missing data - this eye doesnt have calibration scans'
            self.result_df.loc[0, 'Message'] = message
            self.is_missing_scans = 1 #No calibrations
            self.logs.insert_log(message, self.logs.Qualifier_ID, self.logs.ErrorID, self.logs.FailedID)
            self.logs.insert_abort_log()
            self.save_results()

        if number_of_raster_scans < self.required_number_of_scans:
            self.result_df.loc[0, 'ResultID'] = int(self.missing_data_device_ID)
            message = 'Missing data - this eye has %s raster scans where %s are required' % (
            number_of_raster_scans, self.required_number_of_scans)
            self.result_df.loc[0, 'Message'] = message
            self.is_missing_scans = 1 #Less than required raster scans
            self.logs.insert_log(message, self.logs.Qualifier_ID, self.logs.ErrorID, self.logs.FailedID)
            self.logs.insert_abort_log()
            self.save_results()
        else:
            self.actual_number_of_scans = max(self.required_number_of_scans, min(number_of_raster_scans, self.max_required_number_of_scans))
            self.raster_scans = self.raster_scans[0:self.actual_number_of_scans]
            self.raster_scans_ids = self.raster_scans_ids[0:self.actual_number_of_scans]

    def check_failed_scans(self):
        # The function checks if we have a case that all the scans failed. In this
        # case this eye is disqualified

        aup_df = pd.DataFrame(
            columns=['ScanID', 'VGAup', 'DNAup', 'RunModeTypeID', 'UpdateLongiPositions', 'EligibleQuant'])

        for scan in self.raster_scans_ids:
            self.is_failed_scans = 0
            self.cursor.execute('DECLARE @Scan int; set @Scan = ?; \
            select DISTINCT s.ScanID, s.Eye, VG_aup.ID as VG_aup, NOA_aup.ID as DN_aup, \
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
            s.ScanID = @Scan and VG_aup.RunModeTypeID = 1', scan)
            #--and VG_aup.RunModeTypeID = 1 and VG_ScanOutput.UpdateLongiPositions = 1 \
            #--and DN_ScanOutput.EligibleQuant = 1'

            records = self.cursor.fetchall()
            new_row = {
                'ScanID': scan,
                'VGAup': records[0].VG_aup,
                'DNAup': records[0].DN_aup,
                'RunModeTypeID': records[0].RunModeTypeID,
                'UpdateLongiPositions': records[0].UpdateLongiPositions,
                'EligibleQuant': records[0].EligibleQuant
            }
            # Append the new row to the DataFrame
            aup_df = pd.concat([aup_df, pd.DataFrame([new_row])], ignore_index=True)

            records = records[((records.UpdateLongiPositions == 1) & (records.EligibleQuant == 1))]
            if len(records) == 0:
                self.is_failed_scans = 1
                self.result_df.loc[0, 'ResultID'] = int(self.missing_data_analysis_ID)
                message = 'Missing data - scan %d doesnt have VG analysis' % (scan)
                self.result_df.loc[0, 'Message'] = message
                self.logs.insert_log(message, self.logs.Qualifier_ID, self.logs.ErrorID, self.logs.FailedID)
                self.logs.insert_abort_log()
                self.save_results()
                continue
        #     """
        # SELECT s.ScanID, EyeOrdinalOrder, vg.NextAnalysisUnitRunModeTypeID as SwitchToUsage,
        # LongiCalAup.IsFinalResult LongiAupIsFinalResult, LongiCalAup.EndTime LongiAupEndTime,
        # commonAup.IsFinalResult commonAupIsFinalResult,
        # commonAup.RunResult commonAupResult, LongiCalAup.CfgID
        # FROM Scan s
        # JOIN Session se ON se.SessionID = s.SessionID
        # JOIN AnalysisUnitProcess LongiCalAup ON LongiCalAup.ScanID = s.ScanID AND LongiCalAup.RunMOdeTypeID = 0
        # JOIN AnalysisUnitDetails aud ON aud.ID = LongiCalAup.AnalysisUnitDetailsID
        # LEFT JOIN AnalysisUnitProcess commonAup ON commonAup.RunModeTypeID = 3 AND LongiCalAup.ScanID = commonAup.ScanID
        # LEFT JOIN VG_ScanOutput vg ON vg.AnalysisUnitProcessID = LongiCalAup.ID
        # WHERE s.ConfigurationTypeID = 2 AND s.AnalysisStatusTypeID <> 0
        # AND aud.UnitTypeID IN (6,12)
        # AND se.PatientID = %d AND s.Eye = '%s'
        # ORDER BY EyeOrdinalOrder
        # """ % (self.pt_ID, self.eye)

        if self.is_failed_scans == 1:
            message = 'All %d raster scans failed - eye is disqualified' % self.max_required_number_of_scans
            self.logs.insert_log(message, self.logs.Qualifier_ID, self.logs.DiagnosticsID, self.logs.SucceedID)
            self.result_df.loc[0, 'ResultID'] = int(self.disqualified_ID)
            self.result_df.loc[0, 'Message'] = 'Eye is disqualified for monitoring'
            self.save_results()
            self.logs.insert_log('Qualifier suceess', self.logs.UnitExeRunStatusID, self.logs.DiagnosticsID,
                                 self.logs.SucceedID)
        return

    def get_latest_cfg(self):
        # The function checks what is the latest cfg of the analysis
        self.cfg_ID = -1
        scan_string = "','".join(self.raster_scans)
        scan_string = "('" + scan_string + "')"
        q = """
        select DISTINCT _Study.Name,_Patient.patientid,_Patient.StudySubjectID,scan.EndTime,scan.Eye,scan.UniqueIdentifier,AnalysisUnitDetails.UnitTypeID, aup.*
        from AnalysisUnitProcess as aup
        JOIN AnalysisUnitDetails ON AnalysisUnitDetails.ID = aup.AnalysisUnitDetailsID
        join scan on scan.ScanID = aup.ScanID
        join Session on session.SessionID = scan.SessionID 
        join _Patient on _Patient.PatientID = Session.PatientID
        join _patienteyestudy on _patienteyestudy.patientid = _patient.patientid
        join _Study on _Study.StudyID = _patienteyestudy.StudyID
        join _user on _patient.userid = _user.userid
        where _patient.patientid = %d  and aup.RunModeTypeID = 1 and scan.Eye = '%s'
        and scan.UniqueIdentifier in %s
        order by scan.Eye, aup.RunModeTypeID, scan.EndTime asc
        """ % (self.pt_ID, self.eye, scan_string)
        df = pd.io.sql.read_sql(q, self.conn)
        if not df.empty:
            self.cfg_ID = int((df.CfgID).max())
        return

    def check_aup_data(self):
        # The function checks if all VG aups exist for the first scans, and if NOA aups
        # exist
        self.is_missing_aup = 0
        self.get_latest_cfg()

        for n, scan in enumerate(self.raster_scans):
            self.is_missing_aup = 0
            self.cursor.execute('DECLARE @Scan varchar(50);set @Scan = ?; \
            DECLARE @CfgID int;set @CfgID = ?; \
            select aup.ID,scan.scanid,AnalyzerLogDetails.StatusTypeID from analysisunitprocess as aup  \
            JOIN scan ON aup.scanid = scan.scanid  \
            JOIN VG_ScanOutput ON VG_ScanOutput.AnalysisUnitProcessID = aup.ID  \
            join AnalyzerLog on AnalyzerLog.AnalysisUnitProcessID = aup.ID  \
            join AnalyzerLogDetails on AnalyzerLogDetails.RunID = AnalyzerLog.RunID  \
            where scan.uniqueidentifier = @Scan and aup.RunModeTypeID = 1  \
            and AnalyzerLogDetails.operationid = 11  \
            and aup.CfgID = @CfgID \
            AND AnalyzerLogDetails.ExceptionTypeID IS NULl', scan, self.cfg_ID)
            records = self.cursor.fetchall()
            if len(records) == 0:
                self.is_missing_aup = 1
                self.result_df.loc[0, 'ResultID'] = int(self.missing_data_analysis_ID)
                message = 'Missing data - scan %d doesnt have VG analysis' % (self.raster_scans_ids[n])
                self.result_df.loc[0, 'Message'] = message
                self.logs.insert_log(message, self.logs.Qualifier_ID, self.logs.ErrorID, self.logs.FailedID)
                self.logs.insert_abort_log()
                self.save_results()
                continue

            else:  # Check NOA if VG ended succesfuly
                # VG_final_status = records[0][1]
                # if VG_final_status in [1,4]: # Should be DN analysis
                self.cursor.execute('DECLARE @Scan varchar(50);set @Scan = ?; \
                DECLARE @CfgID int;set @CfgID = ?; \
                select aup.ID,scan.scanid from analysisunitprocess as aup  \
                JOIN scan ON aup.scanid = scan.scanid  \
                INNER JOIN AnalysisUnitDetails as aud on aup.AnalysisUnitDetailsID = aud.ID\
                INNER JOIN AnalysisUnitType as aut ON aud.UnitTypeID = aut.ID\
                INNER JOIN AnalysisOutputType aot ON aut.OutputTypeID = aot.ID\
                where aot.Name = ?\
                and aup.CfgID = @CfgID and\
                scan.uniqueidentifier = @Scan and aup.RunModeTypeID = 1', scan, self.cfg_ID, 'DN')

                records = self.cursor.fetchall()
                if len(records) == 0:
                    self.is_missing_aup = 1
                    self.result_df.loc[0, 'ResultID'] = int(self.missing_data_analysis_ID)
                    message = 'Missing data - scan %d doesnt have NOA analysis' % (self.raster_scans_ids[n])
                    self.result_df.loc[0, 'Message'] = message
                    self.logs.insert_log(message, self.logs.Qualifier_ID, self.logs.ErrorID, self.logs.FailedID)
                    self.logs.insert_abort_log()
                    self.save_results()
                    continue

    def extract_aup_data(self):
        # The function extracts the aup of each scan for both VG and NOA
        aup_df = pd.DataFrame(columns=['Scan'])

        self.cursor.execute('DECLARE @Patient int;set @Patient = ?; \
        DECLARE @Eye char;set @Eye = ?; \
        DECLARE @CfgID int;set @CfgID = ?; \
        select DISTINCT scan.UniqueIdentifier, scan.ScanID, VG_aup.cfgID,\
        VG_ScanOutput.QaReachedRast,VG_aup.ID ,AnalyzerLogDetails.StatusTypeID,\
        scan.EndTime from scan \
        JOIN AnalysisUnitProcess as VG_aup ON scan.ScanID = VG_aup.ScanID \
        JOIN VG_ScanOutput ON VG_ScanOutput.AnalysisUnitProcessID = VG_aup.ID \
        JOIN Session ON session.SessionID = scan.SessionID \
        JOIN _Patient ON _Patient.PatientID = Session.PatientID \
        join AnalyzerLog on AnalyzerLog.AnalysisUnitProcessID = VG_aup.ID  \
        join AnalyzerLogDetails on AnalyzerLogDetails.RunID = AnalyzerLog.RunID  \
        where _patient.patientid = @Patient AND SCAN.Eye = @Eye \
        AND VG_aup.RunModeTypeID = 1 and AnalyzerLogDetails.operationid = 11 and  \
        AnalyzerLogDetails.ExceptionTypeID IS NULl \
        and VG_aup.CfgID = @CfgID\
        order by scan.EndTime asc', self.pt_ID, self.eye, self.cfg_ID)

        records_VG = self.cursor.fetchall()
        count = -1
        for row in records_VG:
            count += 1
            aup_df.loc[count, "Scan"] = row[0].replace(' ', '')
            aup_df.loc[count, "ScanID"] = row[1]
            aup_df.loc[count, "cfg"] = row[2]
            aup_df.loc[count, "ReachedRaster"] = row[3]
            aup_df.loc[count, "VG_aup"] = row[4]
            aup_df.loc[count, "VG_status"] = row[5]

        self.cursor.execute('DECLARE @Patient int;set @Patient = ?; \
        DECLARE @Eye char;set @Eye = ?; \
        DECLARE @CfgID int;set @CfgID = ?; \
        SELECT DISTINCT DN_aup.ID, \
        scan.UniqueIdentifier,DN_aup.CfgID from SCAN \
        JOIN AnalysisUnitProcess as DN_aup ON scan.ScanID = DN_aup.ScanID \
        INNER JOIN AnalysisUnitDetails as aud on DN_aup.AnalysisUnitDetailsID = aud.ID\
        INNER JOIN AnalysisUnitType as aut ON aud.UnitTypeID = aut.ID\
        INNER JOIN AnalysisOutputType aot ON aut.OutputTypeID = aot.ID\
        JOIN Session ON session.SessionID = scan.SessionID \
        JOIN _Patient ON _Patient.PatientID = Session.PatientID \
        where _patient.patientid = @Patient AND SCAN.Eye = @Eye \
        and  aot.Name = ?\
        and DN_aup.CfgID = @CfgID\
        AND DN_aup.RunModeTypeID = 1', self.pt_ID, self.eye, self.cfg_ID, 'DN')

        records_DN = self.cursor.fetchall()
        for row in records_DN:
            DN_aup = row[0]
            scan = row[1]
            scan = scan.replace(' ', '')
            cfg = row[2]
            analysis_index = aup_df.index[(aup_df["Scan"] == scan) & (aup_df["cfg"] == cfg)]
            aup_df.loc[analysis_index, "DN_aup"] = DN_aup

        self.aup_df = aup_df

    def save_scan_data(self):
        # The function saves a csv with detailes of the scans used to evaluate
        # the qualification - scan ID, VG aup and NOA aup
        self.scan_data_df = pd.DataFrame(columns=['ScanID', 'VGAup', 'DNAup'])
        self.scan_data_df['ScanID'] = self.aup_df['ScanID'][0:self.required_number_of_scans].astype(int)
        self.scan_data_df['VGAup'] = self.aup_df['VG_aup'][0:self.required_number_of_scans].astype(int)
        self.scan_data_df['DNAup'] = self.aup_df['DN_aup'][0:self.required_number_of_scans].astype(int)
        self.scan_data_df.to_csv(os.path.join(self.output_path, self.save_scan_data_csv), index=False)

    def extract_calibration_data(self):
        # The function extracts the calibration parameters
        first_scan = self.aup_df["Scan"][0]
        self.cursor.execute("DECLARE @Scan varchar(50);set @Scan = ?; \
                       select scan.StartDiopter,scan.StartReferenceArmPosition from scan \
                       where scan.UniqueIdentifier = @Scan", first_scan)
        cal_records = self.cursor.fetchall()
        for row in cal_records:
            self.parameters_df.loc[0, 'Diopter'] = row[0]
            self.parameters_df.loc[0, 'Start_Refarm_Position'] = row[1]

        all_scans = []
        self.cursor.execute("DECLARE @Patient int;set @Patient = ?; \
                       DECLARE @Eye char;set @Eye = ?;\
                       select scan.UniqueIdentifier from scan \
                       join Session on Session.SessionID =  scan.SessionID \
                       join _Patient on _Patient.PatientID =  Session.PatientID \
                       where _Patient.patientid = @Patient and scan.Eye = @Eye \
                       order by Scan.StartTime", self.pt_ID, self.eye)
        scan_records = self.cursor.fetchall()
        for row in scan_records:
            all_scans.append(row[0].replace(' ', ''))
        is_cal = np.array(['CAL' in scan for scan in all_scans]).astype(int)

        try:
            first_cal_ind = np.where(is_cal)[0][0]
        except:
            first_cal_ind = 0

        is_cal_diff = np.diff(is_cal)
        num_calibrations = 1
        for i in range(first_cal_ind, len(is_cal_diff)):
            is_curr_cal = is_cal_diff[i]
            if is_curr_cal == 0:
                num_calibrations += 1
            else:
                break
        self.parameters_df.loc[0, 'Num_calibrations'] = num_calibrations

    def extract_parameters(self):
        # The function extracts the calibration and analysis parameters of the scans
        self.extract_aup_data()
        # The function returns a df with data from VG and DN for all relevant analysis units
        VG_SCAN_DB_values = ['TotalScanTime', 'MeanBMsiVsr', 'QaReachedRast', 'RegStdX', 'RegStdY', 'LongiRegShiftX', \
                             'LongiRegShiftY', 'MaxBMsiAll', '[MeanRetinalThickness3*3]', 'UpdateLongiPositions',
                             'ClippedPrecent']
        DN_SCAN_DB_values = ['EligibleQuant']
        B_classes = [1, 2, 3, 4]
        self.parameters_df = pd.DataFrame()

        # Calibration
        self.extract_calibration_data()

        # Scan
        for i in range(0, self.required_number_of_scans):
            index = i
            vg_aup = self.aup_df['VG_aup'][index]
            vg_status = self.aup_df['VG_status'][index]
            dn_aup = self.aup_df['DN_aup'][index]
            for val in VG_SCAN_DB_values:
                q = "select %s from VG_ScanOutput where VG_ScanOutput.AnalysisUnitProcessID = %d" % (val, vg_aup)
                self.cursor.execute(q)
                records = self.cursor.fetchall()
                self.parameters_df.loc[0, '%s_%d' % (val, i)] = records[0][0]

            for val in DN_SCAN_DB_values:
                if vg_status in [1, 4]:
                    q = "select %s from DN_ScanOutput where DN_ScanOutput.AnalysisUnitProcessID = %d" % (val, dn_aup)
                    self.cursor.execute(q)
                    records = self.cursor.fetchall()
                    self.parameters_df.loc[0, '%s_%d' % (val, i)] = records[0][0]
                else:
                    if val == 'EligibleQuant':
                        self.parameters_df.loc[0, '%s_%d' % (val, i)] = 0

            for Class in B_classes:
                q = "select cast(count(VG_BScanVSROutput.classType) as float)/cast(VG_ScanOutput.NumValidLines as float) as class1 \
                from VG_BScanVSROutput join VG_ScanOutput on VG_ScanOutput.AnalysisUnitProcessID = VG_BScanVSROutput.AnalysisUnitProcessID \
                where classType=%d and VG_ScanOutput.AnalysisUnitProcessID = %d \
                group by VG_BScanVSROutput.AnalysisUnitProcessID,classType,VG_ScanOutput.NumValidLines" % (
                Class, vg_aup)
                self.cursor.execute(q)
                records = self.cursor.fetchall()
                if records == []:
                    self.parameters_df.loc[0, 'Class%d_%d' % (Class, i)] = 0
                else:
                    self.parameters_df.loc[0, 'Class%d_%d' % (Class, i)] = records[0][0]

        self.logs.insert_log('Extracting parameters', self.logs.Qualifier_ID, self.logs.DiagnosticsID,
                             self.logs.SucceedID)


    def save_results(self):
        self.result_df.to_csv(os.path.join(self.output_path, self.save_result_csv), index=False)

