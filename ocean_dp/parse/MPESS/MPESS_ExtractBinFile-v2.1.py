# ------------------------------------------------------------------------------
# Revision History
# v1.0 - First functional Software
# v1.1 - Handle Line Tension Calibration and Units in Data File
# v1.2 - Save data to CSV file
# v1.3 - Handle variable data fields
# v1.3 - Include IMU model number in sample
# v1.4 - Save plots to image
# v1.5 - Display IMU Model Number
# v1.5 - Only plot available fields
# v1.6 - Convert and Plot Euler Angles
# v1.6 - Speed up plotting function
# v1.6 - Disply time axis nicely
# v1.6 - Save figures nicely
# v1.7 - Save data to CSV file
# v1.7 - Save Info to file
# v1.7 - Improve data extraction for corrupt files
# v2.0 - Change Data Structure to include Acceleration
# v2.1 - Added netcdf output
SoftwareVersion = 'v2.1'

# ------------------------------------------------------------------------------
import struct  # unpack
import datetime  # date time for sec conversion
import pytz
#import tzlocal
import os
import ctypes  # c struct and union routines
import pandas as pd  # pandas data frame
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib

import zlib  # used for CRC32 calculation
import csv  # used to creat csv file
import math

import MPESS_Struct

import sys

from netCDF4 import Dataset
from netCDF4 import date2num
import numpy as np
from datetime import timedelta

# %matplotlib qt

# ------------------------------------------------------------------------------
# Global Variables

UnitNr = 6
DeploymentNr = 14

Flag_CreateCSV = False
Flag_CreateNetCDF = True
Flag_PlotData = False
Flag_SavePlots = False

if (Flag_PlotData == False):
    Flag_SavePlots = False

# logger
#DeploymentFilePath = r'/home/parallels/Desktop/Parallels Shared Folders/Home/CSIRO/Projects/0201 MPESS/MiniLogger v1/Data/MPESS_U%02u_D%03u' % (UnitNr, DeploymentNr)


# Mini-Logger
# DeploymentFilePath = r'/Users/pete/DWM/MPESS/MPESS_U%02u_D%03u' %(UnitNr, DeploymentNr)
#
# if (UnitNr == 1):
#     DeploymentFileName = 'MPESS_U01_S24C6C99A_D%03u_Data.bin' % DeploymentNr
# elif (UnitNr == 2):
#     DeploymentFileName = 'MPESS_U02_SC6D2EBFC_D%03u_Data.bin' % DeploymentNr
# elif (UnitNr == 5):
#     DeploymentFileName = 'MPESS_U05_S333FFC1F_D%03u_Data.bin' % DeploymentNr
# elif (UnitNr == 6):
#     DeploymentFileName = 'MPESS_U06_SCE5C93B7_D%03u_Data.bin' % DeploymentNr
# else:
#     DeploymentFileName = 'unknown.bin'

DeploymentFilePath = './'
DeploymentFileName = sys.argv[1]

PressureUnits = ''
LineTensionUnits = ''
NrOfSamplesPerStop = 0

#TimezoneLocal = tzlocal.get_localzone()


# ------------------------------------------------------------------------------
def ExtractAllSamples(deployment_file, file_Info, writer_CSV):
    time_start = datetime.datetime.now()
    NrOfErrors = 0
    df = pd.DataFrame()

    # create CSV header
    if Flag_CreateCSV:
        HeaderList = ['Time (UTC)', 'Battery Voltage [V]', 'Pressure [dbarA]', 'LineTension [kg]',
                      'IMU_TimeStamp [ms]',  
                      'IMU_AccX [g]',       'IMU_AccY [g]',     'IMU_AccZ [g]',
                      'IMU_GyroX [rad/s]',  'IMU_GyroY [rad/s]','IMU_GyroZ [rad/s]', 
                      'IMU_MagnX [Gauss]',  'IMU_MagnY [Gauss]','IMU_MagnZ [Gauss]', 
                      'IMU_OM_M11',         'IMU_OM_M12',       'IMU_OM_M13', 
                      'IMU_OM_M21',         'IMU_OM_M22',       'IMU_OM_M23', 
                      'IMU_OM_M31',         'IMU_OM_M32',       'IMU_OM_M33'  ]
        writer_CSV.writerow(HeaderList)

    # extract data
    SmpNr = 0
    Flag_AllDone = False
    while not Flag_AllDone:

        # read sensor type
        (SampleType, Flag_AllDone, BytesDeleted) = ScanForNextSampleType(deployment_file)
        if BytesDeleted:
            DebugString = 'Currupted Data Deleted (%u Bytes)' % BytesDeleted
            file_Info.write(DebugString + '\r\n')
            print(DebugString)

        # process sample
        if not Flag_AllDone:

            SmpNr += 1
            #            if SmpNr==5:  Flag_AllDone=True

            # -------------------------
            # Noraml Sample
            if (SampleType & 0xFFFFFFF0) == MPESS_Struct.SAMPLE_TYPE_NORMAL:
                DebugString = 'Smp %u - Normal Sample ' % SmpNr
                (FlagOk, InfoString, df_n) = ProcessSample_Normal(deployment_file, writer_CSV)
                if FlagOk == True:
                    DebugString += 'Ok'
                    df = df.append(df_n)
                else:
                    NrOfErrors += 1
                    DebugString += InfoString
                file_Info.write(DebugString + '\r\n')
                print(DebugString)

            # -------------------------
            # Intensive Sample
            elif (SampleType & 0xFFFFFFF0) == MPESS_Struct.SAMPLE_TYPE_INTENSIVE:
                DebugString = 'Smp %u - Intensive Sample ' % SmpNr
                (FlagOk, InfoString, df_i) = ProcessSample_Intensive(deployment_file, writer_CSV)
                if FlagOk == True:
                    DebugString += 'Ok'
                    df = df.append(df_i)
                else:
                    NrOfErrors += 1
                    DebugString += InfoString
                file_Info.write(DebugString + '\r\n')
                print(DebugString)

            # -------------------------
            # Info
            elif SampleType == MPESS_Struct.SAMPLE_TYPE_INFO:
                DebugString = 'Smp %u - Info Sample ' % SmpNr
                (FlagOk, InfoString) = ProcessSample_Info(deployment_file)
                if (FlagOk):
                    DebugString += 'Ok \r\n'
                    DebugString += InfoString
                if FlagOk == False:
                    NrOfErrors += 1
                    DebugString += InfoString
                file_Info.write(DebugString + '\r\n')
                print(DebugString)

            # -------------------------
            # unknown sample type
            else:
                DebugString = 'Smp %u - Unknown Sample Type (0x%08X)\r\n' % (SmpNr, SampleType)
                file_Info.write(DebugString + '\r\n')
                print(DebugString)
                Flag_AllDone = True

    # extraction info
    DebugString = '\r\nProcess time = %s\r\n' % (datetime.datetime.now() - time_start)
    DebugString += 'Processed %u Samples\r\n' % SmpNr
    if NrOfErrors:
        DebugString += '%u Samples with Errors found\r\n' % (NrOfErrors)
    else:
        DebugString += 'No errors detected\r\n'
    print(DebugString)
    file_Info.write(DebugString + '\r\n')
    return df


# ------------------------------------------------------------------------------
def ScanForNextSampleType(deployment_file):
    BytesDeleted = 0
    while True:

        # read the next 4 bytes
        TypeBuffer = deployment_file.read(4)
        if len(TypeBuffer) == 0:
            return (0, True, BytesDeleted)
        elif len(TypeBuffer) < 4:
            BytesDeleted += len(TypeBuffer)
            return (0, False, BytesDeleted)

        # conver buffer to SampleType
        SampleType = struct.unpack('>L', TypeBuffer[:4])[0]
        #        print('%08X' %SampleType)
        if ((SampleType & 0xFFFFFFF0) == MPESS_Struct.SAMPLE_TYPE_NORMAL) or \
                ((SampleType & 0xFFFFFFF0) == MPESS_Struct.SAMPLE_TYPE_INTENSIVE) or \
                (SampleType == MPESS_Struct.SAMPLE_TYPE_INFO):
            deployment_file.seek(-4, 1)  # move file pointer 4 characters back from the current position
            return (SampleType, False, BytesDeleted)

        else:
            deployment_file.seek(-3, 1)  # move file pointer 3 characters back from the current position
            BytesDeleted += 1


#            if BytesDeleted==20:
#                while True:
#                    continue

# ------------------------------------------------------------------------------
def ProcessSample_Intensive(deployment_file, writer_CSV):
    data_lists = []
    InfoString = ''

    # ------------------------------
    # type
    TypeStr = deployment_file.read(4)
    CalcChecksum = zlib.crc32(TypeStr, 0x00000000) & 0xFFFFFFFF

    # ------------------------------
    # read single-shot part of sample
    SensorsPresent = (TypeStr[3])
    sample_size = ctypes.sizeof(MPESS_Struct.Struct_Sample_Single)

    i = 0
    if Flag_CreateNetCDF:
        dataset = Dataset('MPESS.nc', 'w', format='NETCDF4_CLASSIC')
        dataset.filename = bin_path_filename

        # create dimensions
        time = dataset.createDimension('TIME', None)
        v = dataset.createDimension('vector', 3)
        mat = dataset.createDimension('matrix', 3 * 3)

        # create variables
        times = dataset.createVariable('TIME', np.float64, ('TIME',))

        times.units = 'days since 1950-01-01 00:00:00'
        times.calendar = 'gregorian'

        timems = dataset.createVariable('millisec', np.int32, ('TIME',))
        burst = dataset.createVariable('burst', np.int16, ('TIME',))

        accel = dataset.createVariable('accel', np.float32, ('TIME', 'vector',))
        mag = dataset.createVariable('mag', np.float32, ('TIME', 'vector',))
        gyro = dataset.createVariable('gyro', np.float32, ('TIME', 'vector',))

        orient = dataset.createVariable('orientation', np.float32, ('TIME', 'matrix',))

        if SensorsPresent & MPESS_Struct.SNS_MASK_LT:
            load = dataset.createVariable('load', np.float32, ('TIME',))
        if SensorsPresent & MPESS_Struct.SNS_MASK_PT:
            pres = dataset.createVariable('pressure', np.double, ('TIME',))

    class SampleUnion(ctypes.Union):  # must fill blocks of 4 bytes
        _fields_ = [("Data", MPESS_Struct.Struct_Sample_Single),
                    ("Buffer", ctypes.c_uint8 * sample_size)]

    UN = SampleUnion()
    sample_buffer = deployment_file.read(sample_size)
    if len(sample_buffer) != sample_size:  # incorrect file size
        InfoString += 'Missing Sample Data (%u bytes)' % (sample_size - len(sample_buffer))
        return (False, InfoString, pd.DataFrame())
    CalcChecksum = zlib.crc32(sample_buffer, CalcChecksum) & 0xFFFFFFFF
    for sample_byte_index in range(sample_size):
        UN.Buffer[sample_byte_index] = (sample_buffer[sample_byte_index])
    DateTimeUtc = datetime.datetime.fromtimestamp(UN.Data.StartTime)
                                                  #tz=pytz.UTC)  # convert utc seconds to utc date and time
    data = {}
    #data['Time'] = DateTimeUtc.astimezone(TimezoneLocal)
    data['Time'] = DateTimeUtc
    data['BatteryVoltage'] = UN.Data.BatteryVoltage
    if SensorsPresent & MPESS_Struct.SNS_MASK_IMU:
        data['IMU_TimeStampMs'] = np.NaN
        data['IMU_AccX']        = np.NaN
        data['IMU_AccY']        = np.NaN
        data['IMU_AccZ']        = np.NaN
        data['IMU_GyroX']       = np.NaN
        data['IMU_GyroY']       = np.NaN
        data['IMU_GyroZ']       = np.NaN
        data['IMU_MagnX']       = np.NaN
        data['IMU_MagnY']       = np.NaN
        data['IMU_MagnZ']       = np.NaN
        data['IMU_OM_M11']      = np.NaN
        data['IMU_OM_M12']      = np.NaN
        data['IMU_OM_M13']      = np.NaN
        data['IMU_OM_M21']      = np.NaN
        data['IMU_OM_M22']      = np.NaN
        data['IMU_OM_M23']      = np.NaN
        data['IMU_OM_M31']      = np.NaN
        data['IMU_OM_M32']      = np.NaN
        data['IMU_OM_M33']      = np.NaN
    if SensorsPresent & MPESS_Struct.SNS_MASK_LT:
        data['LineTension'] = np.NaN
    if SensorsPresent & MPESS_Struct.SNS_MASK_PT:
        data['Pressure'] = np.NaN
    data_lists.append(data)  # append
    if Flag_CreateCSV:
        writer_CSV.writerow(
            (DateTimeUtc.strftime('%Y/%m/%d %H:%M:%S'), '%.1f' % data['BatteryVoltage'], '', '', '', '', '', ''))

    # ------------------------------
    # read recursive part of sample
    sample_size = 4                                                                 # SamplesToFollow
    if SensorsPresent & MPESS_Struct.SNS_MASK_LT:   sample_size += 4                # Line Tension
    if SensorsPresent & MPESS_Struct.SNS_MASK_IMU:  sample_size += 4*(1+3+3+3+9)    # IMU (TimeStampMs+Accelerometer+Gyro+Magnetometer+OrientationMatrix)
    if SensorsPresent & MPESS_Struct.SNS_MASK_PT:   sample_size += 4                # Pressure
    TimeDelta = datetime.timedelta(seconds=1 / MPESS_Struct.sample_frequency)
    while True:

        # read from file
        sample_buffer = deployment_file.read(sample_size)
        if len(sample_buffer) != sample_size:  # incorrect file size
            InfoString += 'Missing Sample Data (%u bytes)' % (sample_size - len(sample_buffer))
            return (False, InfoString, pd.DataFrame())
        CalcChecksum = zlib.crc32(sample_buffer, CalcChecksum) & 0xFFFFFFFF

        # create dict from data
        data = {}
        #data['Time'] = DateTimeUtc.astimezone(TimezoneLocal)
        data['Time'] = DateTimeUtc
        data['BatteryVoltage'] = np.NaN
        SamplesToFollow = struct.unpack('>L', sample_buffer[0:4])[0]  # read data into union
        sample_buffer = sample_buffer[4:]
        if SensorsPresent & MPESS_Struct.SNS_MASK_LT:  # Line Tension
            data['LineTension'] = struct.unpack('>f', sample_buffer[0:4])[0]
            sample_buffer = sample_buffer[4:]
        if SensorsPresent & MPESS_Struct.SNS_MASK_IMU:  # IMU
            data['IMU_TimeStampMs'] = struct.unpack('>L', sample_buffer[ 0: 4])[0]
            data['IMU_AccX']        = struct.unpack('>f', sample_buffer[ 4: 8])[0]
            data['IMU_AccY']        = struct.unpack('>f', sample_buffer[ 8:12])[0]
            data['IMU_AccZ']        = struct.unpack('>f', sample_buffer[12:16])[0]
            data['IMU_GyroX']       = struct.unpack('>f', sample_buffer[16:20])[0]
            data['IMU_GyroY']       = struct.unpack('>f', sample_buffer[20:24])[0]
            data['IMU_GyroZ']       = struct.unpack('>f', sample_buffer[24:28])[0]
            data['IMU_MagnX']       = struct.unpack('>f', sample_buffer[28:32])[0]
            data['IMU_MagnY']       = struct.unpack('>f', sample_buffer[32:36])[0]
            data['IMU_MagnZ']       = struct.unpack('>f', sample_buffer[36:40])[0]
            data['IMU_OM_M11']      = struct.unpack('>f', sample_buffer[40:44])[0]
            data['IMU_OM_M12']      = struct.unpack('>f', sample_buffer[44:48])[0]
            data['IMU_OM_M13']      = struct.unpack('>f', sample_buffer[48:52])[0]
            data['IMU_OM_M21']      = struct.unpack('>f', sample_buffer[52:56])[0]
            data['IMU_OM_M22']      = struct.unpack('>f', sample_buffer[56:60])[0]
            data['IMU_OM_M23']      = struct.unpack('>f', sample_buffer[60:64])[0]
            data['IMU_OM_M31']      = struct.unpack('>f', sample_buffer[64:68])[0]
            data['IMU_OM_M32']      = struct.unpack('>f', sample_buffer[68:72])[0]
            data['IMU_OM_M33']      = struct.unpack('>f', sample_buffer[72:76])[0]
            sample_buffer = sample_buffer[76:]
        if SensorsPresent & MPESS_Struct.SNS_MASK_PT:  # Pressure
            data['Pressure'] = struct.unpack('>f', sample_buffer[0:4])[0]
            sample_buffer = sample_buffer[4:]
        data_lists.append(data)  # append

        # append to CSV
        if Flag_CreateCSV:
            if SensorsPresent & MPESS_Struct.SNS_MASK_LT:
                StLT = '%.3f' % data['LineTension']
            else:
                StLT = ''
            if SensorsPresent & MPESS_Struct.SNS_MASK_IMU:
                St_IMU_TimeStampMs  = '%.4f' % data['IMU_TimeStampMs']
                St_IMU_AccX         = '%.4f' % data['IMU_AccX']
                St_IMU_AccY         = '%.4f' % data['IMU_AccY']
                St_IMU_AccZ         = '%.4f' % data['IMU_AccZ']
                St_IMU_GyroX        = '%.4f' % data['IMU_GyroX']
                St_IMU_GyroY        = '%.4f' % data['IMU_GyroY']
                St_IMU_GyroZ        = '%.4f' % data['IMU_GyroZ']
                St_IMU_MagnX        = '%.4f' % data['IMU_MagnX']
                St_IMU_MagnY        = '%.4f' % data['IMU_MagnY']
                St_IMU_MagnZ        = '%.4f' % data['IMU_MagnZ']
                St_IMU_OM_M11       = '%.4f' % data['IMU_OM_M11']
                St_IMU_OM_M12       = '%.4f' % data['IMU_OM_M12']
                St_IMU_OM_M13       = '%.4f' % data['IMU_OM_M13']
                St_IMU_OM_M21       = '%.4f' % data['IMU_OM_M21']
                St_IMU_OM_M22       = '%.4f' % data['IMU_OM_M22']
                St_IMU_OM_M23       = '%.4f' % data['IMU_OM_M23']
                St_IMU_OM_M31       = '%.4f' % data['IMU_OM_M31']
                St_IMU_OM_M32       = '%.4f' % data['IMU_OM_M32']
                St_IMU_OM_M33       = '%.4f' % data['IMU_OM_M33']
            else:
                St_IMU_TimeStampMs  = ''
                St_IMU_AccX         = ''
                St_IMU_AccY         = ''
                St_IMU_AccZ         = ''
                St_IMU_GyroX        = ''
                St_IMU_GyroY        = ''
                St_IMU_GyroZ        = ''
                St_IMU_MagnX        = ''
                St_IMU_MagnY        = ''
                St_IMU_MagnZ        = ''
                St_IMU_OM_M11       = ''
                St_IMU_OM_M12       = ''
                St_IMU_OM_M13       = ''
                St_IMU_OM_M21       = ''
                St_IMU_OM_M22       = ''
                St_IMU_OM_M23       = ''
                St_IMU_OM_M31       = ''
                St_IMU_OM_M32       = ''
                St_IMU_OM_M33       = ''
            if SensorsPresent & MPESS_Struct.SNS_MASK_PT:
                StPt = '%.3f' % data['Pressure']
            else:
                StPt = ''
            RowCSV = [  DateTimeUtc.strftime('%Y/%m/%d %H:%M:%S.%f')[:-5], 
                        '', StPt, StLT,
                        St_IMU_TimeStampMs, 
                        St_IMU_AccX,    St_IMU_AccY,    St_IMU_AccZ, 
                        St_IMU_GyroX,   St_IMU_GyroY,   St_IMU_GyroZ, 
                        St_IMU_MagnX,   St_IMU_MagnY,   St_IMU_MagnZ, 
                        St_IMU_OM_M11,  St_IMU_OM_M12,  St_IMU_OM_M13, 
                        St_IMU_OM_M21,  St_IMU_OM_M22,  St_IMU_OM_M23, 
                        St_IMU_OM_M31,  St_IMU_OM_M32,  St_IMU_OM_M33  ]
            writer_CSV.writerow(RowCSV)

        if Flag_CreateNetCDF:
            times[i] = date2num(DateTimeUtc, units=times.units, calendar=times.calendar)
            accel[i, 0] = data['IMU_AccX']
            accel[i, 1] = data['IMU_AccY']
            accel[i, 2] = data['IMU_AccZ']

            gyro[i, 0] = data['IMU_GyroX']
            gyro[i, 1] = data['IMU_GyroY']
            gyro[i, 2] = data['IMU_GyroZ']

            mag[i, 0] = data['IMU_MagnX']
            mag[i, 1] = data['IMU_MagnY']
            mag[i, 2] = data['IMU_MagnZ']

            orient[i, 0] = data['IMU_OM_M11']
            orient[i, 1] = data['IMU_OM_M12']
            orient[i, 2] = data['IMU_OM_M13']
            orient[i, 3] = data['IMU_OM_M21']
            orient[i, 4] = data['IMU_OM_M22']
            orient[i, 5] = data['IMU_OM_M23']
            orient[i, 6] = data['IMU_OM_M31']
            orient[i, 7] = data['IMU_OM_M32']
            orient[i, 8] = data['IMU_OM_M33']

            timems[i] = data['IMU_TimeStampMs']
            burst[i] = 0

            if SensorsPresent & MPESS_Struct.SNS_MASK_LT:
                load[i] = data['LineTension']
            if SensorsPresent & MPESS_Struct.SNS_MASK_PT:  # Pressure
                pres[i] = data['Pressure']

            i = i + 1

        # all done
        if SamplesToFollow == 0:
            break

        # increse timestamp
        DateTimeUtc += TimeDelta

    if Flag_CreateNetCDF:
        dataset.close()

    # ------------------------------
    # check checksum
    sample_buffer = deployment_file.read(4)
    if len(sample_buffer) != 4:  # incorrect file size
        InfoString += 'Missing Sample Data (%u bytes)' % (4 - len(sample_buffer))
        return (False, InfoString, pd.DataFrame())
    SmpChecksum = struct.unpack('>L', sample_buffer)[0]
    if (SmpChecksum != CalcChecksum):
        InfoString += 'Checksum Error'
        return (False, InfoString, pd.DataFrame())

    # convert list to data frame
    return (True, InfoString, pd.DataFrame(data_lists).set_index('Time'))


# ------------------------------------------------------------------------------
def ProcessSample_Normal(deployment_file, writer_CSV):
    global NrOfSamplesPerStop
    data_lists = []
    InfoString = ''

    # ------------------------------
    # check checksum
    sample_buffer = deployment_file.read(4)
    SensorsPresent = ord(sample_buffer[3])
    #    SensorsPresent = MPESS_Struct.SNS_MASK_IMU | MPESS_Struct.SNS_MASK_PT
    sample_size_single = ctypes.sizeof(MPESS_Struct.Struct_Sample_Single)
    sample_size_rec = 0
    if SensorsPresent & MPESS_Struct.SNS_MASK_LT:   sample_size_rec += 4            # Line Tension
    if SensorsPresent & MPESS_Struct.SNS_MASK_IMU:  sample_size_rec += 4*(1+3+3+3+9)    # IMU (TimeStampMs+Accelerometer+Gyro+Magnetometer+OrientationMatrix)
    sample_size_single2 = 0
    if SensorsPresent & MPESS_Struct.SNS_MASK_PT:   sample_size_single2 += 4
    sample_size = 4 + sample_size_single + NrOfSamplesPerStop * sample_size_rec + sample_size_single2 + 4
    sample_buffer += deployment_file.read(sample_size - 4)
    if len(sample_buffer) != sample_size:  # incorrect file size
        InfoString += 'Missing Data (%u bytes missing)' % (sample_size - len(sample_buffer))
        deployment_file.seek(-(len(sample_buffer) - 1),1)  # move file pointer backwards up to just after sample type bytes
        return (False, InfoString, pd.DataFrame())
    SmpChecksum = struct.unpack('>L', sample_buffer[-4:])[0]
    CalcChecksum = zlib.crc32(sample_buffer[:-4], 0x00000000) & 0xFFFFFFFF
    if (SmpChecksum != CalcChecksum):
        InfoString += 'Checksum Error'
        deployment_file.seek(-(sample_size - 4), 1)  # move file pointer backwards up to just after sample type bytes
        return (False, InfoString, pd.DataFrame())

    # ------------------------------
    # process first single-shot part of sample
    class SampleUnion(ctypes.Union):  # must fill blocks of 4 bytes
        _fields_ = [("Data", MPESS_Struct.Struct_Sample_Single),
                    ("Buffer", ctypes.c_uint8 * sample_size)]

    UN = SampleUnion()
    for sample_byte_index in range(sample_size_single):
        UN.Buffer[sample_byte_index] = ord(sample_buffer[4 + sample_byte_index])
    sample_buffer = sample_buffer[4 + sample_size_single:]
    DateTimeUtc = datetime.datetime.fromtimestamp(UN.Data.StartTime, tz=pytz.UTC)  # convert utc seconds to utc date and time
    data = {}
    #data['Time'] = DateTimeUtc.astimezone(TimezoneLocal)
    data['Time'] = DateTimeUtc
    data['BatteryVoltage'] = UN.Data.BatteryVoltage
    if SensorsPresent & MPESS_Struct.SNS_MASK_IMU:
        data['IMU_TimeStampMs'] = np.NaN
        data['IMU_AccX']        = np.NaN
        data['IMU_AccY']        = np.NaN
        data['IMU_AccZ']        = np.NaN
        data['IMU_GyroX']       = np.NaN
        data['IMU_GyroY']       = np.NaN
        data['IMU_GyroZ']       = np.NaN
        data['IMU_MagnX']       = np.NaN
        data['IMU_MagnY']       = np.NaN
        data['IMU_MagnZ']       = np.NaN
        data['IMU_OM_M11']      = np.NaN
        data['IMU_OM_M12']      = np.NaN
        data['IMU_OM_M13']      = np.NaN
        data['IMU_OM_M21']      = np.NaN
        data['IMU_OM_M22']      = np.NaN
        data['IMU_OM_M23']      = np.NaN
        data['IMU_OM_M31']      = np.NaN
        data['IMU_OM_M32']      = np.NaN
        data['IMU_OM_M33']      = np.NaN
    if SensorsPresent & MPESS_Struct.SNS_MASK_LT:
        data['LineTension'] = np.NaN
    if SensorsPresent & MPESS_Struct.SNS_MASK_PT:
        data['Pressure'] = np.NaN
    data_lists.append(data)  # append
    if Flag_CreateCSV:
        writer_CSV.writerow(
            (DateTimeUtc.strftime('%Y/%m/%d %H:%M:%S'), '%.1f' % data['BatteryVoltage'], '', '', '', '', '', ''))

    # ------------------------------
    # read recursive part of sample
    TimeDelta = datetime.timedelta(seconds=1 / MPESS_Struct.sample_frequency)
    for i in range(NrOfSamplesPerStop):

        # create dict from data
        data = {}
        #data['Time'] = DateTimeUtc.astimezone(TimezoneLocal)
        data['Time'] = DateTimeUtc
        data['BatteryVoltage'] = np.NaN
        if SensorsPresent & MPESS_Struct.SNS_MASK_LT:  # Line Tension
            data['LineTension'] = struct.unpack('>f', sample_buffer[0:4])[0]
            sample_buffer = sample_buffer[4:]
        if SensorsPresent & MPESS_Struct.SNS_MASK_IMU:  # IMU
            data['IMU_TimeStampMs'] = struct.unpack('>L', sample_buffer[ 0: 4])[0]
            data['IMU_AccX']        = struct.unpack('>f', sample_buffer[ 4: 8])[0]
            data['IMU_AccY']        = struct.unpack('>f', sample_buffer[ 8:12])[0]
            data['IMU_AccZ']        = struct.unpack('>f', sample_buffer[12:16])[0]
            data['IMU_GyroX']       = struct.unpack('>f', sample_buffer[16:20])[0]
            data['IMU_GyroY']       = struct.unpack('>f', sample_buffer[20:24])[0]
            data['IMU_GyroZ']       = struct.unpack('>f', sample_buffer[24:28])[0]
            data['IMU_MagnX']       = struct.unpack('>f', sample_buffer[28:32])[0]
            data['IMU_MagnY']       = struct.unpack('>f', sample_buffer[32:36])[0]
            data['IMU_MagnZ']       = struct.unpack('>f', sample_buffer[36:40])[0]
            data['IMU_OM_M11']      = struct.unpack('>f', sample_buffer[40:44])[0]
            data['IMU_OM_M12']      = struct.unpack('>f', sample_buffer[44:48])[0]
            data['IMU_OM_M13']      = struct.unpack('>f', sample_buffer[48:52])[0]
            data['IMU_OM_M21']      = struct.unpack('>f', sample_buffer[52:56])[0]
            data['IMU_OM_M22']      = struct.unpack('>f', sample_buffer[56:60])[0]
            data['IMU_OM_M23']      = struct.unpack('>f', sample_buffer[60:64])[0]
            data['IMU_OM_M31']      = struct.unpack('>f', sample_buffer[64:68])[0]
            data['IMU_OM_M32']      = struct.unpack('>f', sample_buffer[68:72])[0]
            data['IMU_OM_M33']      = struct.unpack('>f', sample_buffer[72:76])[0]
            sample_buffer = sample_buffer[76:]
        if SensorsPresent & MPESS_Struct.SNS_MASK_PT:
            data['Pressure'] = np.NaN
        data_lists.append(data)  # append

        # append to CSV
        if Flag_CreateCSV:
            if SensorsPresent & MPESS_Struct.SNS_MASK_LT:
                StLT = '%.3f' % data['LineTension']
            else:
                StLT = ''
            if SensorsPresent & MPESS_Struct.SNS_MASK_IMU:
                St_IMU_TimeStampMs  = '%.4f' % data['IMU_TimeStampMs']
                St_IMU_AccX         = '%.4f' % data['IMU_AccX']
                St_IMU_AccY         = '%.4f' % data['IMU_AccY']
                St_IMU_AccZ         = '%.4f' % data['IMU_AccZ']
                St_IMU_GyroX        = '%.4f' % data['IMU_GyroX']
                St_IMU_GyroY        = '%.4f' % data['IMU_GyroY']
                St_IMU_GyroZ        = '%.4f' % data['IMU_GyroZ']
                St_IMU_MagnX        = '%.4f' % data['IMU_MagnX']
                St_IMU_MagnY        = '%.4f' % data['IMU_MagnY']
                St_IMU_MagnZ        = '%.4f' % data['IMU_MagnZ']
                St_IMU_OM_M11       = '%.4f' % data['IMU_OM_M11']
                St_IMU_OM_M12       = '%.4f' % data['IMU_OM_M12']
                St_IMU_OM_M13       = '%.4f' % data['IMU_OM_M13']
                St_IMU_OM_M21       = '%.4f' % data['IMU_OM_M21']
                St_IMU_OM_M22       = '%.4f' % data['IMU_OM_M22']
                St_IMU_OM_M23       = '%.4f' % data['IMU_OM_M23']
                St_IMU_OM_M31       = '%.4f' % data['IMU_OM_M31']
                St_IMU_OM_M32       = '%.4f' % data['IMU_OM_M32']
                St_IMU_OM_M33       = '%.4f' % data['IMU_OM_M33']
            else:
                St_IMU_TimeStampMs  = ''
                St_IMU_AccX         = ''
                St_IMU_AccY         = ''
                St_IMU_AccZ         = ''
                St_IMU_GyroX        = ''
                St_IMU_GyroY        = ''
                St_IMU_GyroZ        = ''
                St_IMU_MagnX        = ''
                St_IMU_MagnY        = ''
                St_IMU_MagnZ        = ''
                St_IMU_OM_M11       = ''
                St_IMU_OM_M12       = ''
                St_IMU_OM_M13       = ''
                St_IMU_OM_M21       = ''
                St_IMU_OM_M22       = ''
                St_IMU_OM_M23       = ''
                St_IMU_OM_M31       = ''
                St_IMU_OM_M32       = ''
                St_IMU_OM_M33       = ''
            RowCSV = [  DateTimeUtc.strftime('%Y/%m/%d %H:%M:%S.%f')[:-5], 
                        '', '', StLT,
                        St_IMU_TimeStampMs, 
                        St_IMU_AccX,    St_IMU_AccY,    St_IMU_AccZ, 
                        St_IMU_GyroX,   St_IMU_GyroY,   St_IMU_GyroZ, 
                        St_IMU_MagnX,   St_IMU_MagnY,   St_IMU_MagnZ, 
                        St_IMU_OM_M11,  St_IMU_OM_M12,  St_IMU_OM_M13, 
                        St_IMU_OM_M21,  St_IMU_OM_M22,  St_IMU_OM_M23, 
                        St_IMU_OM_M31,  St_IMU_OM_M32,  St_IMU_OM_M33  ]
            writer_CSV.writerow(RowCSV)


        # increse timestamp
        DateTimeUtc += TimeDelta

    # ------------------------------
    # read single pressure
    if SensorsPresent & MPESS_Struct.SNS_MASK_PT:
        data = {}
        #data['Time'] = DateTimeUtc.astimezone(TimezoneLocal)
        data['Time'] = DateTimeUtc
        data['BatteryVoltage'] = np.NaN
        if SensorsPresent & MPESS_Struct.SNS_MASK_IMU:
            data['IMU_Q0'] = np.NaN
            data['IMU_Q1'] = np.NaN
            data['IMU_Q2'] = np.NaN
            data['IMU_Q3'] = np.NaN
        if SensorsPresent & MPESS_Struct.SNS_MASK_LT:
            data['LineTension'] = np.NaN
        data['Pressure'] = struct.unpack('>f', sample_buffer[:4])[0]
        data_lists.append(data)  # append
        if Flag_CreateCSV:
            if SensorsPresent & MPESS_Struct.SNS_MASK_PT:
                StPt = '%.3f' % data['Pressure']
            else:
                StPt = ''
            writer_CSV.writerow((DateTimeUtc.strftime('%Y/%m/%d %H:%M:%S.%f')[:-5], '', StPt, '', '', '', '', ''))
        sample_buffer = sample_buffer[4:]

    # ------------------------------
    # convert list to data frame
    return (True, InfoString, pd.DataFrame(data_lists).set_index('Time'))



# ------------------------------------------------------------------------------
def ProcessSample_Info(deployment_file):
    global NrOfSamplesPerStop
    global PressureUnits
    global LineTensionUnits
    InfoString = ''

    # ------------------------------
    # discard SampleType
    deployment_file.seek(4, 1)

    # ------------------------------
    # read info sample structure
    sample_size = ctypes.sizeof(MPESS_Struct.Struct_Sample_Info)

    class SampleUnion(ctypes.Union):  # must fill blocks of 4 bytes
        _fields_ = [("Data", MPESS_Struct.Struct_Sample_Info),
                    ("Buffer", ctypes.c_uint8 * sample_size)]

    UN = SampleUnion()
    sample_buffer = deployment_file.read(sample_size)
    if len(sample_buffer) != sample_size:  # incorrect file size
        InfoString += 'Missing Data'
        return (False, InfoString)
    for sample_byte_index in range(sample_size):
        UN.Buffer[sample_byte_index] = (sample_buffer[sample_byte_index])

        # ------------------------------
    # check checksum
    CalcChecksum = zlib.crc32(sample_buffer[:-4], 0x00000000) & 0xFFFFFFFF
    if (UN.Data.Checksum != CalcChecksum):
        InfoString += 'Checksum Error'
        return (False, InfoString)

    # ------------------------------
    NrOfSamplesPerStop = UN.Data.Dep_Norm_NrOfSamples

    # ------------------------------
    InfoString += '---------------------------------------------------\r\n'
    InfoString += 'Logger Info:\r\n'
    InfoString += '  Unit Number   = %u\r\n' % UN.Data.Sys_UnitNumber
    InfoString += '  Serial Number = 0x%08lX\r\n' % UN.Data.Sys_SerialNumber

    InfoString += 'Time:\r\n'
    if (UN.Data.RTC_LastTimeSync > 0):
        InfoString += '  Last time synced = ' + datetime.datetime.utcfromtimestamp(UN.Data.RTC_LastTimeSync).strftime(
            '%Y/%m/%d %H:%M:%S') + ' (UTC)\r\n'
    else:
        InfoString += '  Time has not been set before\r\n'

    InfoString += 'Deployment:\r\n'
    InfoString += '  Deployment Start Time   = ' + datetime.datetime.utcfromtimestamp(UN.Data.Dep_StartTime).strftime(
        '%Y/%m/%d %H:%M:%S') + ' (UTC)\r\n'
    if (UN.Data.Dep_StopTime == 0):
        InfoString += '  Deployment Stop Time    = unknown\r\n'
    else:
        InfoString += '  Deployment Stop Time    = ' + datetime.datetime.utcfromtimestamp(
            UN.Data.Dep_StopTime).strftime('%Y/%m/%d %H:%M:%S') + ' (UTC)\r\n'
    InfoString += '  Intensive Sampling Time = %u min @ %.0fsps (%lu Samples)\r\n' % (
    UN.Data.Dep_Int_NrOfSamples / MPESS_Struct.sample_frequency / 60, MPESS_Struct.sample_frequency,
    UN.Data.Dep_Int_NrOfSamples)
    InfoString += '  Normal Sampling Time    = %u min @ %.0fsps (%lu Sample)\r\n' % (
    UN.Data.Dep_Norm_NrOfSamples / MPESS_Struct.sample_frequency / 60, MPESS_Struct.sample_frequency,
    UN.Data.Dep_Norm_NrOfSamples)
    InfoString += '  Sampling Interval       = %u min\r\n' % (UN.Data.Dep_Norm_SampleInterval / 60)

    InfoString += 'Battery:\r\n'
    if (UN.Data.Batt_ReplaceTime == 0):
        InfoString += '  Battery replace time    = unknown'
    else:
        InfoString += '  Battery replace time    = ' + datetime.datetime.utcfromtimestamp(
            UN.Data.Batt_ReplaceTime).strftime('%Y/%m/%d %H:%M:%S') + ' (UTC)\r\n'

    InfoString += 'Sensors:\r\n'
    SensorsPresentString = '  Sensors Present: '
    if UN.Data.Sns_SensorsPresent & 0b111 == 0: SensorsPresentString += 'None, '
    if UN.Data.Sns_SensorsPresent & 0b100:      SensorsPresentString += 'IMU, '
    if UN.Data.Sns_SensorsPresent & 0b010:      SensorsPresentString += 'Line Tension, '
    if UN.Data.Sns_SensorsPresent & 0b001:      SensorsPresentString += 'Pressure, '
    InfoString += SensorsPresentString[:-2] + '\r\n'
    if UN.Data.Sns_SensorsPresent & 0b100:  # IMU
        if UN.Data.Sns_ModelIMU == MPESS_Struct.SNS_IMU_3DM_GX3_25:
            InfoString += '  IMU Model = Microstrain 3DM-GX3-25\r\n'
        elif UN.Data.Sns_ModelIMU == MPESS_Struct.SNS_IMU_3DM_GX4_25:
            InfoString += '  IMU Model = Microstrain 3DM-GX4-25\r\n'
        else:
            InfoString += '  IMU Model = Unknown\r\n'
    if UN.Data.Sns_SensorsPresent & 0b001:  # pressure
        PressureUnits = ''
        index = 0
        while (index < 8) and (UN.Data.PTi_CalUnits[index] != 0):
            PressureUnits += chr(UN.Data.PTi_CalUnits[index])
            index += 1
        InfoString += '  Pressure Transducer Units = ' + PressureUnits + '\r\n'
    if UN.Data.Sns_SensorsPresent & 0b010:  # Line Tension
        LineTensionUnits = ''
        index = 0
        while (index < 4) and (UN.Data.PTx_CalUnits[index] != 0):
            LineTensionUnits += chr(UN.Data.PTx_CalUnits[index])
            index += 1
        InfoString += '  Line Tension Units = ' + LineTensionUnits + '\r\n'

    InfoString += '---------------------------------------------------\r\n'
    return (True, InfoString)


# ------------------------------------------------------------------------------
def QuaternionToOrientationMatrix(Q):

    # Q to M
    M11 = 2.0 * (Q[0] * Q[0] + Q[1] * Q[1]) - 1.0
    M12 = 2.0 * (Q[1] * Q[2] + Q[0] * Q[3])
    M13 = 2.0 * (Q[1] * Q[3] - Q[0] * Q[2])
    M23 = 2.0 * (Q[2] * Q[3] + Q[0] * Q[1])
    M33 = 2.0 * (Q[0] * Q[0] + Q[3] * Q[3]) - 1.0

    return (M11, M12, M13, M23, M33)

# ------------------------------------------------------------------------------
def OrientationMatrixToEuler(M11, M12, M13, M23, M33):

    # M to Euler
    Pitch = math.asin(-M13)
    Roll = math.atan2(M23, M33)
    Yaw = math.atan2(M12, M11)

    return (Roll, Pitch, Yaw)


# ------------------------------------------------------------------------------
def PlotDeploymentData(df):
    global PressureUnits
    global LineTensionUnits

    DeploymentDuration = df.index[-1] - df.index[0]

    FigNr = 1

    matplotlib.rcParams['agg.path.chunksize'] = 10000  # use this for large data sets

    # ------------------------------
    # pressure
    if 'Pressure' in df.columns.values.tolist():
        plt.figure(FigNr, figsize=(24, 12))
        FigNr += 1
        plt.clf()
        df.Pressure[df.Pressure.notnull()].plot(style='.-b')
        #    plt.ylim(12,14)
        #    plt.ylim(10,15)
        plt.grid()
        plt.xlabel('Time (Local)')
        if DeploymentDuration.days > 0:
            plt.gcf().autofmt_xdate()
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%y/%m/%d %H:%M'))
        plt.ylabel('Pressure [%s]' % PressureUnits)
        plt.title('Pressure during Deployment %u (Unit %u)' % (DeploymentNr, UnitNr))
        plt.tight_layout()
        plt.show()
        if Flag_SavePlots == True:
            plt.savefig(os.path.join(DeploymentFilePath, 'Pressure.png'), bbox_inches='tight', dpi=300)

    # ------------------------------
    # line tension
    if 'LineTension' in df.columns.values.tolist():
        plt.figure(FigNr, figsize=(24, 12))
        FigNr += 1
        plt.clf()
        df.LineTension.plot(style='-')
        plt.grid()
        plt.xlabel('Time (Local)')
        if DeploymentDuration.days > 0:
            plt.gcf().autofmt_xdate()
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%y/%m/%d %H:%M'))
        plt.ylabel('Line Tension [%s]' % LineTensionUnits)
        plt.title('Line Tension during Deployment %u (Unit %u)' % (DeploymentNr, UnitNr))
        plt.tight_layout()
        plt.show()
        if Flag_SavePlots == True:
            plt.savefig(os.path.join(DeploymentFilePath, 'Line Tension.png'), bbox_inches='tight', dpi=300)

    # ------------------------------
    # battery voltage
    if 'BatteryVoltage' in df.columns.values.tolist():
        plt.figure(FigNr, figsize=(24, 12))
        FigNr += 1
        plt.clf()
        df.BatteryVoltage[df.BatteryVoltage.notnull()].plot(style='b-')
        df.BatteryVoltage.plot(style='b.')
        plt.ylim(8, 16)
        plt.grid()
        plt.xlabel('Time (Local)')
        if DeploymentDuration.days > 0:
            plt.gcf().autofmt_xdate()
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%y/%m/%d %H:%M'))
        plt.ylabel('Battery Voltage [V]')
        plt.title('Battery Voltage during Deployment %u (Unit %u)' % (DeploymentNr, UnitNr))
        plt.tight_layout()
        plt.show()
        if Flag_SavePlots == True:
            plt.savefig(os.path.join(DeploymentFilePath, 'Battery Voltage.png'), bbox_inches='tight', dpi=300)

    # ------------------------------
    # IMU
    if 'IMU_TimeStampMs' in df.columns.values.tolist():

        # convert to euler and plot
        LstTime = df.index.tolist()
        LstM11 = df.IMU_OM_M11.tolist()
        LstM12 = df.IMU_OM_M12.tolist()
        LstM13 = df.IMU_OM_M13.tolist()
        LstM23 = df.IMU_OM_M23.tolist()
        LstM33 = df.IMU_OM_M33.tolist()
        LstRoll = []
        LstPitch = []
        LstYaw = []
        for i in range(len(LstM11)):
            (Roll, Pitch, Yaw) = OrientationMatrixToEuler(LstM11[i], LstM12[i], LstM13[i], LstM23[i], LstM33[i])
            LstRoll.append(Roll * 180.0 / math.pi)
            LstPitch.append(Pitch * 180.0 / math.pi)
            LstYaw.append(Yaw * 180.0 / math.pi)

        plt.figure(FigNr, figsize=(24, 12))
        FigNr += 1
        plt.clf()
        ax1 = plt.subplot(3, 1, 1)
        plt.title('IMU Orientation during Deployment %u (Unit %u)' % (DeploymentNr, UnitNr))

        plt.subplot(3, 1, 1)
        plt.plot(LstTime, LstRoll, '-')
        plt.grid(True)
        plt.ylabel('IMU Roll [deg]')

        plt.subplot(3, 1, 2, sharex=ax1)
        plt.plot(LstTime, LstPitch, '-')
        plt.grid(True)
        plt.ylabel('IMU Pitch [deg]')

        plt.subplot(3, 1, 3, sharex=ax1)
        plt.plot(LstTime, LstYaw, '-')
        plt.grid(True)
        plt.ylabel('IMU Yaw [deg]')

        plt.xlabel('Time (Local)')
        if DeploymentDuration.days > 0:
            plt.gcf().autofmt_xdate()
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%y/%m/%d %H:%M'))

        plt.tight_layout()
        plt.show()

        if Flag_SavePlots == True:
            plt.savefig(os.path.join(DeploymentFilePath, 'IMU_Orientation.png'), dpi=300)


        # ------------------------------
        plt.figure(FigNr, figsize=(24, 12))
        FigNr += 1
        plt.clf()
        ax1 = plt.subplot(3, 1, 1)
        plt.title('IMU during Deployment %u (Unit %u)' % (DeploymentNr, UnitNr))

        df.IMU_AccX[df.IMU_AccX.notnull()].plot(style='-')
        df.IMU_AccY[df.IMU_AccY.notnull()].plot(style='-')
        df.IMU_AccZ[df.IMU_AccZ.notnull()].plot(style='-')
        plt.grid()
        plt.xlabel('Time (Local)')
        if DeploymentDuration.days > 0:
            plt.gcf().autofmt_xdate()
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%y/%m/%d %H:%M'))
        plt.ylabel('Acceleration [g]')

        plt.subplot(3, 1, 2, sharex=ax1)
        df.IMU_GyroX[df.IMU_GyroX.notnull()].plot(style='-')
        df.IMU_GyroY[df.IMU_GyroY.notnull()].plot(style='-')
        df.IMU_GyroZ[df.IMU_GyroZ.notnull()].plot(style='-')
        plt.grid(True)
        plt.ylabel('Gyro [rad/sec]')

        plt.subplot(3, 1, 3, sharex=ax1)
        df.IMU_MagnX[df.IMU_MagnX.notnull()].plot(style='-')
        df.IMU_MagnY[df.IMU_MagnY.notnull()].plot(style='-')
        df.IMU_MagnZ[df.IMU_MagnZ.notnull()].plot(style='-')
        plt.grid(True)
        plt.ylabel('Magnetometer [Gauss]')

        plt.tight_layout()
        plt.show()
        if Flag_SavePlots == True:
            plt.savefig(os.path.join(DeploymentFilePath, 'IMU.png'), bbox_inches='tight', dpi=300)

        # ------------------------------
        plt.figure(FigNr, figsize=(24, 12))
        FigNr += 1
        plt.clf()
        ax1 = plt.subplot(2, 1, 1)
        plt.title('IMU Timestamp during Deployment %u (Unit %u)' % (DeploymentNr, UnitNr))

        df.IMU_TimeStampMs[df.IMU_TimeStampMs.notnull()].plot(style='b.')
        plt.grid()
        plt.xlabel('Time (Local)')
        if DeploymentDuration.days > 0:
            plt.gcf().autofmt_xdate()
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%y/%m/%d %H:%M'))
        plt.ylabel('Timestamp [ms]')
        plt.subplot(2, 1, 2, sharex=ax1)
        LstTime = df.index.tolist()
        TimestampMs = df.IMU_TimeStampMs.tolist()
        TimeDelta = []
        for Index in range(len(TimestampMs)-1):
            TimeDelta.append(TimestampMs[Index+1]-TimestampMs[Index])
        plt.plot(LstTime[1:], TimeDelta, '.')
        plt.ylabel('Time Delta [ms]')

        plt.tight_layout()
        plt.show()
        if Flag_SavePlots == True:
            plt.savefig(os.path.join(DeploymentFilePath, 'IMU_Timestamp.png'), bbox_inches='tight', dpi=300)





# ------------------------------------------------------------------------------
#                                       MAIN
# ------------------------------------------------------------------------------

print('\nMPESS Logger (Python Script %s)\r\n' % SoftwareVersion)

# read .bin file
bin_path_filename = os.path.join(DeploymentFilePath, DeploymentFileName)
if not os.path.exists(bin_path_filename):
    print('Error - Incorrect file name (%s)' % bin_path_filename)
else:
    deployment_file = open(bin_path_filename, 'rb')

    # create CSV file
    writer_CSV = []
    if Flag_CreateCSV:
        FilenameCSV = DeploymentFileName.rsplit('.', 1)[0] + '.csv'
        file_CSV = open(os.path.join(DeploymentFilePath, FilenameCSV), 'wt')
        writer_CSV = csv.writer(file_CSV, delimiter=',')

    # create info Sample file
    FilenameInfo = DeploymentFileName.rsplit('Data', 1)[0] + 'Info.txt'
    file_Info = open(os.path.join(DeploymentFilePath, FilenameInfo), 'wt')

    # extract data
    df = ExtractAllSamples(deployment_file, file_Info, writer_CSV)

    # close open files
    deployment_file.close()
    file_Info.close()
    if Flag_CreateCSV:
        file_CSV.close()

    # plot deployemnt data
    if Flag_PlotData:
        time_start = datetime.datetime.now()
        PlotDeploymentData(df)
        print ('Plot time = %s' % (datetime.datetime.now() - time_start))
