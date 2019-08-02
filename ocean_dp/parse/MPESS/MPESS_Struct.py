#import  struct                      #pack and unpack dinary data
from ctypes import *             #c struct and union routines

#Hardware Platform
SYS_PLATFORM_LOGGER = 1		# Logger
SYS_PLATFORM_MINI   = 2		# Mini-Logger

#sample rate
sample_frequency = 10.0      #Hz

# Sample Type
SAMPLE_TYPE_INFO        = 0x5A5A5A5A
SAMPLE_TYPE_INTENSIVE   = 0x55AA0000    #lower 4 bits indicate sensors present
SAMPLE_TYPE_NORMAL      = 0xAA550000    #lower 4 bits indicate sensors present

SNS_MASK_IMU = 0b100
SNS_MASK_LT  = 0b010
SNS_MASK_PT  = 0b001

SNS_IMU_3DM_GX3_25 = 3
SNS_IMU_3DM_GX4_25 = 4

#------------------------------------------------------------------------------

#--------------------
# 100 bytes
class Struct_Sample_Info(BigEndianStructure):                   # must fill blocks of 4 bytes
    _fields_ = [('Sys_OppMode',                 c_uint32),      # Operational Mode
                ('Sys_SerialNumber',            c_uint32),      # Unique Serial Number of logger
                ('Sys_Platform',                c_uint8),       # Platform (1=Logger, 2=Mini-Logger)
                ('Sys_UnitNumber',              c_uint8),       # Unit Serial Number
                ('Dep_Number',                  c_uint16),      # Deployment Number (this should not be changed)
                ('Dep_Int_NrOfSamples',         c_uint32),      # Number of samples to record during an Intensive sampling period
                ('Dep_Norm_SampleInterval',     c_uint32),      # Sample Interval [sec]
                ('Dep_Norm_NrOfSamples',        c_uint32),      # Number of samples to record during a normal Sampling period
                ('Dep_StartTime',               c_uint32),      # Start time of deployment in sec since 1970
                ('Dep_StopTime',                c_uint32),      # Deployment stop time in sec since 1970
                ('RTC_LastTimeSync',            c_uint32),      # Last time when the RTC time was corrected [sec since 1970]
                ('RTC_DriftSec',                c_int32),       # RTC Drift rate in seconds (Positive = fast)
                ('RTC_DriftInterval',           c_uint32),      # Total time for drift rate measurement [sec]
                ('RTC_Calibration',             c_int8),        # Signed 5 bit RTC calibration value (fast = negative value)
                ('Sns_SensorsPresent',          c_uint8),       # Sensort Present in System (IMU / Line Tension / Pressure)
                ('Sns_IMU_Model',               c_uint8),       # IMU Model Number ('3' = GX3 or '4' = GX4)
                ('Unused',                      c_uint8*3),     # IMU Model Number ('3' = GX3 or '4' = GX4)
                ('Sns_LC_Serial',               c_uint16),      # LoadCell Serial Number
                ('Sns_IMU_Serial',              c_uint32),      # Last 9 digits of IMU serial number (xxxx-xxxx-xxxxx)
                ('Batt_ReplaceTime',            c_uint32),      # Time when battery was replaced in sec since 1970
                ('Batt_TotalSampleCnt',         c_uint32),      # Total number of samples recorded with current battery
                ('Batt_VoltCal',                c_float*3),     # Battery Voltage Calibration Coefficients
                ('PT_CalUnits',                 c_uint8*8),     # Internal Pressure Transducer Calibration Units String
                ('PT_Cal',                      c_float*3),     # Internal Pressure Transducer Calibration Coefficients
                ('Checksum',                    c_uint32)]      # Checksum

#--------------------
# 104 bytes
class Struct_Sample_Start(BigEndianStructure):                 # must fill blocks of 4 bytes
    _fields_ = [('StartTime',               c_uint32),          # Sample start time (sec since 1970)
                ('BatteryVoltage_Start',    c_float)]           # Battery Voltage [V]





#------------------------------------------------------------------------------
