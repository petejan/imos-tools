# Copyright (C) 2020 Ben Weeding
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
import numpy.ma as ma
import sys
from netCDF4 import Dataset, num2date
from dateutil import parser
import numpy as np
import argparse
import glob
import pytz
import os
import matplotlib.pyplot as plt
from matplotlib import colors
from matplotlib.ticker import PercentFormatter
from sigfig import round

deployments = []

for x in os.listdir("/Users/tru050/Desktop/cloudstor/Shared/SOTS-Temp-Raw-Data"):
    
    if ('Pulse' in x) or ('SOFS' in x):
        
        deployments.append(x)



# check for in water test in history of netcdf file, if not perform the test

netcdffiles = []

mins=[]

maxs=[]

all_dtemp_dtime = np.array([])

all_dtemp_dtime_deps = []

for root, dirs, files in os.walk("/Users/tru050/Desktop/cloudstor/Shared/SOTS-Temp-Raw-Data"):
    
    for fname in files:
      
        if fname.endswith('.nc') and 'FV01' in fname:
        
            print(fname)  #Here, the wanted file name is printed

      
            nc = Dataset(os.path.join(root,fname), mode = 'r')
            
            if 'TEMP_quality_control' in list(nc.variables) and np.array(nc.variables['TEMP'][:]).ndim == 1 and nc.variables['TIME'].getncattr('units') =='days since 1950-01-01 00:00:00 UTC':
                
                # Calculate temperature changes
                nc_temp_diffs = np.diff(np.array(nc.variables['TEMP'][np.array(nc.variables['TEMP_quality_control'][:])!=7]))
                
                # Extract the time data
                nc_time = np.array(nc.variables['TIME'][np.array(nc.variables['TEMP_quality_control'][:])!=7])
            
                # Convert from days to hours
                nc_time_hr = nc_time*24
                
                # Calculate time changes
                nc_time_hr_diffs = np.diff(nc_time_hr)
                
                # Calculate the rate of change of temperature wrt time
                nc_dtemp_dtime = np.divide(nc_temp_diffs,nc_time_hr_diffs)
                
                # Add the results for this netcdf to the record for all files
                all_dtemp_dtime = np.concatenate((all_dtemp_dtime,nc_dtemp_dtime))
                
                all_dtemp_dtime_deps += ([nc.deployment_code] * len(nc_dtemp_dtime))
                
                netcdffiles.append(fname)
                
                mins.append(np.amin(nc_dtemp_dtime))
                
                maxs.append(np.amax(nc_dtemp_dtime))
            
            nc.close()


fig, ax = plt.subplots()

bins = np.linspace(-450,450,901)

line_thick = 0.5

counts,bins,bars = ax.hist(all_dtemp_dtime,bins,log=True)                   

ax.axvline(x=3*np.std(all_dtemp_dtime),color='r',linewidth=line_thick) 

ax.axvline(x=-3*np.std(all_dtemp_dtime),color='r',linewidth=line_thick) 

ax.set_title('Hourly temp changes from all FV01 files in SOTS-TEMP-Raw_Data')    

label_coords = (0.01, 0.9)

label_method = 'axes fraction'

ax.annotate('~1.84E7 measurements',xy=label_coords, xycoords=label_method)



def last_four(entry):
    
    output = entry[-4::]
    
    return output


deployments = []

for x in os.listdir("/Users/tru050/Desktop/cloudstor/Shared/SOTS-Temp-Raw-Data"):
    
    if ('Pulse' in x) or ('SOFS' in x):
        
        deployments.append(x)

deployments.sort(key=last_four)


#######




all_deployment_dtemp_dtime = [None] * len(deployments)

for current_deployment, plt_idx in zip(deployments, range(0,len(deployments))):
    
    print('current deployment is '+current_deployment)
    
    deployment_dtemp_dtime = np.array([])
    
    for root, dirs, files in os.walk("/Users/tru050/Desktop/cloudstor/Shared/SOTS-Temp-Raw-Data"):
    
        for fname in files:
          
            if current_deployment in fname and fname.endswith('.nc') and 'FV00' in fname:
            
                #print(fname)  #Here, the wanted file name is printed

                nc = Dataset(os.path.join(root,fname), mode = 'r')
                
                if 'TEMP' in nc.variables and np.array(nc.variables['TEMP'][:]).ndim == 1 and nc.variables['TIME'].getncattr('units') =='days since 1950-01-01 00:00:00 UTC':
                    
                    time_var = nc.variables["TIME"]
                    
                    time = num2date(time_var[:], units=time_var.units, calendar=time_var.calendar)
                
                    time_deploy = parser.parse(nc.time_deployment_start, ignoretz=True)
                    
                    time_recovery = parser.parse(nc.time_deployment_end, ignoretz=True)
                    
                    #print('using '+fname)
                    
                    temp_extract = np.array(nc.variables['TEMP'][:][(time >= time_deploy) | (time <= time_recovery)])
                    
                    # Calculate temperature changes
                    nc_temp_diffs = np.diff(temp_extract)
                    
                    # Extract the time data
                    nc_time = np.array(nc.variables['TIME'][:][(time >= time_deploy) | (time <= time_recovery)])
                
                    # Convert from days to hours
                    nc_time_hr = nc_time*24
                    
                    # Calculate time changes
                    nc_time_hr_diffs = np.diff(nc_time_hr)
                    
                    # Calculate the rate of change of temperature wrt time
                    nc_dtemp_dtime = np.divide(nc_temp_diffs,nc_time_hr_diffs)
                    
                    # Add the results for this netcdf to the record for the deployment
                    deployment_dtemp_dtime = np.concatenate((deployment_dtemp_dtime,nc_dtemp_dtime))
                    
                    all_deployment_dtemp_dtime[plt_idx] = deployment_dtemp_dtime
                
                nc.close()
                
                






fig, ax = plt.subplots(4,4)

ax=ax.flatten()             

line_thick = 1  

label_coords = (0.6, 0.6)
label_method = 'axes fraction' 
                
for plt_idx,dep_name in zip(range(0,len(deployments)),deployments):            

    print('plotting '+ str((all_deployment_dtemp_dtime[plt_idx])) + ' values')
            
    hist_data = ax[plt_idx].hist(all_deployment_dtemp_dtime[plt_idx],21,log=True)
    
    ax[plt_idx].set_title(dep_name,fontsize=10) 
    
    #ax[plt_idx].axvline(x=3*np.mean(all_deployment_dtemp_dtime[plt_idx]),color='g',linewidth=line_thick)
    
    ax[plt_idx].axvline(x=np.mean(all_deployment_dtemp_dtime[plt_idx])+3*np.std(all_deployment_dtemp_dtime[plt_idx]),color='r',linewidth=line_thick) 

    ax[plt_idx].axvline(x=np.mean(all_deployment_dtemp_dtime[plt_idx])-3*np.std(all_deployment_dtemp_dtime[plt_idx]),color='r',linewidth=line_thick) 
    
    anno = 'mean = '+str(round(float(np.mean(all_deployment_dtemp_dtime[plt_idx])),sigfigs=3))
    
    anno += '\n3SD = ' + str(round(float(3*np.std(all_deployment_dtemp_dtime[plt_idx])),sigfigs=3))
    
    anno += '\nsamples = ' + str(len(all_deployment_dtemp_dtime[plt_idx]))
    
    ax[plt_idx].annotate(anno,xy=label_coords, xycoords=label_method,fontsize=8)
    
    #ax[plt_idx].set_ylim(bottom=0,top=np.max(hist_data[0]))
    
    #ax[plt_idx].set_xlim(left=-450, right=450)      np.linspace(-450,450,901)
    
#ax[-1].axis('off')
    
all_data = np.concatenate(all_deployment_dtemp_dtime)    
    
hist_data = ax[15].hist(all_data,21,log=True)

ax[15].set_title('All data',fontsize=10) 

#ax[plt_idx].axvline(x=3*np.mean(all_deployment_dtemp_dtime[plt_idx]),color='g',linewidth=line_thick)

ax[15].axvline(x=np.mean(all_data)+3*np.std(all_data),color='r',linewidth=line_thick) 

ax[15].axvline(x=np.mean(all_data)-3*np.std(all_data),color='r',linewidth=line_thick) 

anno = 'mean = '+str(round(float(np.mean(all_data)),sigfigs=3))

anno += '\n3SD = ' + str(round(float(3*np.std(all_data)),sigfigs=3))

anno += '\nsamples = ' + str(len(all_data))

ax[15].annotate(anno,xy=label_coords, xycoords=label_method,fontsize=8)

    
fig.subplots_adjust(left=0.05,right=0.99,bottom=0.1,top=0.9,wspace=0.15,hspace=0.4)














files = glob.glob('*.nc')

temp_diffs = np.array([])

for current_file in files:
    
    nc = Dataset(current_file,mode='r')
    
    temp_diffs = np.concatenate((temp_diffs,np.diff(np.array(nc.variables['TEMP'][:]))))

fig, ax = plt.subplots()

ax.hist(temp_diffs,100,log=True)




# use os.walk??? to run in each netcdf folder?? os.scandir()?





# sofs75_60m = Dataset('IMOS_ABOS-SOTS_T_20180801_SOFS_FV00_SOFS-7.5-2018-Starmon-mini-4051-60m_END-20190331_C-20200204.nc',mode='r')
# sofs75_70m = Dataset('IMOS_ABOS-SOTS_T_20180801_SOFS_FV00_SOFS-7.5-2018-Starmon-mini-4052-70m_END-20190331_C-20200204.nc',mode='r')
# sofs75_75m = Dataset('IMOS_ABOS-SOTS_T_20180801_SOFS_FV00_SOFS-7.5-2018-Starmon-mini-4053-75m_END-20190331_C-20200204.nc',mode='r')


# temp_60 = np.array(sofs75_60m.variables['TEMP'][:])
# temp_70 = np.array(sofs75_70m.variables['TEMP'][:])
# temp_75 = np.array(sofs75_75m.variables['TEMP'][:])

# label_coords = (0.01, 0.85)
# label_method = 'axes fraction'

# fig, axs = plt.subplots(3, 1, sharey=True)
# axs[0].set_title('Temp sensor comparison SOFS7.5')

# axs[0].hist(np.diff(temp_60),bins=100,log=True, histtype='bar', stacked=True)
# axs[0].set_ylim(bottom=0.1,top=10E5)
# axs[0].set_xlim(left=-40, right=40)
# axs[0].annotate('60m',xy=label_coords, xycoords=label_method)
# axs[0].tick_params(labelbottom=False)

# axs[1].hist(np.diff(temp_70),bins=100,log=True)
# axs[1].set_ylim(bottom=0.1,top=10E5)
# axs[1].set_xlim(left=-40, right=40)
# axs[1].annotate('70m',xy=label_coords, xycoords=label_method)
# axs[1].tick_params(labelbottom=False)

# axs[2].hist(np.diff(temp_75),bins=100,log=True)
# axs[2].set_ylim(bottom=0.1,top=10E5)
# axs[2].set_xlim(left=-40, right=40)
# axs[2].annotate('75m',xy=label_coords, xycoords=label_method)



# fig.savefig('test.pdf')






