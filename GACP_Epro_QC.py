# -*- coding: utf-8 -*-
"""
Created on Mon Apr  5 09:03:49 2021

@author: russe
"""
# -*- coding: utf-8 -*-
"""
Created on Mon Oct  1 16:22:52 2018

@author: Eric S. Russell
Laboratory for Atmospheric Research
Dept. of Civil and Environmental Engineering
Washington State University
eric.s.russell@wsu.edu
"""
import re
import pandas as pd
import glob
import os
import datetime
import warnings
# Change this path to the directory where the LTAR_Flux_QC.py file is located
os.chdir(r'C:\Users\russe\Documents\GitHub\GACP_Flux_Processing')       
import GACP_Epro_QC_Functions as LLT
import AF_Rename as AFN
import Reddy_Format as REF

#Change this
files = glob.glob(r'C:\Users\russe\Documents\GitHub\GACP_Flux_Processing\Data\Agg\*.csv')

# File with upper and lower limits for the flux values for each site based on visual inspection of each dataset
info = pd.read_csv(r'C:\Users\russe\Documents\GitHub\GACP_Flux_Processing\Bound_Limits_EPro.csv', index_col = 'Var', header = 0)
AF_Cols=r'C:\Users\russe\Documents\GitHub\GACP_Flux_Processing\AF_EP_EF_Column_Renames.csv'

#%%files
for K in range (0,len(files)):
#Read in data and concat to one dataframe; no processing until data all read in - assumes data is from AmeriFlux or in the format that was defined by the group for data requests
    df = pd.read_csv(files[K],header= 0,sep=',',low_memory=False)
    dt = []
    nme = files[K] # These values change with filepath; still need to de-hardcode this value
    nme = re.sub(r'\-', '', nme) #Remove the dashes from the date start and end points
    df.index= pd.to_datetime(df['TIMESTAMP'])
    # df = df.astype(float)
    df = LLT.indx_fill(df,'30min') # Fill in and missing half-hours in the dataset to have a continuous data set from start time to end.
    #%%
    
    data_qc, data_flags = LLT.Grade_cs(df, info) #QC flux data; new data to use is the data_qc dataframe; will be the one AF_Rename cues from  
    # Works to here for the moment; not sure what the hell is going on.
    qn = (data_qc['air_temperature']>150)&(data_qc['air_temperature']<400) #Check to seee if temperature is in Kelvin or not
    data_qc['air_temperature'][qn] = data_qc['air_temperature'][qn] - 273.15 # Convert temperature to Celsius
    
    data_met = LLT.Met_QAQC(Tair=data_qc['air_temperature'], RH=data_qc['RH'].astype(float), VPD=data_qc['VPD'].astype(float), e=data_qc['e']/100, es=data_qc['es']/100, WS=data_qc['wind_speed'], 
                            WD=data_qc['wind_dir'], P = data_qc['air_pressure']/1000, SW_In = data_qc['SW_in_Avg'], SW_Out = data_qc['SW_out_Avg'], LW_In = data_qc['LW_in_Avg'],
                            LW_Out = data_qc['LW_out_Avg'], Rn = data_qc['Rn'], PAR=data_qc['PAR_density_Avg'])
    
    Data_O = pd.concat([data_qc, data_flags,data_met],axis = 1, sort=False)
    
    s = Data_O.index[0]; ss = s
    s+= datetime.timedelta(days=5)
    with warnings.catch_warnings(): #Despike function for turbulent fluxes
        warnings.simplefilter("ignore", category=RuntimeWarning)
        FC = LLT.Despike_7(s,ss,Data_O['co2_flux'].astype(float),'FC',5,3)
        LE = LLT.Despike_7(s,ss,Data_O['LE'].astype(float),'LE',5,3)
        H = LLT.Despike_7(s,ss,Data_O['H'].astype(float),'H',5,3)
      
    Data_O['LE'] = Data_O['LE'][LE['LE']] 
    Data_O['H'] = Data_O['H'][H['H']] 
    Data_O['co2_flux'] = Data_O['co2_flux'][FC['FC']] 
    
    AF_O = AFN.AmeriFlux_Rename(Data_O, info['Val_L']['outfile'], 'EPRO', AF_Cols, 'CSV', GRADE=False)
    Data_O.to_csv('C:\\Users\\russe\\Documents\\GitHub\\GACP_Flux_Processing\\'+ files[K][62:-4] + '_Processed_Test.csv', index=False)
    
    
    # ReddyProc based data is missing either PAR or incoming shortwave radiation to be able to gapfill; otherwise should be easy enough to make happen with the current data set.
    FileName = 'C:\\Users\\russe\\Desktop\\Projects\\Active\\GACP\\QC_Gapped_'+files[K][62:67]+'REddy_format.txt'
    REF.REddy_Format(AF_O, FileName,'Start_AF') # Function that format
    
    
    df = [];data_qc = []
    
