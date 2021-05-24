# -*- coding: utf-8 -*-
"""
Created on Tue Jan 29 10:30:38 2019

@author: Eric Russell
Laboratory for Atmospheric Research
Dept. Civil and Environmental Engineering
Washington State University
eric.s.russell@wsu.edu

Script to rename data from a set-list to AF-format using an external column header list.
"""

import pandas as pd
import datetime
def AmeriFlux_Rename(data, outfile, Format, AF, Output, GRADE=False):
    AF = pd.read_csv(AF,header = 0) # File path for where the column names sit from main driver
    AM = data; cls = AM.columns # Keeping data as an unchanged variable from this point forward in case want to do more with it; can be changed
    # Using data that came from EddyPro so selected the Epro column to check column names against.
    Format = Format.upper() # Which format the initial column headers are in; 'Epro' or 'Eflux' are only accepted; must be in single quotes
    s = cls.isin(AF[Format])
    # Drop columns not in the AmeriFlux data list; list can be updated as needed
    AF_Out = AM.drop(AM[cls[~s]],axis = 1)
    cls = AF_Out.columns  #Grab column headers from AF_Out after dropping unneeded columns    
# Change column header names and keep only columns that match
    for k in range (2,len(AF)):
        if AF[Format][k] in cls:
            qn = AF[Format][k] == cls
            AF_Out = AF_Out.rename(columns={cls[qn][0]:AF['AmeriFlux'][k]})
            print('Converting ',AF[Format][k],' to ',AF['AmeriFlux'][k])
# Shift time to match AmeriFlux format; can change this depending on how averaging time is assigned in the base datafile
    AF_Out['TIMESTAMP_END'] = AF_Out.index.shift(0, '30T')
    AF_Out['TIMESTAMP_START'] = AF_Out.index.shift(-1, '30T')    
    AF_Out['TIMESTAMP_START']= AF_Out.TIMESTAMP_START.map(lambda x: datetime.datetime.strftime(x, '%Y%m%d%H%M'))
    AF_Out['TIMESTAMP_END']= AF_Out.TIMESTAMP_END.map(lambda x: datetime.datetime.strftime(x, '%Y%m%d%H%M'))
# Format columns into a same order as in the input *.csv file because housekeeping is always good
    acl = AF['AmeriFlux']
    tt = acl[acl.isin(AF_Out.columns)]
    AF_Out_QC=AF_Out[tt]   
# Use regex for better than hardcoding? Needs conditional to check what system is being used.
    # Convert to 0-1-2 QC grading system per AMeriFlux requirements
    if GRADE:
        AF_Out_QC['FC_SSITC_TEST'] = AF_Out_QC['FC_SSITC_TEST'].astype(float)
        AF_Out_QC['FC_SSITC_TEST'][(AF_Out_QC['FC_SSITC_TEST'] <= 3) & (AF_Out_QC['FC_SSITC_TEST'] >= 0)] = 0
        AF_Out_QC['FC_SSITC_TEST'][(AF_Out_QC['FC_SSITC_TEST'] >= 4) & (AF_Out_QC['FC_SSITC_TEST'] <7)] = 1
        AF_Out_QC['FC_SSITC_TEST'][AF_Out_QC['FC_SSITC_TEST'] >= 7] = 2
    
        AF_Out_QC['H_SSITC_TEST'] = AF_Out_QC['H_SSITC_TEST'].astype(float)
        AF_Out_QC['H_SSITC_TEST'][(AF_Out_QC['H_SSITC_TEST'] <= 3) & (AF_Out_QC['H_SSITC_TEST'] >= 0)] = 0
        AF_Out_QC['H_SSITC_TEST'][(AF_Out_QC['H_SSITC_TEST'] >= 4) & (AF_Out_QC['H_SSITC_TEST'] <7)] = 1
        AF_Out_QC['H_SSITC_TEST'][AF_Out_QC['H_SSITC_TEST'] >= 7] = 2
    
        AF_Out_QC['LE_SSITC_TEST'] = AF_Out_QC['LE_SSITC_TEST'].astype(float)
        AF_Out_QC['LE_SSITC_TEST'][(AF_Out_QC['LE_SSITC_TEST'] <= 3) & (AF_Out_QC['LE_SSITC_TEST'] >= 0)] = 0
        AF_Out_QC['LE_SSITC_TEST'][(AF_Out_QC['LE_SSITC_TEST'] >= 4) & (AF_Out_QC['LE_SSITC_TEST'] <7)] = 1
        AF_Out_QC['LE_SSITC_TEST'][AF_Out_QC['LE_SSITC_TEST'] >= 7] = 2
    #Turn QC grades into ints and -9999; rest of missing data filleds in output call.
        AF_Out_QC['LE_SSITC_TEST'] = AF_Out_QC['LE_SSITC_TEST'].fillna(-9999).astype(int)
        AF_Out_QC['H_SSITC_TEST'] = AF_Out_QC['H_SSITC_TEST'].fillna(-9999).astype(int)
        AF_Out_QC['FC_SSITC_TEST'] = AF_Out_QC['FC_SSITC_TEST'].fillna(-9999).astype(int)
        
#%%
# Format the timestamps to be the first two columns for easier checking
    cols = AF_Out_QC.columns.tolist()
    cols.insert(0,cols.pop(cols.index('TIMESTAMP_START')))
    cols.insert(1,cols.pop(cols.index('TIMESTAMP_END')))
    AF_Out_QC = AF_Out_QC.reindex(columns = cols) #Index columns to be correct
    # Build output file format; time goes from start time to last end time
    outfile = outfile+str(AF_Out['TIMESTAMP_START'][0])+'_'+str(AF_Out['TIMESTAMP_END'][-1])+'.csv'
    if Output == 'CSV':
        AF_Out_QC.to_csv(outfile, index = False, na_rep = -9999) # Output and fill missing data with -9999 as int
        return AF_Out_QC
    else:
        return AF_Out_QC
