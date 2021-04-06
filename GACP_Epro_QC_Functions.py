# -*- coding: utf-8 -*-
"""
Created on Mon Apr  5 09:47:33 2021

@author: russe
"""
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 21 15:53:46 2018

@author: Eric S. Russell
Laboratory for Atmospheric Research
Dept. of Civil and Environmental Engineering
Washington State University
eric.s.russell@wsu.edu
"""
import numpy as np
import pandas as pd
import datetime
"""        
QA/QC processing for flux data:
    Assumes the column headers follow the AmeriFlux format; other formats will cause errors
    Does NOT check for signal strength or other QC/QA variables; assumes the data has already gone through those checks since this is being used for a 
    synthesis project; not base post-processing. there is another post-processing script with a more rigorous QA/QC system that could be used.
    Outputs:
        data:  Dataframe with the filtered data; does not track reason for removing data.     
"""
def Grade_cs(df,info):
    grade = int(info['Val_L']['grade'])
    LE_B = [float(info['Val_L']['LE_B']),float(info['Val_U']['LE_B'])]
    H_B = [float(info['Val_L']['H_B']),float(info['Val_U']['H_B'])]
    F_B = [float(info['Val_L']['F_B']),float(info['Val_U']['F_B'])]
    T_B = [float(info['Val_L']['T_B']),float(info['Val_U']['T_B'])]
    #Assumes AF-based headers system
    gg = ['qc_H','qc_LE','qc_co2_flux','qc_Taue'] 
    cls =['H','LE','co2_flux', 'Tau'] # Can be de-harcoded 
    pd.options.mode.chained_assignment = None  
    if (grade >9) | (grade<1):
        print('Error: Grade number must be between 0-9.')
        return  # 'exit' function and return error 
    Good = None
    data = []; data=pd.DataFrame(data,index=df.index)
    if cls[1] in df.columns: #Check the LE flux against the bounds
        HL = (df[cls[1]].astype(float) < LE_B[0]) | (df[cls[1]].astype(float)>LE_B[1]) | df[cls[1]].astype(float).isnull()
        if gg[1] in df.columns:
            Grade = (df[gg[1]].astype(float) <= grade) & (~HL) & (df['diag_75_mean']==0) & (df['anemometer_diagnostic_mean']==0) # Is a check on the flux grade, the bounds, and irga and sonic diagnostic not reporting issues
        else: Grade = ~HL
        df[cls[1]][~Grade] = np.NaN
        data[cls[1]+'_Flag'] = 0
        data[cls[1]+'_Flag'][~Grade] = 1
    if cls[0] in df.columns: #Check the H flux against the bounds
        HL = (df[cls[0]].astype(float) < H_B[0]) | (df[cls[0]].astype(float)> H_B[1]) | df[cls[0]].astype(float).isnull()
        if gg[0] in df.columns:
            Grade = (df[gg[0]].astype(float) <= grade) & (~HL) & (df['anemometer_diagnostic_mean']==0)
        else: Grade = ~HL
        df[cls[0]][~Grade] = np.NaN
        data[cls[0]+'_Flag'] = 0
        data[cls[0]+'_Flag'][~Grade] = 1
    if cls[2] in df.columns: #Check the FC flux against the bounds
        HL = (df[cls[2]].astype(float) < F_B[0])|(df[cls[2]].astype(float) > F_B[1]) | df[cls[2]].astype(float).isnull()
        if gg[2] in df.columns:
            Grade = (df[gg[2]].astype(float) <= grade) & (~HL) & (df['diag_75_mean']==0) & (df['anemometer_diagnostic_mean']==0)
        else: Grade = ~HL
        df[cls[2]][~Grade] = np.NaN
        data[cls[2]+'_Flag'] = 0
        data[cls[2]+'_Flag'][~Grade] = 1
    if cls[3] in df.columns: #Check TAU against the bounds
        HL = (df[cls[3]].astype(float) < T_B[0])|(df[cls[3]].astype(float) > T_B[1]) | df[cls[3]].astype(float).isnull()
        if gg[3] in df.columns:
            Grade = (df[gg[3]].astype(float) <= grade) & (~HL)
        else: Grade = ~HL
        data[cls[3]+'_Flag'] = 0
        data[cls[3]+'_Flag'][~Grade] = 1
    # Rain Mask if precip is in the dataset but not in standard EddyPro output
    if 'P' in df.columns:
        Precip = (df['P'].astype(float) == 0) | (df['P'].astype(float) == -9999)
        precip = True
        data['P_Flag'] = 0
        data['P_Flag'][~Precip] = 1
    else: precip = False     
    if precip:
        Good = Precip
    if Good is not None:
        if cls[3] in df.columns:
            df[cls[3]][~Good] = np.NaN
        if cls[2] in df.columns:
            df[cls[2]][~Good] = np.NaN
        if cls[1] in df.columns:
            df[cls[1]][~Good] = np.NaN
        if cls[0] in df.columns:
            df[cls[0]][~Good] = np.NaN
    return df, data

#Fills in the blanks spaces with NaN's so the time index is continuous
def indx_fill(df, time):   
    df.index = pd.to_datetime(df.index)
        # Sort index in case it came in out of order, a possibility depending on filenames and naming scheme
    df = df.sort_index()
        # Remove any duplicate times, can occur if files from mixed sources and have overlapping endpoints
    df = df[~df.index.duplicated(keep='first')]
        # Fill in missing times due to tower being down and pad dataframe to midnight of the first and last day
    idx = pd.date_range(df.index[0].floor('D'),df.index[len(df.index)-1].ceil('D'),freq = time)
    df = df.reindex(idx, fill_value=np.NaN)
    return df

# Reads in a directory of files based on the format for either EddyPro or EasyFlux
def Fast_Read(filenames, time, form):
    if len(filenames) == 0:
        print('No Files in directory, check the path name.')
        return  # 'exit' function and return error
    else:
        #Initialize dataframe used within function
        Final = [];Final = pd.DataFrame(Final)
        if form == 'EF':
            for k in range (0,len(filenames)):
                df = pd.read_csv(filenames[k],index_col = 'TIMESTAMP',header= 1,skiprows=[2,3],low_memory=False)
                Final = pd.concat([Final,df], sort = True)
        elif form == 'EP':
            for k in range (0,len(filenames)):
                df = pd.read_csv(filenames[k],header= 1,skiprows=[2],sep=',',low_memory=False)
                Final = pd.concat([Final,df])
            Final.index = Final['date']+' '+Final['time'] # Eddypro outputs both time and date as separate columns
            Final =Final.drop(['filename'],1) # not needed string-based column; gets in the way of converting to floating point
        elif form == 'Biomet':
            for k in range (0,len(filenames)):
                df = pd.read_csv(filenames[k],header= 0,skiprows=[1],sep=',',low_memory=False)
                Final = pd.concat([Final,df])
            Final.index = Final['date']+' '+Final['time'] # Eddypro outputs both time and date as separate columns
        else: 
            print('Format must be either EF or EP')
            return
        # Convert time index
        Final = Final.sort_index()
        Out = indx_fill(Final, time)
    return Out # Return dataframe to main function.    

#Function to despike flux data using the mean and standard deviation method.
def Despike_7(s,ss,x,lab,delta_time, multi):
    an,Tim = [],[]
    while ss < x.index[-1]:
        x_m = np.nanmean(x[ss:s])
        x_s = np.nanstd(x[ss:s])
        x_d = x[ss:s]
        an.append((x_d > (x_m-(multi*x_s))) & (x_d < (x_m+(multi*x_s))))
        ss+= datetime.timedelta(days=delta_time)
        Tim.append((x_d.index))
        s+= datetime.timedelta(days=delta_time)
    qq = np.hstack(an)
    an = pd.DataFrame(qq, columns = [lab])
    an.index = np.hstack(Tim)
    an = an[~an.index.duplicated(keep='first')]
    return an

def Met_QAQC(**kwargs):
    Q = None
    if 'Tair' in kwargs.keys(): # Air temperature
        Tair = pd.DataFrame(kwargs['Tair'])
        Q = Tair; Q = pd.DataFrame(Q); 
        Q['Tair_Hard_Limit'] = (Q[Tair.columns[0]].astype(float) <= 50) & (Q[Tair.columns[0]].astype(float) >= -40) # Bounds check 
        Q['Tair_Change'] = ~(np.abs(Q[Tair.columns[0]].diff() >= 15)) & (np.abs(Q[Tair.columns[0]].diff() != 0)) # Check if data change between each time step
        Q['Tair_Day_Change'] = (Tair.resample('D').mean().diff !=0) # Checks if the daily average changes from zero
        Q['Tair_Filtered'] = Q[Tair.columns[0]][Q['Tair_Hard_Limit'] & Q['Tair_Change'] & Q['Tair_Day_Change']] #Adds filters and booleans together
        Q.drop(columns=[Tair.columns[0]],inplace=True) # Drops the columns that are filtered out; probably a better way to do this
    else:
        print('******Temperature not present******')
    
    if 'RH' in kwargs.keys():
        RH = pd.DataFrame(kwargs['RH']) 
        if Q is None:
            Q = RH; Q = pd.DataFrame(Q)
        else: Q= Q.join(RH)
        Q['RH_Hard_Limit'] = (Q[RH.columns[0]].astype(float) <= 103) & (Q[RH.columns[0]].astype(float) >= 0)
        Q['RH_gt_100'] = (Q[RH.columns[0]].astype(float) >= 100) & (Q[RH.columns[0]].astype(float) <= 103)
        Q['RH_Change'] = (np.abs(Q[RH.columns[0]].astype(float).diff() <= 50)) & (np.abs(Q[RH.columns[0]].diff() != 0))
        Q['RH_Day_Change'] = (RH.resample('D').mean().diff !=0)  
        Q['RH_Filtered'] = Q[RH.columns[0]][Q['RH_Hard_Limit']&Q['RH_Change']& Q['RH_Day_Change']]
        Q['RH_Filtered']['RH_gt_100'] = 100
        # = Q['RH_Filtered'].replace(to_replace=Q['RH_Filtered'][Q['RH_gt_100']], value = 100)
#        Q['RH_Filtered'][Q['RH_gt_100']]=100
        Q.drop(columns=[RH.columns[0]],inplace=True)

    else:
        print('**** RH not present ****')

    if 'P' in kwargs.keys():
        # Pressure checks; converts from pressure to MSLP as well; checks between the two
        P =  pd.DataFrame(kwargs['P']); 
        if Q is None:
            Q = P; Q = pd.DataFrame(Q)
        else: Q= Q.join(P)    
        Q['P_Hard_Limit'] = (Q[P.columns[0]].astype(float) <= 100) &(Q[P.columns[0]].astype(float) >= 80) 
        Q['P_Change'] = (np.abs(Q[P.columns[0]].diff() <= 3.1)) & (np.abs(Q[P.columns[0]].diff() != 0)) 
        Q['P_Filtered'] = Q[P.columns[0]][Q['P_Hard_Limit'] & Q['P_Change']]
        if ('Tair' in kwargs.keys()) & ('z' in kwargs.keys()):
            MSLP = []; 
            H = pd.DataFrame((8.314*(Tair[Tair.columns[0]]+273.15))/(0.029*9.81)/1000) # Scale height
            x = pd.DataFrame(-kwargs['z']/H[H.columns[0]]); 
            MSLP = P[P.columns[0]]/np.exp(x[x.columns[0]]) # Mean Sea Level Pressure
            MSLP = pd.DataFrame(MSLP);MSLP = MSLP.rename(columns={MSLP.columns[0]:"MSLP"})
            Q= Q.join(MSLP)
            Q['MSLP_Hard_Limit'] = (Q[MSLP.columns[0]].astype(float) <= 110) &(Q[MSLP.columns[0]].astype(float) >= 80)
            Q['MSLP_Change'] = (np.abs(Q[MSLP.columns[0]].diff() <= 31)) & (np.abs(Q[MSLP.columns[0]].diff() != 0)) #& (~np.isnan(Q[MSLP.columns[0]].diff())) 
            Q['MSLP_Filtered'] = Q[MSLP.columns[0]][Q['MSLP_Hard_Limit'] & Q['MSLP_Change']]
        else:
            print('**** Mean sea level pressure not present ****')
        Q.drop(columns=[P.columns[0]],inplace=True)
    else:
        print('**** Pressure not present ****')
        

    if 'WS' in kwargs.keys(): # Wind speed
        WS = pd.DataFrame(kwargs['WS'])
        if Q is None:
            Q = WS; Q = pd.DataFrame(Q)
        else: Q= Q.join(WS)
        Q['WS_Hard_Limit'] = (Q[WS.columns[0]].astype(float) < 60) & (Q[WS.columns[0]].astype(float) >= 0)
        Q['WS_Change'] = (np.abs(Q[WS.columns[0]].diff() <= 15)) & (np.abs(Q[WS.columns[0]].diff() != 0)) #& (~np.isnan(Q[WS.columns[0]].diff())) 
        Q['WS_Day_Change'] = (WS.resample('D').mean().diff !=0) 
        Q['WS_Filtered'] = Q[WS.columns[0]][Q['WS_Hard_Limit']&Q['WS_Change']&Q['WS_Day_Change']]
        Q.drop(columns=[WS.columns[0]],inplace=True)
    else:
        print('**** Wind Speed not present ****')
    
    if 'WD' in kwargs.keys(): # Wind direction
        WD = pd.DataFrame(kwargs['WD'])
        if Q is None:
            Q = WD; Q = pd.DataFrame(Q)
        else: Q= Q.join(WD)
        Q['WD_Hard_Limit'] = (Q[WD.columns[0]].astype(float) < 360) & (Q[WD.columns[0]].astype(float) >= 0)
        Q['WD_Change'] =  (np.abs(Q[WD.columns[0]].diff() != 0)) # (~np.isnan(Q[WD.columns[0]].diff())) &
        Q['WD_Filtered'] = Q[WD.columns[0]][Q['WD_Hard_Limit']&Q['WD_Change']]
        Q.drop(columns=[WD.columns[0]],inplace=True)
    else:
        print('**** Wind Direction not present ****')
    
    if 'PAR' in kwargs.keys():
        PAR = pd.DataFrame(kwargs['PAR']); 
        if Q is None:
            Q = PAR; Q = pd.DataFrame(Q)
        else: Q= Q.join(PAR)
        Q['PAR_Hard_Limit'] = (Q[PAR.columns[0]].astype(float) >= 0) & (Q[PAR.columns[0]].astype(float) < 5000)
        Q['PAR_Change'] = (np.abs(Q[PAR.columns[0]].diff() <= 1500))# & (~np.isnan(Q[PAR.columns[0]].diff()))
        Q['PAR_Day_Change'] = (PAR.resample('D').mean().diff != 0) # Causing problems for some reason
        Q['PAR_Filtered'] = Q[PAR.columns[0]][Q['PAR_Hard_Limit']&Q['PAR_Change']&Q['PAR_Day_Change']]
        Q.drop(columns=[PAR.columns[0]],inplace=True)
    else:
        print('**** PAR not present ****')
    
    if 'Rn' in kwargs.keys():
        Rn = pd.DataFrame(kwargs['Rn'])    
        if Q is None:
            Q = Rn; Q = pd.DataFrame(Q)
        else: Q= Q.join(Rn)
        Q['Rn_Hard_Limit'] = (Q[Rn.columns[0]].astype(float) >= -150) & (Q[Rn.columns[0]].astype(float) <= 1500)       
        Q['Rn_Change'] = (np.abs(Q[Rn.columns[0]].astype(float).diff() <= 500)) & (np.abs(Q[Rn.columns[0]].diff() != 0)) #& (~np.isnan(Q[Rn.columns[0]].astype(float).diff()))   
        Q['Rn_Day_Change'] = (Rn.resample('D').mean().diff !=0) 
        Q['Rn_Filtered'] = Q[Rn.columns[0]][Q['Rn_Hard_Limit']&Q['Rn_Change']&Q['Rn_Day_Change']]
        Q.drop(columns=[Rn.columns[0]],inplace=True)
    else:
        print('**** Net Radiations not present ****')
    
    if 'Precip' in kwargs.keys(): # Lot of filters because of the difference of precip is there is or is not RH and check for frozen precip with temperature as the tipping bucket is bad with snow
        Precip = pd.DataFrame(kwargs['Precip'])
        if Q is None:
            Q = P; Q = pd.DataFrame(Q)
        else: Q= Q.join(Precip)
        Q['Precip_Hard_Limit'] = (Q[Precip.columns[0]].astype(float) < 100) & (Q[Precip.columns[0]].astype(float) >= 0)
        Z_Precip = Q[Precip.columns[0]].astype(float) ==0
        if ('RH' in kwargs.keys()) & ('Tair' in kwargs.keys()): # Checks for temp and RH in correct ranges.
            Q['Precip_RH_gt_90'] = (Q[Precip.columns[0]].astype(float) > 0) & (Q['RH_Filtered'].astype(float) >= 90)
            Q['Precip_Tair_lt_Zero'] = (Q[Precip.columns[0]].astype(float) > 0) & (Q['Tair_Filtered'] < 0)
            Q['Precip_Filtered'] = Q[Precip.columns[0]][Q['Precip_Hard_Limit']&Q['Precip_RH_gt_90']&~Q['Precip_Tair_lt_Zero']]
            Q['Precip_Filtered'][Z_Precip] = 0
            # = Q['Precip_Filtered'].replace(to_replace=Q['Precip_Filtered'][Z_Precip], value = 0)
            Q.drop(columns=[Precip.columns[0]],inplace=True)
        elif ('RH' in kwargs.keys()) & ('Tair' not in kwargs.keys()):
            Q['Precip_RH_gt_90'] = (Q[Precip.columns[0]].astype(float) > 0) & (Q['RH_Filtered'].astype(float) >= 90)
            Q['Precip_Filtered'] = Q[Precip.columns[0]][Q['Precip_Hard_Limit']&Q['Precip_RH']]
            Q['Precip_Filtered'][Z_Precip] = 0
            Q.drop(columns=[Precip.columns[0]],inplace=True)
        elif ('RH' not in kwargs.keys()) & ('Tair' in kwargs.keys()):
            Q['Precip_Tair_lt_Zero'] = (Q[Precip.columns[0]].astype(float) > 0) & (Q['Tair_Filtered'] < 0)
            Q['Precip_Filtered'] = Q[Precip.columns[0]][Q['Precip_Hard_Limit']& ~Q['Precip_Tair_lt_Zero']]
            Q['Precip_Filtered'][Z_Precip] = 0
            Q.drop(columns=[Precip.columns[0]],inplace=True)
        else:
            Q['Precip_Filtered'] = Q[Precip.columns[0]][Q['Precip_Hard_Limit']]
            Q['Precip_Filtered'][Z_Precip] = 0
            Q.drop(columns=[Precip.columns[0]],inplace=True)
    else:
        print('**** Precipitation not present ****')
    
    if 'VPD' in kwargs.keys():
        VPD = pd.DataFrame(kwargs['VPD'])
        if Q is None:
            Q = VPD; Q = pd.DataFrame(Q)
        else: Q= Q.join(VPD)
        Q['VPD_Hard_Limit'] = (Q[VPD.columns[0]].astype(float) < 50) & (Q[VPD.columns[0]].astype(float) >= 0)
        Q['VPD_Change'] = (np.abs(Q[VPD.columns[0]].astype(float).diff() <= 10)) & (np.abs(Q[VPD.columns[0]].diff() != 0)) 
        Q['VPD_Day_Change'] = (VPD.resample('D').mean().diff !=0) 
        Q['VPD_Filtered'] = Q[VPD.columns[0]][Q['VPD_Hard_Limit']&Q['VPD_Change']&Q['VPD_Day_Change']]
        Q.drop(columns=[VPD.columns[0]],inplace=True)
    if 'e' in kwargs.keys():
        e = pd.DataFrame(kwargs['e'])
        if Q is None:
            Q = e; Q = pd.DataFrame(Q)
        else: Q= Q.join(e)
        Q['e_Hard_Limit'] = (Q[e.columns[0]].astype(float) < 50) & (Q[e.columns[0]].astype(float) >= 0)
        Q['e_Change'] = (np.abs(Q[e.columns[0]].astype(float).diff() <= 10)) & (np.abs(Q[e.columns[0]].diff() != 0)) 
        Q['e_Day_Change'] = (e.resample('D').mean().diff !=0) 
        Q['e_Filtered'] = Q[e.columns[0]][Q['e_Hard_Limit']&Q['e_Change']&Q['e_Day_Change']]
        Q.drop(columns=[e.columns[0]],inplace=True)
    if 'e_s' in kwargs.keys():
        e_s = pd.DataFrame(kwargs['e_s'])
        if Q is None:
            Q = e_s; Q = pd.DataFrame(Q)
        else: Q= Q.join(e_s)
        Q['e_s_Hard_Limit'] = (Q[e_s.columns[0]].astype(float) < 50) & (Q[e_s.columns[0]].astype(float) >= 0)
        Q['e_s_Change'] = (np.abs(Q[e_s.columns[0]].astype(float).diff() <= 10)) & (np.abs(Q[e_s.columns[0]].diff() != 0)) 
        Q['e_s_Day_Change'] = (e_s.resample('D').mean().diff !=0) 
        Q['e_s_Filtered'] = Q[e_s.columns[0]][Q['e_s_Hard_Limit']&Q['e_s_Change']&Q['e_s_Day_Change']]        
        Q.drop(columns=[e_s.columns[0]],inplace=True)
    return Q