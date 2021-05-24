# -*- coding: utf-8 -*-
"""
Created on Fri May  7 10:11:28 2021

@author: russe
"""
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 25 15:40:25 2019

@author: Eric
"""

import pandas as pd

df = pd.read_csv(r'C:\Users\russe\Documents\GitHub\GACP_Flux_Processing\Data\Williford_GACP_Combined20190404_20210106.csv',header = 0, index_col = 'TIMESTAMP', low_memory=False)
SW = pd.read_csv(r'C:\Users\russe\Documents\GitHub\GACP_Flux_Processing\Data\Williford_GACP_Combined_Flux20190403_20210106.csv',header = 0, index_col = 'TIMESTAMP', low_memory=False)
SW.index = pd.to_datetime(SW.index)
df.index=pd.to_datetime(df.index) # Time-based index
#%

#%%
df['SW_in_Avg'] = SW['R_SW_in_Avg']
df['SW_out_Avg'] = SW['R_SW_out_Avg']
df['LW_in_Avg'] = SW['R_LW_in_Avg']
df['LW_out_Avg'] = SW['R_LW_out_Avg']
df['PAR_density_Avg'] = SW['PAR_density_Avg']
df['Rn'] = SW['Rn']
df['RH'] = SW['RH_Avg']
df['VPD'] = (SW['VPD_air'].astype(float))*10

df.to_csv(r'C:\Users\russe\Documents\GitHub\GACP_Flux_Processing\Data\Williford_GACP_FLUX_EPRO_Combined_20210524.csv',index= True)