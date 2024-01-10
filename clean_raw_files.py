# -*- coding: utf-8 -*-
"""
Created on Mon Jan  8 18:54:12 2024

@author: narju
"""

import pandas as pd
import numpy as np
import re

incsv = r"C:\Users\narju\OneDrive - Intel Corporation\Documents\Device\804_Dispo\D340FCA0_ZTO_Hot_As_FIN_ISO\perf_raw.csv"
dt=pd.read_csv(incsv)

oper = 'ETEST_OPERATION'
lot,lot7 = 'LOT','LOT7'
wid = 'WAFER'
lw = 'LOT7_WID'
xx = 'X'
yy= 'Y'
xy='XY'
lwxy='Lot7WidXY'
devrev = 'ETEST_DEVREVSTEP'
val = 'VALUE'
label = 'TEST_NAME'
struct='STRUCTURE_NAME'
prog='PROGRAM_NAME'
time_etest='TEST_END_DATE@ETEST'
t_m1='DATE@M1'
ww='ETEST_TEST_END_WORK_WEEK'
alias='ALIAS'
val1='VALUE_ABS'


dt[lot7]=dt[lot].apply(lambda x: x[:7])

def clean_arr(dt): #dt is a column
    dt.loc[(abs(dt)>9999.9) & (abs(dt)<10000)]=np.nan
    
    med=dt.median(skipna=True)
    q75=dt.quantile(.75)
    q25=dt.quantile(.25)
    ulim=med+14.8*(q75-med)
    llim=med-14.8*(med-q25)
    
    # dt=dt.set_index([struct,label])
    dt.loc[dt>ulim]=np.nan
    dt.loc[dt<llim]=np.nan
    return dt

def copy_col(d, regex,new_col_name):
    dtf = d.filter(regex=regex, axis=1)
    if not dtf.empty:
        assert len(dtf.columns)==1
        d[new_col_name]=d[dtf.columns]
    # return dtf[dtf.columns]

# data columns
d_numeric = dt.filter(regex='\[.*\].*@ETEST', axis=1)

if not d_numeric.empty:
    # other columns
    d_remainder = dt.drop(d_numeric.columns, axis=1)
    d_numeric=d_numeric.apply(lambda x: clean_arr(x), axis=0)
    # check if IDEFF@OPT exists
    # ideffopt_n = d_numeric.filter(regex='IDEFF.*1\.1\/.*NA\[N(?!.*/)(?!.*CMOS)', axis=1)
    # if not ideffopt_n.empty:
    #     assert len(ideffopt_n.columns)==1
    #     d_numeric['IDEFF@OPT NMOS']=d_numeric[ideffopt_n.columns]
    # ideffopt_p = d_numeric.filter(regex='IDEFF.*1\.1\/.*NA\[P(?!.*/)(?!.*CMOS)', axis=1)
    # if not ideffopt_p.empty:
    #     assert len(ideffopt_p.columns)==1
    #     d_numeric['IDEFF@OPT PMOS']=d_numeric[ideffopt_p.columns]
    copy_col(d_numeric, 'IDEFF.*1\.1\/.*NA\[N(?!.*/)(?!.*CMOS)', 'IDEFF@OPT_N')
    copy_col(d_numeric, 'IDEFF.*1\.1\/.*NA\[P(?!.*/)(?!.*CMOS)', 'IDEFF@OPT_P')
    copy_col(d_numeric, 'COV_0\.5G\[N(?!.*/)(?!.*CMOS)', 'COV_N')
    copy_col(d_numeric, 'COV_0\.5G\[P(?!.*/)(?!.*CMOS)', 'COV_P')
    copy_col(d_numeric, 'CDEL_1\.1G\[N(?!.*/)(?!.*CMOS)', 'CDEL_N')
    copy_col(d_numeric, 'CDEL_1\.1G\[P(?!.*/)(?!.*CMOS)', 'CDEL_P')
    if d_numeric.filter(items=['IDEFF@OPT_N','COV_N','CDEL_N'], axis=1).shape[1]==3:
        d_numeric['CDEL+4COV_N'] = d_numeric['CDEL_N'] + 4*d_numeric['COV_N']
        d_numeric['.I/C N']=d_numeric.apply(lambda x: x['IDEFF@OPT_N']/x['CDEL+4COV_N'], axis=1)
    if d_numeric.filter(items=['IDEFF@OPT_P','COV_P','CDEL_P'], axis=1).shape[1]==3:
            d_numeric['CDEL+4COV_P'] = d_numeric['CDEL_P'] + 4*d_numeric['COV_P']
            d_numeric['.I/C P']=d_numeric.apply(lambda x: x['IDEFF@OPT_P']/x['CDEL+4COV_P'], axis=1)

    
    dt=pd.merge(d_remainder,d_numeric,left_index=True,right_index=True)
elif not dt.filter(items=[val,label,struct,alias], axis=1).empty:
    dt[alias]=dt[alias]+"@ETEST"
    dt['IS_INVALID?']='VALID'
    dt.loc[(abs(dt[val])>9999.9) & (abs(dt[val])<10000), 'IS_INVALID?']='INVALID'
    
    dt[val1]=dt[val]
    dt.loc[(abs(dt[val])>9999.9) & (abs(dt[val])<10000), val1]=np.nan
    
    med=dt.groupby([struct,label])[val1].median(skipna=True)
    q75=dt.groupby([struct,label])[val1].quantile(.75)
    q25=dt.groupby([struct,label])[val1].quantile(.25)
    ulim=med+14.8*(q75-med)
    llim=med-14.8*(med-q25)
    
    dt=dt.set_index([struct,label])
    dt.loc[(dt[val1]>ulim[dt.index]).values,val1]=np.nan
    dt.loc[(dt[val1]<llim[dt.index]).values,val1]=np.nan
    dt.loc[(dt[val1]>ulim[dt.index]).values, 'IS_INVALID?']='HIGH'
    dt.loc[(dt[val1]<llim[dt.index]).values, 'IS_INVALID?']='LOW'
    
    dt=dt.reset_index()
    
    dtlw=dt[[lot7,wid]].groupby([lot7]).min().reset_index()
    dt['LW']=0
    dt=dt.set_index([lot7,wid])
    dt.loc[pd.Index(dtlw),'LW']=1
    dt=dt.reset_index()
 
# 'IDEFF.*1\.1\/.*NA\[P(?!.*/)(?!.*CMOS)'  'IDEFF@OPT PMOS'