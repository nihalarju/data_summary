# -*- coding: utf-8 -*-
"""
Created on Mon Jul 17 11:02:14 2023

@author: narju
"""

import pandas as pd
# from win32com.client import Dispatch
import os
import re

# jmpfile=r"C:\Users\narju\OneDrive - Intel Corporation\Documents\Device\804_Dispo\D307F5L0 (Argos in DPR and DNU)\D307F5L0 Argos skew w bsl_splitlot_w_median.jmp"
# incsv=r"C:\Users\narju\OneDrive - Intel Corporation\Documents\Device\804_Dispo\D315FCW0 EVA recess FIN ISO\D315FCW0 EVA recess FIN ISO_splitlot.csv"
incsv=r"C:\Users\narju\OneDrive - Intel Corporation\Documents\Device\804_Dispo\D340FCA0_ZTO_Hot_As_FIN_ISO\perf_wfr2.csv"
d=pd.read_csv(incsv)


#%%
prod=d.columns[d.columns.str.contains('PRODUCT', regex=True)].to_list()
assert len(prod)==1
prod=prod[0]
device_names=['[NP]M.+[AEZ]3.+\-L(?!.*/)(?!.*CMOS)', '[NP]S.+[AEZ]3.+1X1.+\-L(?!.*/)(?!.*CMOS)']
test_names=['BETA4_0.05D', 'CDEL_1.1G', 'COV_0.5G', 'IDEFF4_1.1\W\d+', 'DBL4_0.65D', \
       'SS4_0.05D', 'VTGM4_0.05D', 'R[_-]1E\W0.65', 'REXTB4_0.05D', 'LOG_LD_0.65D\W0G']
wid='WAFER'
lot7='LOT7'

#%%


#%% 
#Insert I/CV if not present

def calc_change(val_array, bsl_wfr):
    # return val_array-val_array[bsl_wfr]
    # return val_array.name
    if re.search('(DBL)|(SS.*_)', val_array.name): return (val_array-val_array[bsl_wfr])
    elif re.search('VTGM', val_array.name): return  ((val_array-val_array[bsl_wfr])*1e3)
    else: return (val_array/val_array[bsl_wfr]-1)
    
def mos_flavor(alias):
    if re.search('([\(\[]?N[M|S])|(NMOS)', alias): return 'NMOS'
    elif re.search('([\(\[]?P[M|S])|(PMOS)', alias): return 'PMOS'
    else: return 'NONE'
    
def test_name(alias):
    if 'I/C' in alias and 'MOS' in alias: return '.I/C'
    elif 'CDEL+4COV' in alias: return 'CDEL+4COV'
    elif re.search('[\(\[]?\D+.*[\(\[]', alias): 
        return re.search('[\(\[]?\D+.*[\(\[]', alias).group().strip('()[]')
    else: return alias
    
def df_regex_multi_filter(d, regex_list, axis=1):
    ds=pd.DataFrame()
    for r in regex_list:
        di=d.filter(regex=r, axis=axis)
        if not ds.empty:
            ds=pd.merge(ds, di, left_index=True, right_index=True)
        else:
            ds=di
    return ds
    
def regex_from_list(rlist):
    # rlist=['CDEL_1.1G', 'BETA4_0.05D']
    r=['('+ri+')' for ri in rlist]
    r='|'.join(r)
    return r

def main():
    pass

if True:
    group=d.columns[d.columns.str.contains('_grp$', regex=True)].to_list()
    
    if len(group)>1:
        for i in zip(range(len(group)), group ):
            print(i)

        selection=input("select grouping: ")
        if selection.rstrip().isdigit(): 
            print(group[int(selection.rstrip())], " selected")
            group=group[int(selection.rstrip())]
    elif len(group)==1: group=group[0]
    # else: group = '_grp'

    # groupby _grp and statistic of key params
    # summary_columns_reg='(M45.+[AEZ]3\d.+\-L(?!.*/)(?!.*CMOS))|(S45.+[AEZ]3\d.+1X1.+\-L(?!.*/)(?!.*CMOS))|_grp$|LOT7|WAFER'
    
    
    lw = ['_grp$'] #lot7,wid
    rx1 = regex_from_list(device_names+lw)
    groupstat=d.filter(regex=rx1, axis=1).groupby(group).median()

    rx=regex_from_list(test_names)
    groupstat = groupstat.filter(regex=rx, axis=1).filter(regex='(Median)|(\@50\%)', axis=1)
    
    if groupstat.filter(regex='I/C', axis=1).empty:
        nmos_i=groupstat.filter(regex='IDEFF.*\[N', axis=1).values
        nmos_cdel=groupstat.filter(regex='CDEL.*\[N', axis=1).values
        nmos_cov=groupstat.filter(regex='COV.*\[N', axis=1).values
        groupstat['CDEL+4COV NMOS']=nmos_cdel+4*nmos_cov
        groupstat['I/C NMOS']=nmos_i/(nmos_cdel+4*nmos_cov)
        
        pmos_i=groupstat.filter(regex='IDEFF.*\[P', axis=1).values
        pmos_cdel=groupstat.filter(regex='CDEL.*\[P', axis=1).values
        pmos_cov=groupstat.filter(regex='COV.*\[P', axis=1).values
        groupstat['CDEL+4COV PMOS']=pmos_cdel+4*pmos_cov
        groupstat['I/C PMOS']=pmos_i/(pmos_cdel+4*pmos_cov)

    #%%
    # ask which one is the baseline

    bsl_wfr_candidates=groupstat.index[groupstat.index.str.contains('BSL|POR|MEDIAN|\d{3}', regex=True)].to_list()

    for i in zip(range(len(bsl_wfr_candidates)), bsl_wfr_candidates ):
        print(i)

    selection=input("select baseline: ")
    if selection.rstrip().isdigit(): 
        print(bsl_wfr_candidates[int(selection.rstrip())], " selected")
        bsl_wfr=bsl_wfr_candidates[int(selection.rstrip())]

    # stats[stats.columns].sub(stats[bsl_wfr], axis=0)

    #%% 

    stats=groupstat.transpose()
    groups=stats.columns.to_list()

    delta=stats.apply(lambda x: calc_change(x, bsl_wfr), axis=1)
    delta['MOS'] = pd.Series(delta.index).apply(lambda x: mos_flavor(x)).values
    delta['TEST'] = pd.Series(delta.index).apply(lambda x: test_name(x)).values 

    cols_in_order=['MOS','TEST',]+groups
    delta=delta[cols_in_order]
    delta=delta.sort_values(by=['MOS','TEST'])

    # output into a table
    directory=os.path.dirname(incsv)
    delta.to_csv(os.path.join(directory, '_stats2.csv'), index=True)
    groupstat.to_csv(os.path.join(directory, '_groupstat2.csv'), index=True)
    # return delta



# if __name__=="__main__":
#     delta=main()
    