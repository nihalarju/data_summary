#import csv, sys
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import glob, os
#from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
#from sklearn.metrics import accuracy_score, precision_score, recall_score
#from sklearn.model_selection import cross_val_score
#import warnings
import PyUber
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

camp = '//SHUser-Prod.intel.com/SHProdUser$/narju/PY/WOPR_PULL_MODS/'
#camp = 'C:/Users/narju/OneDrive - Intel Corporation/Documents/Training/SQL/WOPR_PULL_MODS/'

email_lists = {\
    'MY': ['nihal.arju@intel.com'],\
    'LAT': ['nihal.arju@intel.com', 'Kathleen.Larson@intel.com', 'lat_pat_shift_gl_d1@intel.com',\
            'nicole.k.poe@intel.com', 'brad.s.hamlin@intel.com', 'lat_ply_d1d@intel.com'], \
    'REX': ['nihal.arju@intel.com', 'zachary.pottorf@intel.com', 'tyler.peterson@intel.com', 'jeremy.t.roy@intel.com', \
            'david.a.martinez@intel.com', 'Pawel.Mostowski@intel.com', 'clayton.d.chincio@intel.com'],\
    'TEL': ['nihal.arju@intel.com', 'richard.j.freach@intel.com', 'nadeesha.jayasekara@intel.com', ],
    'DEA': ['nihal.arju@intel.com', 'hunter.l.neilson@intel.com', 'kurt.t.schneider@intel.com', 'travis.malik@intel.com', 'james.d.gallagher@intel.com'],\
	}

module_prefixes = {'MY': ['LAT', 'PAT'], 'LAT': ["LAT", "PAT", 'SLS'], 'REX' : ['REX'], \
 'TVO' : ['TVO', 'GTX'], 'TEL' : ['ONT', 'ANT', 'GTT'], 'DEA': ['DEA'], }

modules = ['LAT', 'REX', 'DEA', 'TEL']
#modules = ['MY']

"""Corral snapshot csv files into one clean file and then train model"""
def main():
    print("Starting: ", datetime.now())
    print(os.getcwd())
    wopr_training = pd.DataFrame()
    csvs = glob.glob(camp + 'training_data2/*.csv')
    for fnode in csvs:
        wopr = read_pull(fnode)
        wopr = pivot_features(wopr)
        #if 'last_event_date' in wopr.columns: wopr.drop(['last_event_date'], axis=1, inplace=True)
        if 'Column1' in wopr.columns: wopr.drop(['Column1'], axis=1, inplace=True)
        if wopr_training.empty: # load first file
            wopr_training = wopr
            training_columns = wopr.columns
        else: # load other files
            wopr_training = wopr_training.append(wopr, ignore_index=True)

    """write corraled data to a file"""
    training_file = camp + 'old_files/wopr-training.csv'
    #wopr_training.to_csv(training_file, index = False)

    """Fit a ML model"""
    """Step 2: clean up to leave only continuous or categorical data"""
    wopr_features = get_features(wopr_training)
    labels = wopr_features['needs_attention']
    #wopr_features.to_csv(camp + 'old_files/features.csv', index = False)
    wopr_features.drop('needs_attention', axis=1, inplace=True)
    rf3 = RandomForestClassifier(n_estimators=10, max_depth=10)
    rf3.fit(wopr_features, labels.values.ravel())

    """Step 3: Read the latest file and make prediction"""
    for module in modules:
        try:
            print(module, ": Pulling SQL: ", datetime.now())
            sql = sql1.format('\nOR '.join(['e.entity LIKE \'{}%\''.format(too) for too in module_prefixes[module]]))
            tools = SQL_DataFrame(sql).drop_duplicates()
            sql = sql2.format('\nOR '.join(['a1.toolname LIKE \'{}%\''.format(too) for too in module_prefixes[module]]))
            woprs = SQL_DataFrame(sql).drop_duplicates()
            sql = sql3.format('\nOR '.join(['e.entity LIKE \'{}%\''.format(too) for too in module_prefixes[module]]))
            n_opers = SQL_DataFrame(sql).drop_duplicates()
            print(module, ": Got data. Final stretch: ", datetime.now())
            #woprs.to_csv(camp + 'output/' + module + '/woprs.csv', index = False) #### DELETE
            n_opers = n_opers[['ENTITY', 'OPERATION']].groupby('ENTITY').count().reset_index()
            woprs['GP_COMPLETED_TASKS'].fillna(0, inplace = True)
            idx = woprs.groupby('WORKORDER_ID')['LOG_CREATED_ON'].transform(max) == woprs['LOG_CREATED_ON']
            woprs = woprs[idx]
            idx = woprs.groupby('WORKORDER_ID')['GP_COMPLETED_TASKS'].transform(max) == woprs['GP_COMPLETED_TASKS']
            woprs = woprs[idx]
            woprs.drop_duplicates(inplace=True)
            merged = pd.merge(woprs, tools, left_on=['TOOL_NAME', 'STATE', 'AVAILABILITY'],
                              right_on=['ENTITY', 'STATE', 'AVAILABILITY'], how='outer')
            merged.drop(['TOOL_NAME', 'LAST_EVENT', 'ATTRIBUTE_VALUE', 'CEID'], axis = 1, inplace = True)
            merged = merged.rename(columns={'ENTITY': 'TOOL_NAME'})
            merged.columns = merged.columns.str.lower()
            #Only keep lines for woprs or tools that are down
            merged = merged[((merged['availability'] == 'Down') | (merged['workorder_id'].notnull()))]
            merged = convert_to_date(merged)
            merged['wopr_idle'] = datetime.now() - merged[['last_updated_date', 'log_created_on', 'last_event_date']].max(
                axis=1)
            merged['wopr_age'] = datetime.now() - merged['created_date']
            # Fill in data for down tools without wopr
            idx = merged['workorder_id'].isnull()
            merged['priority_id'].loc[idx] = (np.zeros(len(merged)) + 6)[idx]
            merged['wopr_age'].loc[idx] = np.zeros(len(merged))[idx]
            merged['gp_completed_tasks'].loc[idx] = np.zeros(len(merged))[idx]
            merged['wopr_idle'].fillna(timedelta(days=365), inplace=True)
            #merged.to_csv(camp + 'output/' + module + '/merged.csv', index = False)

            current_list = merged[merged['tool_name'].notnull()]
            current_list['needs_attention'] = 0
            current_list['n_opers_10x'] = current_list['tool_name'].apply(lambda x: num_opers_10x(n_opers, x))
            #current_list['n_opers_10x'].fillna(0., inplace=True)
            current_list = pivot_features(current_list)

            current_features = get_features(current_list)
            current_features = current_features.drop('needs_attention', axis=1)[wopr_features.columns]
            #current_features.to_csv(camp + 'old_files/' + module + 'current_features.csv', index=False)
            y_pred = rf3.predict(current_features)
            current_list['needs_attention'] = y_pred
            current_list['availability_Down'] = current_features['availability_Down']

            current_list.sort_values(by=['needs_attention', 'is_prod_tool', 'availability_Down', 'wopr_idle'], inplace=True,\
                                     ascending=False)
            print("Analysis complete. Printing: ", datetime.now())
            print(current_list[['tool_name', 'state', 'workorder_id', 'wopr_idle', 'needs_attention']].head(20))
            current_list = convert_hours_to_timedelta(current_list)[wopr_training.columns]

            current_fname = camp + 'output/' + module + '/wopr-' + str(datetime.now())[:13] + '.csv'
            attend_html_fname = camp + 'output/' + module + '/wopr.htm'
            current_list[training_columns].to_csv(current_fname,\
                                index=False)  # used to be camp + 'wopr-processed.csv'

            needs_attention = current_list[current_list['needs_attention'] == 1]
            needs_attention = convert_timedelta_to_hours(needs_attention)
            needs_attention.sort_values(by = 'tool_name', inplace = True)

            pd.set_option('display.max_colwidth', -1)
            needs_attention['workorder'] = needs_attention['workorder_id'].apply(lambda x: \
                '<a href = \"https://rf3-apps-fuzion.rf3prod.mfg.intel.com/EditWorkOrderPage.aspx?WorkOrderId=' + str(int(x)) + '\">' +\
                str(int(x)) + '</a>' if pd.notna(x) else '')
            needs_attention['status'] = needs_attention['status'].apply(lambda x: \
                str(x) if pd.notna(x) else '')
            needs_attention['workorder_desc'] = needs_attention['workorder_desc'].apply(lambda x: \
                str(x) if pd.notna(x) else '')
            needs_attention = needs_attention[['tool_name', 'state', 'wopr_idle', 'workorder', 'status', 'workorder_desc']]
            needs_attention.to_html(attend_html_fname, index = False, render_links = True, escape = False)
    
            # Create message container - the correct MIME type is multipart/alternative.msg = MIMEMultipart('alternative')
            msg = MIMEMultipart('alternative')
            msg['Subject'] = module + " tools that need attention"
            msg['From'] = 'nihal.arju@intel.com'
            msg['To'] = ', '.join(email_lists[module])
    
            # Create the body of the message (a plain-text and an HTML version).
            text = "The email is not displaying properly."
            with open(attend_html_fname) as fp:
                html = fp.read()
    
            # Record the MIME types of both parts - text/plain and text/html.
            part1 = MIMEText(text, 'plain')
            part2 = MIMEText(html, 'html')
    
            # Attach parts into message container.
            # According to RFC 2046, the last part of a multipart message, in this case
            # the HTML message, is best and preferred.
            msg.attach(part1)
            msg.attach(part2)
    
            # Send the message via local SMTP server.
            s = smtplib.SMTP('mail.intel.com')
            # sendmail function takes 3 arguments: sender's address, recipient's address
            # and message to send - here it is sent as one string.
            s.sendmail('nihal.arju@intel.com',\
              email_lists[module], msg.as_string())
            # s.sendmail('nihal.arju@intel.com','nihal.arju@intel.com', msg.as_string())
            s.quit()
        except:
            print(module + " failed to run.")
###############################################################################
def convert_timedelta_to_hours(df, column1 = 'wopr_idle', column2 = "wopr_age"):
    if column1 in df.columns:
        df[column1] = pd.to_timedelta(df[column1] )/pd.to_timedelta(1, unit='h')
    if column2 in df.columns:
        df[column2] = pd.to_timedelta(df[column2])/pd.to_timedelta(1, unit='h')
    return df

def convert_hours_to_timedelta(df, column1 = 'wopr_idle', column2 = "wopr_age"):
    if column1 in df.columns:
        df[column1] = df[column1]*pd.to_timedelta(1, unit='h')
    if column2 in df.columns:
        df[column2] = df[column2]*pd.to_timedelta(1, unit='h')
    return df

def convert_to_date(df, column1 = 'last_updated_date', column2 = 'created_date', column3 = 'log_created_on', column4 = 'last_event_date'):
    if column1 in df.columns:
        df[column1] = pd.to_datetime(df[column1])
    if column2 in df.columns:
        df[column2] = pd.to_datetime(df[column2])
    if column3 in df.columns:
        df[column3] = pd.to_datetime(df[column3])
    if column4 in df.columns:
        df[column4] = pd.to_datetime(df[column4])
    return df

def add_wopr_count_4tool(wopr):
    # add a column with number of live woprs for tool
    if 'n_wopr_4tool' in wopr.columns: print('n_wopr already in table')
    wopr['n_wopr_4tool'] = wopr['workorder_id'].notnull()*1
    wopr_count = wopr[['tool_name', 'n_wopr_4tool']].groupby('tool_name').sum().reset_index()
    wopr.drop(['n_wopr_4tool'], axis =1 , inplace=True)
    wopr = pd.merge(wopr, wopr_count, left_on='tool_name', right_on='tool_name', how = 'outer')
    return wopr

def get_features(wopr_list):
    wopr_list = wopr_list[wopr_list['tool_name'] != 'LAT_RFC']
    wopr_list['needs_attention'].fillna(0, inplace = True)
    wopr_list = pd.get_dummies(wopr_list, columns = ['status', 'availability'])
    if 'status_Open' not in wopr_list.columns:
        wopr_list['status_Open'] = 0

    if 'last_event_date' in wopr_list.columns: wopr_list.drop(['last_event_date'], axis=1, inplace=True)
    wopr_list['in_qual'] = wopr_list['state'].apply(lambda x: \
      x in ['SchedQual', 'UnschQual', 'UnschFacilities'])*1
    wopr_list['in_waiting'] = wopr_list['state'].apply(lambda x: \
      x in ['WaitingMetrology', 'WaitingIntel', 'EquipUpgrade','UnschWaitPart', 'UnschWaitSupplier', 'WaitingResources', 'WaitingTechnician', 'SupplierRepair'])*1
    wopr_list['in_Eng'] = (wopr_list['state'] == 'Engineering')*1
    wopr_list['in_OOC'] = (wopr_list['state'] == 'OutOfControl')*1
    wopr_list['in_PMOD'] = (wopr_list['state'] == 'PMOverdue')*1
    wopr_list['in_Repair'] = (wopr_list['state'] == 'InRepair')*1
    wopr_features = wopr_list.drop(['latest_pm_name',
                    'tool_name',
                    'state',
                    'last_updated_by_user',
                    'last_updated_date',
                    'created_date',
                    'workorder_desc',
                    'workorder_id',
                    'availability_Up',
                    'gp_completed_tasks',
                   'log_created_on',],
                    axis = 1)
    return wopr_features

def read_pull(fname):
    wopr_list = pd.read_csv(fname)
    if 'Column1' in wopr_list.columns: wopr_list.drop(['Column1'], axis=1, inplace=True)
    wopr_list = wopr_list[wopr_list['tool_name'] != 'LAT_RFC']
    return wopr_list

def pivot_features(wopr_list):
    wopr_list = convert_to_date(wopr_list)
    wopr_list = convert_timedelta_to_hours(wopr_list)
    wopr_list = add_wopr_count_4tool(wopr_list)
    wopr_list['is_prod_tool'] = (pd.notnull(wopr_list['latest_pm_name']) & (
                wopr_list['wopr_idle'] < 24.*2)) * 1
    wopr_list['is_mom'] = wopr_list['tool_name'].apply(lambda x: (len(x) <= 6) * 1)
    moms_w_wopr = set(wopr_list[ (wopr_list['is_mom'] == 1 ) & (pd.notnull(wopr_list['workorder_id']))]['tool_name'])
    wopr_list['mom_has_wopr'] = wopr_list['tool_name'].apply(lambda x: x.split('_')[0] in moms_w_wopr) * 1
    mom_down_list = set(wopr_list[((wopr_list['is_mom'] == 1) & (wopr_list['availability'] == 'Down'))]['tool_name'])
    wopr_list['mom_down'] = wopr_list['tool_name'].apply(lambda x: x.split('_')[0] in mom_down_list) * 1
    return wopr_list

def SQL_DataFrame(sql, source = 'D1D_PROD_XEUS'):
    conn = PyUber.connect(source)
    df = pd.read_sql(sql, conn)
    return df

def num_opers_10x(n_opers, tool = 'LAT01_PM1'):
    try:
        n_opers_d = n_opers[n_opers['ENTITY'] == tool]['OPERATION'].values[0]/10.
        return n_opers_d
    except:
        return 0.

sql1 = """
SELECT 
          e.entity AS entity
         ,e.state AS state
         ,e.last_event AS last_event
         ,To_Char(e.last_event_date,'yyyy-mm-dd hh24:mi:ss') AS last_event_date
         ,ea.attribute_value AS attribute_value
         ,e.ceid AS ceid
         ,e.latest_pm_name AS latest_pm_name
         ,e.availability AS availability
FROM 
F_ENTITY e
LEFT JOIN F_ENTITYATTRIBUTE ea ON ea.entity = e.entity AND ea.history_deleted_flag='N'
WHERE
              (
{}
) 
 AND      ea.attribute_name = 'ModuleAllowed' 
ORDER BY
           1 Asc"""

sql2 = """
SELECT 
          a1.toolname AS tool_name
         ,a1.statusoptionname AS status
         ,f0.availability AS availability
         ,f0.state AS state
         ,a1.lastupdatedusername AS last_updated_by_user
         ,To_Char(a1.lastupdatedon,'yyyy-mm-dd hh24:mi:ss') AS last_updated_date
         ,To_Char(a1.createdon,'yyyy-mm-dd hh24:mi:ss') AS created_date
         ,Replace(Replace(Replace(Replace(Replace(Replace(a1.description,',',';'),chr(9),' '),chr(10),' '),chr(13),' '),chr(34),''''),chr(7),' ') AS workorder_desc
         ,a1.workorderid AS workorder_id
         ,a1.priorityid AS priority_id
         ,a5.completedgameplan AS gp_completed_tasks
         ,To_Char(a4.createdon,'yyyy-mm-dd hh24:mi:ss') AS log_created_on
FROM 
F_FUZION_WORKORDERS a1
LEFT JOIN F_ENTITY f0 ON f0.entity=a1.toolname
LEFT JOIN F_FUZION_WORKORDERLOGS       a4 ON a1.workorderid=a4.workorderid
LEFT JOIN F_FUZION_GAMEPLANDETAILS     a5 ON a1.workorderid=a5.workorderid
WHERE
              (
{}
) 
 AND      a1.createdon >= TRUNC(SYSDATE) - 14 
 AND      a1.statusoptionname In ('Accepted'
,'Data Due', 'Open') """

sql3 = """
SELECT  DISTINCT 
          e.entity AS entity
         ,o.operation_type AS operation_type
         ,eo.operation AS operation
FROM 
F_ENTITY e
LEFT JOIN F_ENTITYOPER eo ON eo.facility = e.facility AND eo.entity = e.entity
LEFT JOIN F_OPERATION o ON o.facility = eo.facility AND o.operation = eo.operation AND o.latest_version='Y'
WHERE
              (
{}
) 
 AND      o.operation_type = 'STD' 
 AND      o.state = 'Active' 
 AND      eo.dissociate_date >= SYSDATE 
ORDER BY
           1 Asc
          ,3 Asc
"""

if __name__ == "__main__": main()
