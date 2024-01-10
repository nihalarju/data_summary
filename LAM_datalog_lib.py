
import numpy as np
import pandas as pd


pressure = 'ProcessManometerAdjustedPressure'
power = 'TCPRFGenForwardPower_AI'
rpower = 'TCPRFGenReflectedPower_AI'
biaspower = 'BiasRFGenForwardPower_AI'
biasrpower = 'BiasRFGenReflectedPower_AI'
pvpos = 'ThrottleValvePosition_AI'
biasseries = 'BiasMatchSeriesCapPosition_AI'
biasshunt = 'BiasMatchShuntCapPosition_AI'
c1pos = 'TCPMatchC1CapPosition_AI'
c3pos = 'TCPMatchC3CapPosition_AI'
c4pos = 'TCPMatchC4CapPosition_AI'
c5pos = 'TCPMatchC5CapPosition_AI'
miduty = 'MidInnerESCTemperatureDutyCycle_AI'
mit = 'MidInnerESCTemperature_AI'
gas1 = 'Gas_1_Flow_AI'
gas2 = 'Gas_2_Flow_AI'
gas3 = 'Gas_3_Flow_AI'
gas4 = 'Gas_4_Flow_AI'
gas5 = 'Gas_5_Flow_AI'
gas6 = 'Gas_6_Flow_AI'
gas7 = 'Gas_7_Flow_AI'
gas8 = 'Gas_8_Flow_AI'
gas9 = 'Gas_9_Flow_AI'
gas10 = 'Gas_10_Flow_AI'
gas11 = 'Gas_11_Flow_AI'
gas12 = 'Gas_12_Flow_AI'
gas13 = 'Gas_13_Flow_AI'
gas14 = 'Gas_14_Flow_AI'


def float_floatable(a):
    try:
        try:
            return float(a)
        except:
            return a.strip()
    except: return a

def pick_valuables(x):
    #the datalog array has '---' for empty rows
    return np.array(x[lambda x: (x != '---')].values, dtype = 'float64')

def find_max_t(df):
    a = np.arange(0,len(df.columns),2)
    a = list(map(str, a))
    #print('length', len(df.columns))
    #print('max a', a[-1:])
    return df[a].max().max()

def standerdize(x2, x, y):
    x, y = pick_valuables(x), pick_valuables(y)
    return np.interp(x2, x, y)

def conv_to_string(bstr):
    return bstr.decode('utf-8').strip()

def read_ziptext(fname):
    import zipfile

    with zipfile.ZipFile(fname) as z:
        with z.open(fname.split('.zip')[0]) as f:
            return list(map(conv_to_string, f.readlines()))


def to_ms(a):
    np.timedelta64(a, 'ms')
    
def clean_fname(fname):
    if '.zip' in fname:
        return fname.split('.zip')[0]
    else: 
        return fname

def find_line(text, string, forward = True, stop = None):
    m = np.nan
    #string ='Version = Rev 1.8.4'
    for i, line in enumerate(text):
        if string in line:
            if forward: return i
            else: m = i
        # stop or reverse search
        if stop is not None: #if 'stop' is defined 
            if np.isnan(m) == False:
                if stop in line: break # search only till 'stop' is found
    return m
    
#if 1==1:
def load_datalog(fname):
    fname = clean_fname(fname)
    slotno = str(fname.split('-')[1].split('.')[0])
    text = read_ziptext(fname + '.zip')
    n = find_line(text, 'HistoricalData:')
    #print(n)
    cols = text[n+1].split('\t')
    textl = [line.split('\t') for line in text[n+3:]]
    df = pd.DataFrame.from_records(textl, coerce_float=True)
    df.columns = map(str, np.arange(len(df.columns)))
    if len(df.columns) % 2 != 0: # there should be even number of columns
        try: #if there are odd number of columns
            df.drop(str(len(df.columns)-1), 1, inplace = True) #delete the odd column in the end
        except: pass
    df = df.applymap(float_floatable)

    xmax = float(find_max_t(df))
    x2 = np.arange(0, xmax, xmax/500, dtype='float64')
    df_std = pd.DataFrame(index=pd.TimedeltaIndex(x2, unit = 'ms'))
    
    for i, item in enumerate(cols):
        try:
            df_std[cols[i]] = standerdize(x2, df[str(2*i)], df[str(2*i+1)])
        except:
            df_std[cols[i]] = np.full(len(x2), np.nan)
    
    n1 = find_line(text, 'SET_TIME')
    n2 = find_line(text, 'EPD_TIME', forward = False, stop = 'Process Fine Tune:')
    #print('n2' + str(n1))
    times = pd.read_table(fname + '.zip', skiprows = n1-1, nrows = n2-n1, names = ['qty', 'unit', 'value', 'recipestep'])
    step_times = times[times['qty'] == 'PROC_TIME']
    step_end_times = np.cumsum(np.array(step_times['value'].values, dtype = 'float'))#.astype('timedelta64[s]')
    step_numbers = np.array(step_times['recipestep'].values, dtype = 'float')
    df_std['recipestep'] = np.ceil(np.interp(x2, step_end_times*1e3, step_numbers ))
    df_std['entity'] = text[find_line(text, 'PM ID: ')].split(':')[1].strip()
    df_std['date'] = fname.split('.')[1][:4]
    #df_std['slot'] = float(slotno)
    #df_std['DATE'] = date
    #n1 = find_line(text, 'End: (')
    #date = text[find_line(text, 'xxx ')].split(':')[1].strip()
    return df_std

def select_step(df):
    df_step = df[df['RecipeStep'] == recipe_step]
    df_step.index = df_step.index - df_step.index.values[0]
    return df_step

def make_keys(labels, slots):
    colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k', 'w']
    if type(labels) != list: labels = [labels]
    if type(slots) != list: slots = [slots]
    assert all(isinstance(x, str) for x in labels) == True
    assert all(isinstance(x, (int, float)) for x in slots) == True
    i = -1
    for label in labels:
        for slot in slots:
            i +=1
            yield (colors[i], label + str(slot))

def pv_dispo(src = '.'):
    os.chdir(scr)
    recipe_steps = [2, 5, 7]
    #recipe_step_max = recipe_step_min + 0
    try:    del ax
    except:    pass
    legends = []
    colors = ['g', 'b']
    
    for i, key in enumerate(keys): #make_keys(['fail','old'],[25, 24]):  
        c = 0 if 'p2' in key else 1
        print(key)
        #del df 
        #del df_step
        df = dfs[key]
        df_step = df[ (df['RecipeStep'] >= recipe_step_min) & (df['RecipeStep'] <= recipe_step_max) ]
        df_step.index = df_step.index - df_step.index.values[0]
        try: df_step[chpr].plot(ax = ax, style = colors[c])
        except: ax = df_step[ch1].plot(style = colors[c])
        df_step[ch2].plot(ax = ax, secondary_y = True, style = colors[c] + '--')

        legends.append(key)