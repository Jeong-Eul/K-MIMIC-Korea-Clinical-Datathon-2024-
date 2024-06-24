import pandas as pd
import numpy as np
import os
from tqdm import tqdm
import warnings
pd.set_option('mode.chained_assignment',  None) 
warnings.simplefilter(action='ignore', category=FutureWarning)

def tabularization(selected, day, hospital_id):
    valid_stay_ids = selected.STAY_ID.unique()
    part_csv=pd.DataFrame()   
    
    print(f'Start Tabularize Day:{day}/Hosp:{hospital_id}-')
    print('Number of patient: ', len(selected.SUBJECT_ID.unique()))
    
    for hid in tqdm(valid_stay_ids, desc = f'Tabularize EHR part-Day:{day}/Hosp:{hospital_id}'):
        gc.collect()

        practice = selected[selected['STAY_ID']==hid]
        practice.loc[:, 'VALUENUM'] = pd.to_numeric(practice['VALUENUM'], errors='coerce')
        val=practice.pivot_table(index='start_time',columns='ITEMID',values='VALUENUM').reset_index(drop=True)
        val['STAY_ID'] = hid
        val['SUBJECT_ID'] = practice.SUBJECT_ID.unique()[0]
        val['Time_since_ICU_admission'] = val.index
        
        if not os.path.exists(local+f"/tabular_records/csv_tab/part-{day}/{hospital_id}/"):
            os.makedirs(local+f"/tabular_records/csv_tab/part-{day}/{hospital_id}/")
        
        if(part_csv.empty):
            part_csv=val
        else:
            part_csv=pd.concat([part_csv,val],axis=0)
        
        #[ ====== Save temporal data to csv ====== ]
        dir = f'/tabular_records/csv_tab/part-{day}/{hospital_id}/tab.csv'
        part_csv.to_csv(local+dir,index=False)
        
    print("[ SUCCESSFULLY SAVED TOTAL UNIT STAY DATA ]")


hosp_id = ['433', '434', '435']
days = ['002', '001', '006', '008', '010']
for d in days:
    if d == '006':
        new_hosp_id = ['433', '434']
        for h in new_hosp_id:
            try:
                target = pd.read_csv(f'./tmp/PreprocSet/Checkpoint/{d}/{h}/ckpt.csv')
                target.drop('Unnamed: 0', axis = 1, inplace =True)
                target['ITEMID'] = target['ITEMID'].map(variable_name_dict)
                tabularization(target[target['STAY_ID'].isin(target.STAY_ID.unique())], d, h)
            except:
                print('해당사항 없음')
    else:
        for h in hosp_id:
            try:
                target = pd.read_csv(f'./tmp/PreprocSet/Checkpoint/{d}/{h}/ckpt.csv')
                target.drop('Unnamed: 0', axis = 1, inplace =True)
                target['ITEMID'] = target['ITEMID'].map(variable_name_dict)
                tabularization(target[target['STAY_ID'].isin(target.STAY_ID.unique())], d, h)
            except:
                print('해당사항 없음')

raw_data_433 = pd.read_csv('tmp/PreprocSet/tabular_records/csv_tab/part-001/433/tab.csv')
raw_data_433['Amphotericin B'] = 0

raw_data_434 = pd.read_csv('tmp/PreprocSet/tabular_records/csv_tab/part-001/434/tab.csv')
raw_data_435 = pd.read_csv('tmp/PreprocSet/tabular_records/csv_tab/part-001/435/tab.csv')
raw_data_435['Amphotericin B'] = 0
raw_data_435['aminophyline(Methylxanthines)'] = 0

a = concat_patient(raw_data_433, raw_data_434)
data = concat_patient(a, raw_data_435)

def compute_outlier_imputation(arr, cut_off,left_thresh,impute):
    perc_up = np.percentile(arr, left_thresh)
    perc_down = np.percentile(arr, cut_off)
    #print(perc_up,perc_down)
    if impute:
        arr[arr < perc_up] = perc_up
        arr[arr > perc_down] = perc_down
    else:
        #print(arr[arr < perc_up].shape,arr[arr > perc_down].shape)
        arr[arr < perc_up] = np.nan
        arr[arr > perc_down] = np.nan
    return arr


def outlier_imputation(data, cut_off,left_thresh,impute):
    df = data.copy()
    grouped = ['SBP', 'HR', 'SpO2']
    #print(cut_off)
    for col in grouped:
        values = df[col].values
        values = compute_outlier_imputation(values, cut_off,left_thresh,impute)
        df[col] = values
    #print(data.shape)
    return df

data_filtering = outlier_imputation(data, 0.98, 0.02, False)

print('환자 수: ', data.SUBJECT_ID.nunique())
print('입원 수: ', data.STAY_ID.nunique())


def imputation(data):
    df = data.copy()

    result = pd.DataFrame()
    for stay in tqdm(df.STAY_ID.unique()):
        current_stay = df[df['STAY_ID']==stay]
        
        forward_set = current_stay[['SUBJECT_ID', 'STAY_ID','Time_since_ICU_admission', 'SBP', 'HR', 'SpO2']]
        forward_set = forward_set.ffill().reset_index(drop=True)

        lab_result = pd.DataFrame()
    
        for lab_object in ['Serum_Creatinine']:
        
            lab_measure = lab_object
    
            current_lab = psql.sqldf(f"SELECT {lab_measure} FROM current_stay")
            current_lab[f'{lab_measure}_up'] = 'N'
    
            if len(current_lab.dropna())>=2:
                
                for idx, measure in enumerate(current_lab.dropna()[f'{lab_measure}'].values):
                    current_idx = current_lab.dropna().index[idx]
                    
                    if idx == 0: #initialize, not calcalurate first for first lab value
                        current_lab.loc[current_idx, f'{lab_measure}_up'] = 0
                        initial_lactate = current_lab.loc[current_idx, f'{lab_measure}']
                    else:
                        if current_lab.loc[current_idx, f'{lab_measure}'] > initial_lactate:
                            current_lab.loc[current_idx, f'{lab_measure}_up'] = 1
                        else:
                            current_lab.loc[current_idx, f'{lab_measure}_up'] = 0
                        initial_lactate = current_lab.loc[current_idx, f'{lab_measure}']

                # lab up/down forward fill
                current_lab[f'{lab_measure}_up'] = current_lab[f'{lab_measure}_up'].replace({'N':np.nan})
                current_lab[f'{lab_measure}_up'] = current_lab[f'{lab_measure}_up'].ffill()
                
                # missing lab measurement default value
                current_lab[f'{lab_measure}_presence'] = current_lab[f'{lab_measure}'].values
                current_lab[f'{lab_measure}'] = current_lab[f'{lab_measure}'].ffill()
                current_lab[f'{lab_measure}_presence'] = current_lab[f'{lab_measure}_presence'].fillna(0)
                current_lab[f'{lab_measure}_presence'] = (current_lab[f'{lab_measure}_presence'] > 0).astype(int)
                
                # lab up/down before first measure
                current_lab[f'{lab_measure}_up'] = current_lab[f'{lab_measure}_up'].fillna(0)
                
            else:
                current_lab[f'{lab_measure}'] = current_lab[f'{lab_measure}'].fillna(0)
                current_lab[f'{lab_measure}_up'] = 0
                current_lab[f'{lab_measure}_presence'] = 0
            columns_to_concat = [f'{lab_measure}', f'{lab_measure}_up', f'{lab_measure}_presence']
            lab_result = pd.concat([lab_result, current_lab[columns_to_concat]], axis=1).reset_index(drop=True)

        required_columns = ['Amikacin', 'Vancomycin', 'piperacillin/tazobactam', 'aminophyline(Methylxanthines)', 'Amphotericin B', 'Urine_Output', 'RRT_Urine_Output']
        phar_set = current_stay[required_columns].fillna(0).reset_index(drop=True)

        merged_lab_chart = pd.concat([forward_set, lab_result], axis = 1)
        merged_total = pd.concat([merged_lab_chart, phar_set], axis = 1)
        
        result = pd.concat([result, merged_total], axis = 0).reset_index(drop=True)
        
    
    return result

dataset = imputation(data)


result = dataset.copy()
result['Urine_Output'] = result['Urine_Output'] + result['RRT_Urine_Output']
result = result.rename(columns = {'RRT_Urine_Output':'RRT'})
result['RRT'] = (result['RRT'] > 0).astype(int) #RRT 여부


# Vital sign에 대해서 RSI
delta = result['HR'].diff()
gain = (delta.where(delta > 0, 0)).rolling (window=3).mean()
loss = (-delta.where(delta < 0, 0)).rolling (window=3).mean()
rs = gain / loss
result['HR_RSI'] = 100 - (100 / (1 + rs))

delta = result['SBP'].diff()
gain = (delta.where(delta > 0, 0)).rolling (window=3).mean()
loss = (-delta.where(delta < 0, 0)).rolling (window=3).mean()
rs = gain / loss
result['SBP_RSI'] = 100 - (100 / (1 + rs))

delta = result['SpO2'].diff()
gain = (delta.where(delta > 0, 0)).rolling (window=3).mean()
loss = (-delta.where(delta < 0, 0)).rolling (window=3).mean()
rs = gain / loss
result['SpO2_RSI'] = 100 - (100 / (1 + rs))

result.fillna(0, inplace=True)