from memory_profiler import memory_usage
import pandas as pd
import numpy as np
import os
from tqdm import tqdm
import warnings
pd.set_option('mode.chained_assignment',  None) 
warnings.simplefilter(action='ignore', category=FutureWarning)

import pandasql as psql

def concat_patient(df1, df2):
    data1 = df1.copy()
    data2 = df2.copy()

    return pd.concat([data1, data2], axis = 0, ignore_index=True)


print('Integreating hospital Read')
patient_001_433 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/001/433/ICUSTAYS.csv")
patient_001_434 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/001/434/ICUSTAYS.csv")
patient_001_435 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/001/435/ICUSTAYS.csv")
print(patient_001_433.shape)
print(patient_001_434.shape)
print(patient_001_435.shape)

patient_002_433 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/002/433/ICUSTAYS.csv")
patient_002_434 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/002/434/ICUSTAYS.csv")
patient_002_435 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/002/435/ICUSTAYS.csv")
print(patient_002_433.shape)
print(patient_002_434.shape)
print(patient_002_435.shape)


patient_006_433 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/006/433/ICUSTAYS.csv")
patient_006_434 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/006/434/ICUSTAYS.csv")
print(patient_006_433.shape)
print(patient_006_434.shape)

patient_008_433 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/008/433/ICUSTAYS.csv")
patient_008_434 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/008/434/ICUSTAYS.csv")
patient_008_435 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/008/435/ICUSTAYS.csv")
print(patient_008_433.shape)
print(patient_008_434.shape)
print(patient_008_435.shape)

patient_010_433 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/010/433/ICUSTAYS.csv")
patient_010_434 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/010/434/ICUSTAYS.csv")
patient_010_435 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/010/435/ICUSTAYS.csv")
print(patient_010_433.shape)
print(patient_010_434.shape)
print(patient_010_435.shape)


proc1 = concat_patient(patient_001_433, patient_001_434)
proc2 = concat_patient(proc1, patient_001_435)
proc3 = concat_patient(proc2, patient_002_433)
proc4 = concat_patient(proc3, patient_002_434)
proc5 = concat_patient(proc4, patient_002_435)
proc6 = concat_patient(proc5, patient_006_433)
proc7 = concat_patient(proc6, patient_006_434)
proc9 = concat_patient(proc7, patient_008_433)
proc10 = concat_patient(proc9, patient_008_434)
proc11 = concat_patient(proc10, patient_008_435)
proc12 = concat_patient(proc11, patient_010_433)
proc13 = concat_patient(proc12, patient_010_434)
icustay = concat_patient(proc13, patient_010_435)


print('Integreating hospital Read')
patient_001_433 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/001/433/PATIENTS.csv")
patient_001_434 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/001/434/PATIENTS.csv")
patient_001_435 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/001/435/PATIENTS.csv")
print(patient_001_433.shape)
print(patient_001_434.shape)
print(patient_001_435.shape)

patient_002_433 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/002/433/PATIENTS.csv")
patient_002_434 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/002/434/PATIENTS.csv")
patient_002_435 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/002/435/PATIENTS.csv")
print(patient_002_433.shape)
print(patient_002_434.shape)
print(patient_002_435.shape)


patient_006_433 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/006/433/PATIENTS.csv")
patient_006_434 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/006/434/PATIENTS.csv")
print(patient_006_433.shape)
print(patient_006_434.shape)

patient_008_433 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/008/433/PATIENTS.csv")
patient_008_434 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/008/434/PATIENTS.csv")
patient_008_435 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/008/435/PATIENTS.csv")
print(patient_008_433.shape)
print(patient_008_434.shape)
print(patient_008_435.shape)

patient_010_433 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/010/433/PATIENTS.csv")
patient_010_434 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/010/434/PATIENTS.csv")
patient_010_435 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/010/435/PATIENTS.csv")
print(patient_010_433.shape)
print(patient_010_434.shape)
print(patient_010_435.shape)


proc1 = concat_patient(patient_001_433, patient_001_434)
proc2 = concat_patient(proc1, patient_001_435)
proc3 = concat_patient(proc2, patient_002_433)
proc4 = concat_patient(proc3, patient_002_434)
proc5 = concat_patient(proc4, patient_002_435)
proc6 = concat_patient(proc5, patient_006_433)
proc7 = concat_patient(proc6, patient_006_434)
proc9 = concat_patient(proc7, patient_008_433)
proc10 = concat_patient(proc9, patient_008_434)
proc11 = concat_patient(proc10, patient_008_435)
proc12 = concat_patient(proc11, patient_010_433)
proc13 = concat_patient(proc12, patient_010_434)
proc14 = concat_patient(proc13, patient_010_435)

nicu_snjid = pd.read_csv('target.csv') #patient who first admit was NICU
Nicu_patient_id = nicu_snjid.SUBJECT_ID.unique()

variable_name_dict = {
    '001C_1003_24480':'SpO2',
    '001C_20210_24480':'SpO2',
    '001C_1012_24795':'SBP',
    '001C_1015_24795':'SBP',
    '001C_1018_24795':'SBP',
    '001C_1013_25640': 'DBP', 
    '001C_1016_25640': 'DBP', 
    '001C_1019_25640': 'DBP', 
    '002C_21002_150000003_01': 'DBP', 
    '002C_21002_150000006_01': 'DBP',
    '001C_1021_25105': 'HR', '001C_1647_26805': 'HR', '001C_1903_26805': 'HR', '001C_1973_26805': 'HR', '001C_2127_26805': 'HR', '001C_2136_26805': 'HR',
    '001O_1635_24120': 'RRT_Urine_Output', '001O_1804_26960': 'RRT_Urine_Output', '001O_1890_26955': 'RRT_Urine_Output', '001O_2090_24095': 'RRT_Urine_Output', '002O_21046_220330_01': 'RRT_Urine_Output',
    '001O_1481_25470': 'Urine_Output', '001O_1479_25880': 'Urine_Output', '001O_1482_23585': 'Urine_Output', '001O_1483_25905': 'Urine_Output', '002O_21046_220295_01': 'Urine_Output', '002O_21046_150000300_01': 'Urine_Output',
    '008O_2115800414': 'Urine_Output', '008O_2115800415': 'Urine_Output', '008O_2115800461': 'Urine_Output', '008O_2115800462': 'Urine_Output',
    '008L322202': 'Serum_Creatinine', '010L310601': 'Serum_Creatinine', '010L310701': 'Serum_Creatinine', '010L933901': 'Serum_Creatinine', '010L1148A1': 'Serum_Creatinine', '001L31252': 'Serum_Creatinine', '001L3041': 'Serum_Creatinine', '001L0031': 'Serum_Creatinine', '001L31242': 'Serum_Creatinine', '001L8135': 'Serum_Creatinine',
    '006L8434': 'Serum_Creatinine', '008L3022': 'Serum_Creatinine', '008L302201': 'Serum_Creatinine', '010L1118': 'Serum_Creatinine', '010L3106': 'Serum_Creatinine','010L310603': 'Serum_Creatinine', '010L904016': 'Serum_Creatinine', '010L9325': 'Serum_Creatinine', '010L1148A': 'Serum_Creatinine', '010L3025': 'Serum_Creatinine',
   '010L8022': 'Serum_Creatinine', '010L9015': 'Serum_Creatinine', '010L932501': 'Serum_Creatinine', '010L302501': 'Serum_Creatinine', '010L802201': 'Serum_Creatinine','010L1148A3': 'Serum_Creatinine', '010L111801': 'Serum_Creatinine',
    '001200538800007753880':'Theophylline(Methylxanthines)', '001200502770015350277':'aminophyline(Methylxanthines)',
    '1200501430063250143':'Acyclovir', '1200508400058650840':'Acyclovir', '001880603810194650113':'Acyclovir', '001200501430035950143':'Acyclovir',
    '1201508370026450837':'Amikacin', '001201508370026450837':'Amikacin','001880648502471350837':'Amikacin', '1880648502471350837':'Amikacin',
    '1200518130001551813': 'Amphotericin B', '001200518130001551813': 'Amphotericin B', '001200508370019750837' : 'Gentamicin',
    '1880643303671350162':'piperacillin/tazobactam', '001880643303671350162':'piperacillin/tazobactam','001880643303661450162':'piperacillin/tazobactam', '1880643303661450162':'piperacillin/tazobactam',
    '1201501300120150130': 'Vancomycin', '1200501300096050130': 'Vancomycin','001200501300096050130': 'Vancomycin', '001201501300120150130': 'Vancomycin','001200501430072450143': 'Vancomycin',
}

def Data_Preprocessing_Pipeline(day, hospital_id):
    # global chart_lab_out_merged, testing1, testing2, c_part_1, o_part_1, l_part_1, p_part_1, e_part_1, chart_part_1, lab_part_1
    # inter = icustay[icustay['FIRST_CAREUNIT'] == 'SICU']
    # sicu_patient_id = inter[inter['SUBJECT_ID'].isin(target[target['AGE']>=18].SUBJECT_ID.unique())].SUBJECT_ID.unique()

    nicu_snjid = pd.read_csv('target.csv')
    Nicu_patient_id = nicu_snjid.SUBJECT_ID.unique()
    
    # feature selection---
    
    spo2 = ['001C_1003_24480', '001C_20210_24480']
    SBP = ['001C_1012_24795', '001C_1015_24795', '001C_1018_24795']
    DBP = ['001C_1013_25640', '001C_1016_25640', '001C_1019_25640', '002C_21002_150000003_01', '002C_21002_150000006_01']
    HR = ['001C_1021_25105', '001C_1647_26805', '001C_1903_26805', '001C_1973_26805', '001C_2127_26805', '001C_2136_26805']
    
    RRT = ['001O_1635_24120', '001O_1804_26960', '001O_1890_26955', '001O_2090_24095', '002O_21046_220330_01']
    Foley_catheter = ['001O_1481_25470']
    self_voiding = ['001O_1479_25880']
    Neleton = ['001O_1482_23585']
    residual_urine = ['001O_1483_25905', '002O_21046_220295_01']
    general_urine = ['002O_21046_150000300_01', '008O_2115800414', '008O_2115800415', '008O_2115800461', '008O_2115800462'] # 소변의 경우 하루 8번 기록이 되어 있어야 함
    
    Urine_output = list(set(RRT)|set(Foley_catheter)|set(self_voiding)|set(Neleton)|set(residual_urine)|set(general_urine))
    
    serum_creatinine = ['008L322202', '010L310601', '010L310701', '010L933901', '010L1148A1', '001L31252', '001L3041', '001L0031', '001L31242', '001L8135',
                        '006L8434', '008L3022', '008L302201', '010L1118', '010L3106','010L310603', '010L904016', '010L9325', '010L1148A', '010L3025',
                       '010L8022', '010L9015', '010L932501', '010L302501', '010L802201','010L1148A3', '010L111801']
    
    # input event - Aminophylline(Methylxanthines) 002I_21042_321130_01
    # emar - Theophylline(Methylxanthines) 001200538800007753880
    # emar - aminophyline(Methylxanthines)  001200502770015350277
    # prescription - Theophylline(Methylxanthines) 001200538800007753880
    # prescription - aminophyline(Methylxanthines) 001200502770015350277
    # prescription - oxicam(NSAID) 001200502770015350277
    
    # acyclovir - '1200501430063250143', '1200508400058650840', '001880603810194650113', '001200501430035950143'
    # amikacin - '1201508370026450837', '001201508370026450837','001880648502471350837', '1880648502471350837'
    # amphotericin b - '1200518130001551813', '001200518130001551813'
    # gentamicin - '001200508370019750837'
    # indomethacin
    # piperacillin/tazobactam - '1880643303671350162', '001880643303671350162','001880643303661450162', '1880643303661450162'
    # vancomycin - '1201501300120150130', '1200501300096050130','001200501300096050130', '001201501300120150130','001200501430072450143'
    
    # 투석
    # Anastomosis for renal dialysis, others -  001P_OPFG120105
    # Insertion of peritoneal dialysis catheter - 001P_OPFI130901
    # Insertion of peritoneal dialysis catheter with greater omentectomy - 002P_H2304_01
    # Angioplasty : hemodialysis fistula	- 001P_OPFN091309
    # AV Fistula(Artificial vein) for Hemodialysis - 002P_H3235_01, 002P_H3234_01
    
    drug_administration = ['002I_21042_321130_01', '001200538800007753880', '001200502770015350277', '001200538800007753880',
                           '001200502770015350277', '001200502770015350277', '1200501430063250143', '1200508400058650840', 
                           '001880603810194650113', '001200501430035950143', '1201508370026450837', '001201508370026450837',
                           '001880648502471350837', '1880648502471350837', '1200518130001551813', '001200518130001551813',
                          '001200508370019750837', '1880643303671350162', '001880643303671350162','001880643303661450162',
                           '1880643303661450162', '1201501300120150130', '1200501300096050130','001200501300096050130',
                           '001201501300120150130','001200501430072450143']
    
    use_col = ['SUBJECT_ID', 'STAY_ID', 'CHARTTIME', 'ITEMID', 'VALUENUM'] #chartevent use col
    o_use_col = ['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'ITEMID', 'VALUE'] #outputevent use col
    l_use_col = ['SUBJECT_ID', 'STAY_ID', 'CHARTTIME', 'ITEMID', 'VALUENUM'] #labevent use col
    
    inp_usecol = ['SUBJECT_ID', 'ICUSTAY_ID', 'STARTTIME', 'ENDTIME', 'ITEMID'] #inputevent use col
    pres_usecol = ['SUBJECT_ID', 'STAY_ID', 'STARTTIME', 'ITEMID'] #prescriptions use col
    emar_usecol = ['SUBJECT_ID', 'STAY_ID', 'CHARTTIME','ITEMID', 'EMAR_TYPE'] #emar use col
    
    proc_usecol = ['SUBJECT_ID', 'STAY_ID', 'STARTTIME', 'ENDTIME', 'ITEMID'] #procedures
    dialysis = ['001P_OPFG120105', '001P_OPFI130901', '002P_H2304_01', '001P_OPFN091309', '002P_H3235_01', '002P_H3234_01']
    
    item_id_set = set(spo2) | set(SBP) | set(HR) | set(Urine_output) | set(serum_creatinine) | set(drug_administration) | set(dialysis)
    item_id_list = list(item_id_set)
    
    chart_part_1 = pd.read_csv(f"/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/{day}/{hospital_id}/CHARTEVENTS.csv", usecols = use_col)
    c_part_1 = chart_part_1[(chart_part_1['ITEMID'].isin(item_id_list)) & (chart_part_1['SUBJECT_ID'].isin(Nicu_patient_id))].dropna(subset=['STAY_ID']) # Stay id 가 Null 인 것 제거
    
    output_part_1 = pd.read_csv(f"/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/{day}/{hospital_id}/OUTPUTEVENTS.csv", usecols = o_use_col)
    o_part_1 = output_part_1[(output_part_1['ITEMID'].isin(item_id_list)) & (output_part_1['SUBJECT_ID'].isin(Nicu_patient_id))].dropna(subset=['ICUSTAY_ID'])
    
    lab_part_1 = pd.read_csv(f"/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/{day}/{hospital_id}/LABEVENTS.csv", usecols = l_use_col)
    l_part_1 = lab_part_1[(lab_part_1['ITEMID'].isin(item_id_list)) & (lab_part_1['SUBJECT_ID'].isin(Nicu_patient_id))].dropna(subset=['STAY_ID'])
    
    # inp_part_1 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/001/433/INPUTEVENTS.csv", usecols = inp_usecol)
    # i_part_1 = inp_part_1[(inp_part_1['ITEMID'].isin(item_id_list)) & (inp_part_1['SUBJECT_ID'].isin(Nicu_patient_id))].dropna(subset=['ICUSTAY_ID'])  #start time, stop time 존재
    
    pres_part_1 = pd.read_csv(f"/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/{day}/{hospital_id}/PRESCRIPTIONS.csv", usecols = pres_usecol)
    p_part_1 = pres_part_1[(pres_part_1['ITEMID'].isin(item_id_list)) & (pres_part_1['SUBJECT_ID'].isin(Nicu_patient_id))].dropna(subset=['STAY_ID']) #start time, stop time 존재
    
    emar_part_1 = pd.read_csv(f"/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/{day}/{hospital_id}/EMAR.csv", usecols = emar_usecol)
    e_part_1 = emar_part_1[(emar_part_1['ITEMID'].isin(item_id_list)) & (emar_part_1['SUBJECT_ID'].isin(Nicu_patient_id))].dropna(subset=['STAY_ID'])
    
    # proc_part_1 = pd.read_csv("/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/001/433/PROCEDUREEVENTS.csv", usecols = proc_usecol)
    # pr_part_1 = proc_part_1[(proc_part_1['ITEMID'].isin(item_id_list)) & (proc_part_1['SUBJECT_ID'].isin(Nicu_patient_id))].dropna(subset=['STAY_ID'])
    
    # Procedure event에 RRT는 없었음
    
    del chart_part_1
    del output_part_1
    del lab_part_1
    # del inp_part_1
    # del pres_part_1
    del emar_part_1
    
    # c_part_1.to_csv('./PreprocSet/feature_selection/preproc_chart.csv.gz')
    # l_part_1.to_csv('./PreprocSet/feature_selection/preproc_lab.csv.gz')
    
    o_part_1.rename(columns = {'ICUSTAY_ID': 'STAY_ID', 'VALUE': 'VALUENUM'}, inplace = True)
    # i_part_1.rename(columns = {'ICUSTAY_ID': 'STAY_ID', 'ENDTIME':'STOPTIME'}, inplace = True)
    
    p_part_1.rename(columns = {'ICUSTAY_ID': 'STAY_ID', 'STARTTIME': 'CHARTTIME'}, inplace = True)
    e_part_1.rename(columns = {'EMAR_TYPE': 'VALUENUM'}, inplace = True)
    
    # pr_part_1.rename(columns = {'ENDTIME':'STOPTIME'}, inplace = True)
    
    #Discretization
    
    icustays = pd.read_csv(f"/mnt/dataset/dataset-2064568626958041088/K-MIMIC/EMR/{day}/{hospital_id}/ICUSTAYS.csv")
    interest_adm = icustays[icustays['SUBJECT_ID'].isin(Nicu_patient_id)][['SUBJECT_ID', 'INTIME', 'OUTTIME']]
    
    # chart, lab, prescription 동시에
    chart_lab = concat_patient(c_part_1, l_part_1[c_part_1.columns]) 
    
    p_part_1['VALUENUM'] = 1
    e_part_1['VALUENUM'] = 1
    
    testing1 = chart_lab.copy()
    chart_lab_out = concat_patient(chart_lab, p_part_1[c_part_1.columns].dropna(subset=['STAY_ID'])) 
    chart_lab_out = concat_patient(chart_lab_out, e_part_1[c_part_1.columns].dropna(subset=['STAY_ID'])) 
    
    chart_lab_out = concat_patient(chart_lab_out, o_part_1[c_part_1.columns].dropna(subset=['STAY_ID'])) 
    
    chart_lab_out_merged = pd.merge(chart_lab_out, interest_adm[['SUBJECT_ID', 'INTIME']], how = 'left', on = ['SUBJECT_ID'])
    chart_lab_out_merged['CHARTTIME'] = chart_lab_out_merged['CHARTTIME'].str.replace('T',' ').astype('str')
    chart_lab_out_merged['INTIME'] = chart_lab_out_merged['INTIME'].str.replace('T',' ').astype('str') 

    if len(chart_lab_out_merged) == 0:
        return
    else:
    
        charttime = chart_lab_out_merged['CHARTTIME'].values
        chart_list = []
        
        admittime = chart_lab_out_merged['INTIME'].values
        admit_list = []
        
        for d in charttime:
            year = int(d[:4])
            chart_list.append(year)
        
        for d in admittime:
            year = int(d[:4])
            admit_list.append(year)
        
        year_difference = np.array(chart_list) - np.array(admit_list)
        
        def random_year():
            return np.random.randint(2003, 2020)
        
        # 년도 변경 함수
        def change_year_chart(date_str):
            output = []
            for d in date_str:
                year = d[:4]  # 앞의 4자리로 년도 추출
                new_year = str(random_year())
                new_date_str = d.replace(year, new_year, 1)  # 첫 번째 항목만 변경
                output.append(new_date_str)
            return output
        
        def change_year_admit(date_str, chart_year , year_difference):
            output = []
            for idx, d in enumerate(date_str):
                year = d[:4]  # 앞의 4자리로 년도 추출
                new_year = int(chart_year[idx][:4]) - year_difference[idx]
                new_date_str = d.replace(year, str(new_year), 1)  # 첫 번째 항목만 변경
                output.append(new_date_str)
            return output
        
        chart_lab_out_merged['CHARTTIME'] = change_year_chart(chart_lab_out_merged['CHARTTIME'].values)
        chart_lab_out_merged['INTIME'] = change_year_admit(chart_lab_out_merged['INTIME'].values,chart_lab_out_merged['CHARTTIME'].values, year_difference)
        
        chart_lab_out_merged['CHARTTIME'] = chart_lab_out_merged['CHARTTIME'].astype('str')
        chart_lab_out_merged['INTIME'] = chart_lab_out_merged['INTIME'].astype('str')   
        
        chart_lab_out_merged['CHARTTIME'] = pd.to_datetime(chart_lab_out_merged['CHARTTIME'].values, errors = 'coerce')
        chart_lab_out_merged['INTIME'] = pd.to_datetime(chart_lab_out_merged['INTIME'].values, errors = 'coerce')
        chart_lab_out_merged['Time_since_ICU_admission'] = chart_lab_out_merged['CHARTTIME'] - chart_lab_out_merged['INTIME']
        
        del chart_lab_out_merged['CHARTTIME']
        del chart_lab_out_merged['INTIME']
    
        chart_lab_out_merged[['start_days', 'dummy','start_hours']] = chart_lab_out_merged['Time_since_ICU_admission'].astype('str').str.split(' ', expand=True)
        chart_lab_out_merged[['start_hours','min','sec']] = chart_lab_out_merged['start_hours'].str.split(':', expand=True)
        chart_lab_out_merged.dropna(inplace=True)
        
        chart_lab_out_merged['start_time'] = pd.to_numeric(chart_lab_out_merged['start_days'])*24+pd.to_numeric(chart_lab_out_merged['start_hours'])
        
        chart_lab_out_merged=chart_lab_out_merged.drop(columns=['start_days', 'dummy','start_hours','min','sec'])
        chart_lab_out_merged=chart_lab_out_merged[chart_lab_out_merged['start_time']>=0]
        del chart_lab_out_merged['Time_since_ICU_admission']
        
        gc.collect()
    
        chart_lab_out_merged.to_csv(f'./tmp/PreprocSet/Checkpoint/{day}/{hospital_id}/ckpt.csv')
    
        del chart_lab_out_merged

hosp_id = ['433', '434', '435']
days = ['002', '001', '006', '008', '010']
for d in days:
    if d == '006':
        new_hosp_id = ['433', '434']
        for h in new_hosp_id:

            if not os.path.exists(f"tmp/PreprocSet/Checkpoint/{d}/{h}"):
                os.makedirs(f"tmp/PreprocSet/Checkpoint/{d}/{h}")
            Data_Preprocessing_Pipeline(d, h)
    else:
        for h in hosp_id:
            if not os.path.exists(f"tmp/PreprocSet/Checkpoint/{d}/{h}"):
                os.makedirs(f"tmp/PreprocSet/Checkpoint/{d}/{h}")
            Data_Preprocessing_Pipeline(d, h)

local = 'tmp/PreprocSet'




