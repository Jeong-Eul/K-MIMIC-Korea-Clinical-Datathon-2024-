def Identification_Stage1(df):
    data = df.copy()

    data['Stage_1_SCr_1'] = np.nan
    data['Stage_1_SCr_2'] = np.nan
    data['Stage_1_Urine'] = np.nan

    for stay in tqdm(data.STAY_ID.unique()):
        stage_1_identification = data[data['STAY_ID'] == stay]
        # true_creatinine = stage_1_identification[stage_1_identification['Serum_Creatinine_presence'] == 1]

        times = stage_1_identification['Time_since_ICU_admission'].values
        serum_creatinine = stage_1_identification['Serum_Creatinine'].values
        

        for idx, t in enumerate(times):
            if t >= 7*24:
                current_creatinine = serum_creatinine[idx]

                # SCr 조건
                before_2days_mask = (times >= t - 2*24) & (times < t)
                window_2days = serum_creatinine[before_2days_mask]

                if window_2days.size > 0:
                    max_scr = window_2days.max()
                    data.loc[data.index[idx], 'Stage_1_SCr_1'] = 1 if (current_creatinine - max_scr >= 0.3) else 0
                else:
                    data.loc[data.index[idx], 'Stage_1_SCr_1'] = 0

                before_7days_mask = (times >= t - 7*24) & (times < t)
                window_7days = serum_creatinine[before_7days_mask]

                if window_7days.size > 0:
                    min_scr = window_7days.min()
                    data.loc[data.index[idx], 'Stage_1_SCr_2'] = 1 if (current_creatinine - min_scr >= 1.5) | (current_creatinine - min_scr < 2) else 0
                else:
                    data.loc[data.index[idx], 'Stage_1_SCr_2'] = 0
                    
        urine_output = stage_1_identification['Urine_Output'].values
        weight = stage_1_identification['Weight'].values            
        for idx, t in enumerate(times):
            if t >= 7*24:
                # urine 조건
                before_24hours_mask = (times >= t - 24) & (times < t)
                total_urine_output = urine_output[before_24hours_mask].sum()/weight[idx]/24

                data.loc[idx, 'Stage_1_Urine'] = 1 if total_urine_output <= 1 else 0
    
            else:
                data.loc[data.index[idx], 'Stage_1_SCr_1'] = 'Not Annotation Target'
                data.loc[data.index[idx], 'Stage_1_SCr_2'] = 'Not Annotation Target'
                data.loc[data.index[idx], 'Stage_1_Urine'] = 'Not Annotation Target'
                

    return data

def Identification_Stage2(df):
    data = df.copy()

    data['Stage_2_SCr'] = np.nan
    data['Stage_2_Urine'] = np.nan

    for stay in tqdm(data.STAY_ID.unique()):
        stage_2_identification = data[data['STAY_ID'] == stay]
        # stage_2_identification = stage_2_identification[stage_2_identification['Serum_Creatinine_presence'] == 1]

        times = stage_2_identification['Time_since_ICU_admission'].values
        serum_creatinine = stage_2_identification['Serum_Creatinine'].values
        urine_output = stage_2_identification['Urine_Output'].values
        weight = stage_2_identification['Weight'].values

        for idx, t in enumerate(times):
            if t >= 7*24:
                current_creatinine = serum_creatinine[idx]

                # SCr 조건
                before_7days_mask = (times >= t - 7*24) & (times < t)
                window_7days = serum_creatinine[before_7days_mask]

                if window_7days.size > 0:
                    min_scr = window_7days.min()
                    data.loc[data.index[idx], 'Stage_2_SCr'] = 1 if (current_creatinine - min_scr >= 2.0) and (current_creatinine - min_scr < 3.0) else 0
                else:
                    data.loc[data.index[idx], 'Stage_2_SCr'] = 0

        for idx, t in enumerate(times):
            if t >= 7*24:
                before_24hours_mask = (times >= t - 24) & (times < t)
                total_urine_output = urine_output[before_24hours_mask].sum()/weight[idx]/24

                data.loc[data.index[idx], 'Stage_2_Urine'] = 1 if total_urine_output <= 0.5 else 0

            
            else:
                data.loc[data.index[idx], 'Stage_2_SCr'] = 'Not Annotation Target'
                data.loc[data.index[idx], 'Stage_2_Urine'] = 'Not Annotation Target'


    return data

def Identification_Stage3(df):
    data = df.copy()

    data['Stage_3_SCr_1'] = np.nan
    data['Stage_3_SCr_2'] = np.nan
    data['Stage_3_Urine'] = np.nan

    for stay in tqdm(data.STAY_ID.unique()):
        stage_1_identification = data[data['STAY_ID'] == stay]
        # stage_1_identification = stage_1_identification[stage_1_identification['Serum_Creatinine_presence'] == 1]

        times = stage_1_identification['Time_since_ICU_admission'].values
        serum_creatinine = stage_1_identification['Serum_Creatinine'].values
        urine_output = stage_1_identification['Urine_Output'].values
        weight = stage_1_identification['Weight'].values

        for idx, t in enumerate(times):
            if t >= 7*24:
                current_creatinine = serum_creatinine[idx]

                # SCr 조건
                before_2days_mask = (times >= t - 2*24) & (times < t)
                window_2days = serum_creatinine[before_2days_mask]

                if window_2days.size > 0:
                    max_scr = window_2days.max()
                    data.loc[data.index[idx], 'Stage_3_SCr_1'] = 1 if (current_creatinine - max_scr >= 0.3) else 0
                else:
                    data.loc[data.index[idx], 'Stage_3_SCr_1'] = 0

                before_7days_mask = (times >= t - 7*24) & (times < t)
                window_7days = serum_creatinine[before_7days_mask]

                if window_7days.size > 0:
                    min_scr = window_7days.min()
                    data.loc[data.index[idx], 'Stage_3_SCr_2'] = 1 if (current_creatinine - min_scr >= 1.5) | (current_creatinine >= 2.5) else 0
                else:
                    data.loc[data.index[idx], 'Stage_3_SCr_2'] = 0
                    
        for idx, t in enumerate(times):
            if t >= 7*24:
                # urine 조건
                before_24hours_mask = (times >= t - 24) & (times < t)
                total_urine_output = urine_output[before_24hours_mask].sum()/weight[idx]/24

                data.loc[data.index[idx], 'Stage_3_Urine'] = 1 if total_urine_output <= 0.3 else 0
    
            else:
                data.loc[data.index[idx], 'Stage_3_SCr_1'] = 'Not Annotation Target'
                data.loc[data.index[idx], 'Stage_3_SCr_2'] = 'Not Annotation Target'
                data.loc[data.index[idx], 'Stage_3_Urine'] = 'Not Annotation Target'
                

    return data

test1 = Identification_Stage1(dataset)
test2 = Identification_Stage2(test1)
test3 = Identification_Stage3(test2)

gogo = test3.fillna(0)
new_data = pd.DataFrame()
for stay in gogo['STAY_ID'].unique():

    interest = gogo[gogo['STAY_ID']==stay]
    interest = interest[(interest['Time_since_ICU_admission'] >= 7*24)]
    new_data = pd.concat([new_data, interest], axis = 0, ignore_index=True).reset_index(drop=True)

new_data['Stage_1'] = new_data['Stage_1_SCr_1'] + new_data['Stage_1_SCr_2'] + new_data['Stage_1_Urine']
new_data['Stage_2'] = new_data['Stage_2_SCr'] + new_data['Stage_2_Urine']
new_data['Stage_3'] = new_data['Stage_3_SCr_1'] + new_data['Stage_3_SCr_2'] + new_data['Stage_3_Urine']

del new_data['Stage_1_SCr_1']
del new_data['Stage_1_SCr_2']
del new_data['Stage_1_Urine']

del new_data['Stage_2_SCr'] 
del new_data['Stage_2_Urine']

del new_data['Stage_3_SCr_1']
del new_data['Stage_3_SCr_2'] 
del new_data['Stage_3_Urine']


new_data['AKI'] = new_data['Stage_1'] + new_data['Stage_2'] + new_data['Stage_3']
new_data['AKI'] = new_data['AKI'].replace({2:1, 3:1, 4:1, 6:1})

for stay_id in tqdm(new_data['STAY_ID'].unique()):
    stay_df = new_data[new_data['STAY_ID'] == stay_id].sort_values(by='Time_since_ICU_admission')
    stay_df['endpoint_window'] = stay_df['Time_since_ICU_admission'] + 24

    for idx, row in stay_df.iterrows():
        current_time = row['Time_since_ICU_admission']
        endpoint_window = row['endpoint_window']

        future_rows = stay_df[(stay_df['Time_since_ICU_admission'] > current_time) & (stay_df['Time_since_ICU_admission'] <= endpoint_window)]

        if any(future_rows['AKI'] == 1):
            new_data.loc[idx, 'AKI_next_1day'] = 1
        else:
            new_data.loc[idx, 'AKI_next_1day'] = 0