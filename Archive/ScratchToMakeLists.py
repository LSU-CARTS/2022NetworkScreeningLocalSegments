# import statements
import pandas as pd
import numpy as np
from scipy.stats import gamma
import scipy
import pyodbc


# for the time being, manually upload collapsed segment data
DB_output = pd.read_excel('2022_Local_Segments_Collapsed.xlsx')

DB_output.info()

df = DB_output.copy()

# Changing Data Types
# int
df['Fatal'] = df['Fatal'].astype(int)
df['Serious'] = df['Serious'].astype(int)
df['Minor'] = df['Minor'].astype(int)
df['Possible'] = df['Possible'].astype(int)
df['PDO'] = df['PDO'].astype(int)
df['TotalCrashes'] = df['TotalCrashes'].astype(int)

#float
df['BeginLogMile'] = df['BeginLogMile'].astype(float)
df['EndLogMile'] = df['EndLogMile'].astype(float)
df['SegmentLength'] = df['SegmentLength'].astype(float)
df['AADT'] = df['AADT'].astype(float)

#string
df['LRSID'] = df['LRSID'].astype(str)
df['DIRECTION'] = df['DIRECTION'].astype(str)
df['LLL_SequentialOccurance'] = df['LLL_SequentialOccurance'].astype(str)
df['HighwayClassDescription'] = df['HighwayClassDescription'].astype(str)
df['HighwayClassCode'] = df['HighwayClassCode'].astype(str)
df['RouteNames'] = df['RouteNames'].astype(str)
df['Parishes'] = df['Parishes'].astype(str)
df['DOTDDistricts'] = df['DOTDDistricts'].astype(str)

# rename the highway class field for joining later
# change data type to string
df.rename(columns={'HighwayClassCode': 'HwyClass'}, inplace=True)
df['HwyClass'] = df['HwyClass'].astype(str)

# import SPF values
HC = pd.read_excel("2022_SPFs.xlsx")

# change the data type of highway class in SPF file
HC['HwyClass'] = HC['HwyClass'].astype(str)

# defining which hwy classes do not have spf values to be excluded from the ranking process
no_spf_hwy_clss = [1, 2, 3, 4, 6, 8, 9,
                   11, 14, 18, 26, 27, 29, 30, 31, 35, 41, 42, 43]

# look through the data where AADT is null
# returns 467 rows
null_aadt = df.loc[(df['AADT'].isnull())]
null_aadt

# filtering AvgAADT > 0 and SectionLength > 0
# returns 6075 rows
df1 = df.loc[(df['AADT'] > 0) & (df['SegmentLength'] > 0)]
df1

df1.info()
df1['HwyClass'] = df1['HwyClass'].astype(str)

# Inner joining HC to df1 on HwyClass
# returns 6047 records (this result excluded HwyClass 'I' -> 28 records removed)
# HwyClass 'I' does not exist in HC
df1_HC = pd.merge(df1, HC, how='inner', on = 'HwyClass')
df1_HC = df1_HC.sort_values(by=['ControlSection'])
df1_HC

# make copy of df1_HC with easier df name
r = df1_HC.copy()
r.info()

# changing AvgAADT from int to float
r['AADT'] = r['AADT'].astype(float)


# Total Crashes (TL) = Fatal, Serious, Minor, Possible, PDO
# Fatal and Injury Crashes (FI) = Fatal, Serious, Minor, Possible
# Fatal, Serious, Minor Crashes (FSM) = Fatal, Serious, Minor
# Years Sampled
YS = 5.0

# Defining Calculations
r['TL_5y'] = r['TotalCrashes']  # total over the past 5 years
r['FI_5y'] = r['Fatal'] + r['Serious'] + r['Minor'] + r['Possible']  # Fatal Injury over past 5 years
r['FSM_5y'] = r['Fatal'] + r['Serious'] + r['Minor']  # Fatal Serious Minor Crashes over last 5 years

r['TLCpY'] = r['TL_5y']/YS  # total crashes per year
r['TLCpMpY'] = r['TLCpY']/r['SegmentLength']  # total crashes per mile per year

# SPF
def SPF(L,AADT,y,b1,b2,b3,b4):
    Predicted = L * YS * y * (b4 + ((b1 * AADT**b2)/(AADT**b2 + b3**b2)))
    return Predicted

r['PredTL'] = SPF(r['SegmentLength'], r['AADT'],
                  r['Y'], r['TL_B1'], r['TL_B2'], r['TL_B3'], r['TL_B4'])  # predicted total crashes

r['PredTLpMpY'] = (r['PredTL']/r['SegmentLength']/YS)  # predicted total crashes per mile per year

r['PSI_Limit_TL'] = gamma.ppf(0.8, r['TL_OD'], scale=(r['PredTLpMpY']/r['TL_OD']))  # High LOSS boundary (80%)

r['Expected_TL'] = ((1/(1 + r['PredTL']/(r['TL_OD'] * r['SegmentLength']))) * r['PredTL']
                    + (1 - (1/(1 + r['PredTL']/(r['TL_OD'] * r['SegmentLength']))))
                    * r['TL_5y'])/r['SegmentLength']/YS  # expected total crashes

r['PSI_Limit_TL_Low'] = gamma.ppf(0.2, r['TL_OD'], scale=(r['PredTLpMpY']/r['TL_OD']))  # low LOSS boundary (20%)

r['TLCr_Delta'] = r['Expected_TL'] - r['PredTLpMpY']  # delta for total crashes

# creating an empty column for TL_LOSS
# this will be filled in with the appropriate values based upon the following conditions
r['TL_LOSS'] = ''

# defining which conditions will be appied to the dataset
def conditionss(cc):
    if (cc['Expected_TL'] > cc['PSI_Limit_TL']):
        return 'LOSS 4'
    elif (cc['Expected_TL']> cc['PredTLpMpY']):
        return 'LOSS 3'
    elif (cc['Expected_TL'] > cc['PSI_Limit_TL_Low']):
        return 'LOSS 2'
    #elif (cc['HwyClass'] in no_spf_hwy_clss):
        #return 'NO RANK'
    else:
        return 'LOSS 1'

# use the built-in function, 'apply' to apply the conditions to the appropriate column/df
# applying the conditions to each row (axis=1 to apply to each row instead of each column-->which would be axis=0)
r['TL_LOSS'] = r.apply((conditionss), axis=1)

# turn HwyClass field into int
r['HwyClass'] = r['HwyClass'].astype(int)

# total crashes ranking
# first order the dataframe by 'HwyClss'
r = r.sort_values(by=['HwyClass'], ascending=[True])

# creating an empty column to be filled in
r['TL_Rank'] = 0

# descending order for 'TLCr_Delta' creates the ranking
# Loop through each row and update 'TL_Rank_new' based on the condition
for index, row in r.iterrows():
    if row['HwyClass'] not in no_spf_hwy_clss:
        r.loc[index, 'TL_Rank'] = r.loc[r['HwyClass'] == row['HwyClass']].sort_values(by='TLCr_Delta', ascending=False).index.get_loc(index) + 1

# fatal and injury crashes
r['FICpY'] = r['FI_5y']/YS  # total fatal and injury crash per year over 5 years

r['FICpMpY'] = r['FICpY']/r['SegmentLength']  # fatal and injury crashes per mile  per year

r['PredFI'] = SPF(r['SegmentLength'], r['AADT'], r['Y'], r['FI_B1'], r['FI_B2'], r['FI_B3'], r['FI_B4'])  # predicted fatal and injury crashes

r['PredFIpMpY'] = (r['PredFI']/r['SegmentLength']/YS)  # predicted fatal and injury crashes per mile per year

r['Expected_FI'] = ((1/(1 + r['PredFI']/(r['FI_OD'] * r['SegmentLength']))) * r['PredFI']
                    + (1 - (1/(1 + r['PredFI']/(r['FI_OD'] * r['SegmentLength'])))) * r['FI_5y'])/r['SegmentLength']/YS  # expected fatal and injury crashes

r['FICr_Delta'] = r['Expected_FI'] - r['PredFIpMpY']  # delta for fatal and injury crashes

r['PSI_Limit_FI'] = gamma.ppf(0.8, r['FI_OD'], scale=(r['PredFIpMpY']/r['FI_OD']))  # High LOSS boundary limit (80%)

r['PSI_Limit_FI_Low'] = gamma.ppf(0.2, r['FI_OD'], scale=(r['PredFIpMpY']/r['FI_OD']))  # Low LOSS boundary limit (20%)

# creating an empty column for FI_LOSS
# this will be filled in with the appropriate values based upon the following conditions
r['FI_LOSS'] = ''

# defining which conditions will be appied to the dataset
def conditions(c):
    if (c['Expected_FI'] > c['PSI_Limit_FI']):
        return 'LOSS 4'
    elif (c['Expected_FI'] > c['PredFIpMpY']):
        return 'LOSS 3'
    elif (c['Expected_FI'] > c['PSI_Limit_FI_Low']):
        return 'LOSS 2'
    #elif (c['HwyClass'] in no_spf_hwy_clss):
        #return 'NO RANK'
    else:
        return 'LOSS 1'

# use the built-in function, 'apply' to apply the conditions to the appropriate column/df
# applying the conditions to each row (axis=1 to apply to each row instead of each column-->which would be axis=0)
r['FI_LOSS'] = r.apply((conditions), axis=1)


# FI Ranking
# first order the dataframe by 'HwyClss'
r = r.sort_values(by=['HwyClass'], ascending=[True])

# Define the 'FI_Rank_new' column with default values
r['FI_Rank'] = 0

# Loop through each row and update 'FI_Rank_new' based on the condition
for index, row in r.iterrows():
    if row['HwyClass'] not in no_spf_hwy_clss:
        r.loc[index, 'FI_Rank'] = r.loc[r['HwyClass'] == row['HwyClass']].sort_values(by='FICr_Delta', ascending=False).index.get_loc(index) + 1

# criteria for HPSI
# 1. FSM_5y >= 5 AND
# 2. FI_LOSS == 'LOSS 4'

# for five years (2018-2022)
hpsi_list = r.loc[(r['FSM_5y'] >= 5) & (r['FI_LOSS'] == 'LOSS 4')]

# first order the dataframe by 'HwyClss'
hpsi_list = hpsi_list.sort_values(by=['HwyClass'], ascending=[True])

# HPSI Ranking
hpsi_list['HPSI_Rank'] = 0
hpsi_list = hpsi_list.sort_values(by=['FSM_5y', 'FICr_Delta'], ascending=[False, False])
hpsi_list['HPSI_Rank'] = hpsi_list.groupby('HwyClass').cumcount() + 1
hpsi_list = hpsi_list.sort_values(by=['HwyClass', 'HPSI_Rank'], ascending=[True, True])

# prining HPSI List
# hpsi_list.to_excel("All_HPSI_2022_updated.xlsx")

# Prining PSI List
# first order the dataframe by 'HwyClss'
all_psi_list = r.copy()
all_psi_list = all_psi_list.sort_values(by=['HwyClass'], ascending=[True])
# all_psi_list.to_excel("All_PSI_2022_updated.xlsx")