#imports
import pandas as pd
from scipy.stats import gamma

# reading from Excel file
DB_Output = pd.read_excel('new_collapsed_segments.xlsx')

# showing output
DB_Output
df = DB_Output.copy()
df.info()
df

# rename Highway Class Code column to join on the SPF Highway Class Code column (requires same name)
df.rename(columns={'HighwayClassCode': 'HwyClass'}, inplace=True)
df['HwyClass'] = df['HwyClass'].astype(str)

# load in the SPF values for highway classes
HC = pd.read_excel("2022_SPFs.xlsx")
HC

# rename Highway Class Code column to match the df's column name (to join on this column)
HC.rename(columns={'HwyClassCode': 'HwyClass'}, inplace=True)
HC['HwyClass'] = HC['HwyClass'].astype(str)
HC.info()

# defining which hwy classes do not have spf values to be excluded from the ranking process
no_spf_hwy_clss = [1, 2, 3, 4, 6, 8, 9, 11, 14, 18, 26, 27, 29, 30, 31, 35, 41, 42, 43]

# filtering AvgAADT > 0 and SectionLength > 0
null_aadt = df.loc[(df['AADT'].isnull())]
null_aadt


# filtering AvgAADT > 0 and SectionLength > 0
df1 = df.loc[(df['AADT'] > 0) & (df['SegmentLength'] > 0)]
df1

# changing data type of HwyClass
df1['HwyClass'] = df1['HwyClass'].astype(str)

# Inner joining HC to df1 on HwyClass
df1_HC = pd.merge(df1, HC, how='left', on = 'HwyClass')
df1_HC = df1_HC.sort_values(by=['LRSID'])

# make copy of df1_HC with easier df name
r = df1_HC.copy()

# changing AvgAADT from int to float
r['AADT'] = r['AADT'].astype(float)
null_aadt['AADT'] = null_aadt['AADT'].astype(float)


# Number of years sampled in crash data:
YS = 5.0


# Defining Total Crashes over the last 5 years
r['TL_5y'] = r['TotalCrashes']
null_aadt['TL_5y'] = null_aadt['TotalCrashes']


# In[24]:

# Defining Fatal, Serious, Minor, and Possible Injury Crashes over the last 5 years
r['FI_5y'] = r['Fatal'] + r['Serious'] + r['Minor'] + r['Possible']
null_aadt['FI_5y'] = null_aadt['Fatal'] + null_aadt['Serious'] + null_aadt['Minor'] + null_aadt['Possible']

# Defining Fatal, Serious, and Minor Injury Crashes over the last 5 years
r['FSM_5y'] = r['Fatal'] + r['Serious'] + r['Minor']
null_aadt['FSM_5y'] = null_aadt['Fatal'] + null_aadt['Serious'] + null_aadt['Minor']


# r.loc[r['FSM_5y'] >= 5]

# Calculating Total Crashes per Year
r['TLCpY'] = r['TL_5y']/YS
null_aadt['TLCpY'] = null_aadt['TL_5y']/YS


# Calculating Total Crashes per Mile per Year
r['TLCpMpY'] = r['TLCpY']/r['SegmentLength']
null_aadt['TLCpMpY'] = null_aadt['TLCpY']/null_aadt['SegmentLength']


# Defining the Predicted Crashes SPF function
def SPF(L,AADT,y,b1,b2,b3,b4):
    Predicted = L * YS * y * (b4 + ((b1 * AADT**b2)/(AADT**b2 + b3**b2)))
    return Predicted


# Generating the Predicted Total Crashes by using the SPF function
r['PredTL'] = SPF(r['SegmentLength'], r['AADT'], r['Y'], r['TL_B1'], r['TL_B2'], r['TL_B3'], r['TL_B4'])


# Generating the Predicted Total Crashes per Mile per Year by using the SPF function
r['PredTLpMpY'] = (r['PredTL']/r['SegmentLength']/YS)


# c$PSI_Limit_TL <- qgamma(0.8,TLod,,(c$PredTLpMpY/TLod))
r['PSI_Limit_TL'] = gamma.ppf(0.8, r['TL_OD'], scale=(r['PredTLpMpY']/r['TL_OD']))


# Defining the Expected Crashes SPF function
r['Expected_TL'] = ((1/(1 + r['PredTL']/(r['TL_OD'] * r['SegmentLength']))) * r['PredTL']
                    + (1 - (1/(1 + r['PredTL']/(r['TL_OD'] * r['SegmentLength'])))) * r['TL_5y'])/r['SegmentLength']/YS

# c$PSI_Limit_TL_Low <- qgamma(0.2,TLod,,(c$PredTLpMpY/TLod))
r['PSI_Limit_TL_Low'] = gamma.ppf(0.2, r['TL_OD'], scale=(r['PredTLpMpY']/r['TL_OD']))


# c$TLCr_Delta <- c$Expected_TL - c$PredTLpMpY
r['TLCr_Delta'] = r['Expected_TL'] - r['PredTLpMpY']


# In[36]:


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


# after joining over 'HwyClss' (which had to be a string since the old highway classes had characters) change 'HwyClss' to an
# int so it can be sorted numerically
r['HwyClass'] = r['HwyClass'].astype(int)


# first order the dataframe by 'HwyClss'
r = r.sort_values(by=['HwyClass'], ascending=[True])


# creating an empty column to be filled in
r['TL_Rank'] = ''
r['TL_Rank'] = r.groupby('HwyClass')['TLCr_Delta'].rank(ascending=False)

# Calculate Fatal and Injury Crashes per Year
r['FICpY'] = r['FI_5y']/YS
null_aadt['FICpY'] = null_aadt['FI_5y']/YS

# Calculate Fatal and Injury Crashes per Mile per Year
r['FICpMpY'] = r['FICpY']/r['SegmentLength']
null_aadt['FICpMpY'] = null_aadt['FICpY']/null_aadt['SegmentLength']

# Generate the Predicted Fatal and Injury Crashes with the SPF function
r['PredFI'] = SPF(r['SegmentLength'], r['AADT'], r['Y'], r['FI_B1'], r['FI_B2'], r['FI_B3'], r['FI_B4'])

# Generate the Predicted Fatal and Injury Crashes per Mile per Year
r['PredFIpMpY'] = (r['PredFI']/r['SegmentLength']/YS)


# Calculate the 80% upper limit
r['PSI_Limit_FI'] = gamma.ppf(0.8, r['FI_OD'], scale=(r['PredFIpMpY']/r['FI_OD']))

# Generate the Expected Fatal and Injury Crashes with the SPF function
r['Expected_FI'] = ((1/(1 + r['PredFI']/(r['FI_OD'] * r['SegmentLength']))) * r['PredFI']
                    + (1 - (1/(1 + r['PredFI']/(r['FI_OD'] * r['SegmentLength'])))) * r['FI_5y'])/r['SegmentLength']/YS


# Generate the 20% lower limit
r['PSI_Limit_FI_Low'] = gamma.ppf(0.2, r['FI_OD'], scale=(r['PredFIpMpY']/r['FI_OD']))


# Generate the Fatal and Injury Crashes Delta
r['FICr_Delta'] = r['Expected_FI'] - r['PredFIpMpY']


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


# ### Deleting LOSS levels for segments without SPF values
null_rows = r[r['Y'].isnull()]
r.loc[null_rows.index, ['TL_LOSS', 'FI_LOSS']] = ''


# first order the dataframe by 'HwyClss'
r = r.sort_values(by=['HwyClass'], ascending=[True])


# Define the 'FI_Rank_new' column with default values
r['FI_Rank'] = ''
r['FI_Rank'] = r.groupby('HwyClass')['FICr_Delta'].rank(ascending=False)


# Generate the HPSI List by locating rows which:
# 1. have FSM_5y >= 5 AND
# 2. FI_LOSS = LOSS 4
hpsi_list = r.loc[(r['FSM_5y'] >= 5) & (r['FI_LOSS'] == 'LOSS 4')]

# Next, order the dataframe by 'HwyClss'
hpsi_list = hpsi_list.sort_values(by=['HwyClass'], ascending=[True])

# Generate the HPSI Rank column
hpsi_list['HPSI_Rank'] = 0

# Sort and then rank the HPSI list
hpsi_list = hpsi_list.sort_values(by=['FSM_5y', 'FICr_Delta'], ascending=[False, False])
hpsi_list['HPSI_Rank'] = hpsi_list.groupby('HwyClass').cumcount() + 1

# Final Sorting of HPSI List
hpsi_list = hpsi_list.sort_values(by=['HwyClass', 'HPSI_Rank'], ascending=[True, True])
hpsi_list

# Print the HPSI List
hpsi_list.to_excel("HPSI TESTING.xlsx")

# Generate a coppy of the r df
all_psi_list = r.copy()

# reattach the segments which had "NULL" AADT values to the all_psi_list dataframe
all_psi_list = all_psi_list.append(null_aadt)


# order the dataframe by 'HwyClss'
all_psi_list['HwyClass'] = all_psi_list['HwyClass'].astype(int)
all_psi_list = all_psi_list.sort_values(by=['HwyClass'], ascending=[True])


# Print the PSI List
all_psi_list.to_excel("PSI TESTING.xlsx")


# Table which displays the total number of crashes grouped by the highway class
# total_crashes_per_hwyclass = all_psi_list[['HighwayClassDescription', 'TotalCrashes']].groupby('HighwayClassDescription').sum('TotalCrashes').sort_values(by=['TotalCrashes'], ascending=[False])

# Table which displays the total number of crashes grouped by the parishes
#total_crashes_per_parish = all_psi_list[['Parishes', 'TotalCrashes']].groupby('Parishes').sum('TotalCrashes').sort_values(by=['TotalCrashes'], ascending=[False])

# printing the tables
#total_crashes_per_parish.to_excel('crashes per parish.xlsx')
#total_crashes_per_hwyclass.to_excel('crashes per hwy class.xlsx')