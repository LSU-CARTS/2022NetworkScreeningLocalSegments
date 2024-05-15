# import statements
import pandas as pd
import numpy as np
from scipy.stats import gamma
import scipy
import pyodbc

# Connect to Local Instance (SQL Server Management Studio)
server = '.\RP_Analytics'
database = 'LOCAL'

# Create connection string
conn_str = (
        r"Driver={ODBC Driver 17 for SQL Server};"
        f"Server={server};"
        f"Database={database};"
        r"Trusted_Connection=yes"
    )

# Establish Connection and Cursor
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Copy SQL Data Preparation Queries
# Convert to String for Python to correctly interpret
# By the way, this portion of SQL code CANNOT contain any notes within the queries--Python will misinterpret if so
# At the end, the (replace, '/n', ' ').(replace, '/t', ' ') is to remove the new lines and tabs from the
# string conversion

# Query 1
NSstring = '''
CREATE TABLE CollapsedSegments2022 (
LRSID nvarchar(225) NOT NULL,
LLL_SequentialOccurance nvarchar(225) NOT NULL,
HighwayClassCode nvarchar(20) NOT NULL,
AADT float NULL,
BeginLogMile float NOT NULL,
EndLogMile float NOT NULL,
SegmentLength float NOT NULL,
SubsectionCount int NULL,
RouteNames nvarchar(500) NULL,
ParishCodes nvarchar(50) NULL,
Parishes nvarchar(500) NULL,
DOTDDistricts nvarchar(50) NULL,
CONSTRAINT PK_CollapsedSegments2022 PRIMARY KEY (LRSID, LLL_SequentialOccurance, BeginLogMile, EndLogMile, HighwayClassCode, SegmentLength)
);

WITH LocalRoads AS
(
SELECT  LRSID
,HighwayClassCode
,BeginLogMile
,EndLogMile
,SegmentLength
,LocationID
,AADT
,RouteNameFull
,DOTDDistrict
,ParishCode
,[YEAR]
,HWYSectionsSK
, SUBSTRING(r.LRSID, 1, 3) AS 'PPP_Parish'
, SUBSTRING(r.LRSID, 4, 1) AS 'X_PrefixCode'
, SUBSTRING(r.LRSID, 5, 6) AS 'NNNNNN_RoadNameCode'
, SUBSTRING(r.LRSID, 11, 3) AS 'TTT_StreetTypeCode'
, SUBSTRING(r.LRSID, 14, 1) AS 'S_SuffixCode'
, SUBSTRING(r.LRSID, 15, 1) AS 'F_FeatureTypeCode'
, SUBSTRING(r.LRSID, 16, 3) AS 'LLL_SequentialOccurance'
FROM dbo.refroadwaydata R
WHERE [YEAR] = 2022
AND LRSID IS NOT NULL
AND ControlSection IS NULL
AND RoadClassificationCode IN ('103', '104')
AND LRSID like '______________[1]___'
),

tb AS (
SELECT *,
CASE 
        WHEN AADT IS NULL
             AND LAG(AADT) OVER (PARTITION BY LRSID, HighwayClassCode ORDER BY BeginLogMile) IS NOT NULL
             AND LAG(EndLogMile) OVER (PARTITION BY LRSID, HighwayClassCode ORDER BY BeginLogMile) = BeginLogMile
        THEN
            (LAG(AADT) OVER (PARTITION BY LRSID, HighwayClassCode ORDER BY BeginLogMile))
        ELSE
            AADT
    END AS ImputedAADT
FROM LocalRoads),

tb1 AS (
SELECT LRSID
,LLL_SequentialOccurance
,BeginLogMile
,EndLogMile
,ImputedAADT as "AADT"
,SegmentLength
,highwayclasscode
,RouteNameFull
,parishcode
,DOTDDistrict
,CASE WHEN ( CASE
WHEN ImputedAADT = 0 THEN 0  ELSE 
Abs(( (Lead(ImputedAADT ,1) OVER (partition BY LRSID ,highwayclasscode ORDER BY [BeginLogMile]) - ImputedAADT) / CONVERT(FLOAT ,ImputedAADT))) END ) < .025 
AND Lead(BeginLogMile ,1) OVER (partition BY LRSID ORDER BY [BeginLogMile]) = EndLogMile 
AND Lead(highwayclasscode ,1) OVER (partition BY LRSID ORDER BY [BeginLogMile]) = highwayclasscode 
THEN 'yes'  ELSE 'no' 
END AS forward
,CASE WHEN ( CASE
WHEN ImputedAADT = 0 THEN 0  ELSE 
Abs(( (Lag(ImputedAADT ,1) OVER (partition BY LRSID ,highwayclasscode ORDER BY [BeginLogMile]) - ImputedAADT) / CONVERT(FLOAT ,ImputedAADT))) END ) < .025 
AND Lag(EndLogMile ,1) OVER (partition BY LRSID ORDER BY [BeginLogMile]) = BeginLogMile 
AND Lag(highwayclasscode ,1) OVER (partition BY LRSID ORDER BY [BeginLogMile]) = highwayclasscode 
THEN 'yes'  ELSE 'no' 
END AS behind
FROM tb
), 

tb2 AS (
SELECT LRSID
,LLL_SequentialOccurance
,BeginLogMile
,EndLogMile
,AADT
,SegmentLength
,highwayclasscode
,RouteNameFull
,parishcode
,DOTDDistrict
,forward
,behind
,CASE WHEN behind = 'no' AND forward = 'yes' THEN 'start'
WHEN behind = 'yes' AND forward = 'no' THEN 'end'  ELSE '' END AS 'segpos'
FROM tb1
),

tb3 AS (
SELECT  *
,grouping = Sum ( CASE WHEN segpos = 'start' THEN 1 WHEN forward = 'no' AND behind = 'no' THEN 1 ELSE 0 END ) OVER(partition BY LRSID ORDER BY BeginLogMile)
FROM tb2
),

grouped AS (
SELECT  Row_number() OVER (ORDER BY LRSID ,MIN(BeginLogMile)) AS id
,LRSID 
,LLL_SequentialOccurance
,MIN(BeginLogMile) AS 'LogmileFrom'
,MAX(EndLogMile) AS 'LogmileTo'
,SUM(SegmentLength) AS 'SectionLength'
,highwayclasscode AS 'HwyClass'
,AVG(AADT) AS 'AvgAADT'
,Concat('[' ,String_agg(CONVERT(NVARCHAR(max) ,Concat('"' ,Replace(RouteNameFull,'"','') ,'"')) ,',') ,']') AS RouteNameFull
,Concat('[' ,String_agg(CONVERT(NVARCHAR(max) ,Concat('"' ,parishcode ,'"')) ,',') ,']')    AS parishcode
,Concat('[' ,String_agg(CONVERT(NVARCHAR(max) ,Concat('"' ,DOTDDistrict ,'"')) ,',') ,']')  AS DOTDDistrict
,COUNT(*) AS 'SubsectionCount'
FROM tb3
GROUP BY LRSID, LLL_SequentialOccurance, RouteNameFull, highwayclasscode, [grouping]
),

distinctroutenames AS
(
SELECT  Concat('[' ,String_agg(CONVERT(NVARCHAR(max) ,Concat('"' ,routenames ,'"')) ,',') ,']') AS RouteNameFull
,id
FROM
(
SELECT  id
,routenames
FROM grouped CROSS apply Openjson
(RouteNameFull , '$'
)
WITH
( routenames nvarchar(150) '$'
) z
GROUP BY  routenames
 ,id
)x
GROUP BY  id
) , distinctparishcodes AS
(
SELECT  concat('[' ,string_agg(CONVERT(nvarchar(max) ,concat('"' ,code ,'"')) ,',') ,']') AS parishcode,
concat('[' ,string_agg(CONVERT(nvarchar(max) ,concat('"' ,ParishDescription ,'"')) ,',') ,']') AS Parish,
id
FROM
(
SELECT  grouped.id
,z.code
,ParishDescription
FROM grouped
CROSS apply openjson (parishcode , '$') WITH (code nvarchar(50) '$') z
inner join RefParish
on z.code = RefParish.ParishCode
GROUP BY  z.code,ParishDescription,grouped.id
)x
GROUP BY  id
) , distinctdotddistricts AS
(
SELECT  concat('[' ,string_agg(CONVERT(nvarchar(max) ,concat('"' ,dotddistricts ,'"')) ,',') ,']') AS DOTDDistrict
,id
FROM
(
SELECT  id
,dotddistricts
FROM grouped CROSS apply openjson
(DOTDDistrict , '$'
)
WITH
( dotddistricts nvarchar(50) '$'
) z
GROUP BY  dotddistricts
 ,id
)x
GROUP BY  id
),

GroupedwithData as (
SELECT  LRSID
,LLL_SequentialOccurance
,hwyclass
,avgaadt
,logmilefrom
,logmileto
,sectionlength
,subsectioncount
,distinctroutenames.RouteNameFull
,distinctparishcodes.parishcode
,distinctparishcodes.Parish
,distinctdotddistricts.DOTDDistrict
FROM grouped , distinctroutenames , distinctparishcodes , distinctdotddistricts
WHERE grouped.id = distinctroutenames.id
AND grouped.id = distinctparishcodes.id
AND grouped.id = distinctdotddistricts.id
)

insert into CollapsedSegments2022([LRSID], [LLL_SequentialOccurance], [HighwayClassCode], 
[AADT], [BeginLogMile], [EndLogMile], [SegmentLength], [SubsectionCount], [RouteNames], [ParishCodes], [Parishes], [DOTDDistricts])
Select * from GroupedwithData;


CREATE TABLE Results (
LRSID nvarchar(225) NOT NULL,
LLL_SequentialOccurance nvarchar(225) NOT NULL,
HighwayClassCode nvarchar(20) NOT NULL,
AADT float NULL,
BeginLogMile float NOT NULL,
EndLogMile float NOT NULL,
SegmentLength float NOT NULL,
RouteNames nvarchar(500) NULL,
Parishes nvarchar(500) NULL,
DOTDDistricts nvarchar(50) NULL,
Fatal float NULL,
Serious float NULL,
Minor float NULL,
Possible float NULL,
PDO float NULL,
TotalCrashes float NULL,
CONSTRAINT PK_Results PRIMARY KEY (LRSID, LLL_SequentialOccurance, HighwayClassCode, BeginLogMile, EndLogMile, SegmentLength)
); 

with crashes as 
(
SELECT CrashSeverityCode, 
CASE WHEN fc.CrashSeverityCode = '100' THEN 1 ELSE 0 END AS 'Fatal',
CASE WHEN fc.CrashSeverityCode = '101' THEN 1 ELSE 0 END AS 'Serious',
CASE WHEN fc.CrashSeverityCode = '102' THEN 1 ELSE 0 END AS 'Minor',
CASE WHEN fc.CrashSeverityCode = '103' THEN 1 ELSE 0 END AS 'Possible',
CASE WHEN fc.CrashSeverityCode = '104' THEN 1 ELSE 0 END AS 'PropertyDamageOnly',
r.HighwayClassCode,
r.LRSID,
HWYSectionsAK, 
fc.LogMile
FROM dbo.FactCrash fc right join RefRoadwayData r on fc.HWYSectionsAK = r.HWYSectionsSK
WHERE fc.DateSK BETWEEN 20180101 AND 20221231
AND fc.IsIntersection = 'NO' 
AND r.LRSID like '______________[12]___'
and fc.LogMile IS not NULL 
)

insert into Results([LRSID],[LLL_SequentialOccurance], [HighwayClassCode], [AADT], [BeginLogMile], 
[EndLogMile], [SegmentLength], [RouteNames], [Parishes], [DOTDDistricts], [Fatal], [Serious], [Minor], [Possible], [PDO], [TotalCrashes])

select a.LRSID,
a.LLL_SequentialOccurance,
a.HighwayClassCode,
a.AADT,
a.BeginLogMile,
a.EndLogMile,
a.SegmentLength,
a.RouteNames,
a.Parishes,
a.DOTDDistricts,
isnull(sum(b.Fatal),0) as 'Fatal',
isnull(sum(b.Serious),0) as 'Serious',
isnull(sum(b.Minor),0) as 'Minor',
isnull(sum(b.Possible),0) as 'Possible',
isnull(sum(b.PropertyDamageOnly),0) as 'PDO',
isnull(sum(b.Fatal) + sum(b.Serious) + sum(b.Minor) + sum(b.Possible) + sum(b.PropertyDamageOnly), 0) as 'TotalCrashes'
from CollapsedSegments2022 a
left join crashes b
on a.LRSID = b.LRSID and b.LogMile between a.BeginLogMile and a.EndLogMile --and a.HighwayClassCode = b.HighwayClassCode
group by a.LRSID, a.LLL_SequentialOccurance, a.BeginLogMile, a.EndLogMile, a.HighwayClassCode, a.AADT, a.RouteNames, a.Parishes, a.DOTDDistricts, a.SegmentLength
order by a.LRSID, a.BeginLogMile;



CREATE TABLE Results2 (
LRSID nvarchar(225) NOT NULL,
LLL_SequentialOccurance nvarchar(225) NOT NULL,
HighwayClassDescription nvarchar(225) NULL,
HighwayClassCode nvarchar(20) NOT NULL,
AADT float NULL,
BeginLogMile float NOT NULL,
EndLogMile float NOT NULL,
SegmentLength float NOT NULL,
RouteNames nvarchar(500) NULL,
Parishes nvarchar(500) NULL,
DOTDDistricts nvarchar(50) NULL,
Fatal float NULL,
Serious float NULL,
Minor float NULL,
Possible float NULL,
PDO float NULL,
TotalCrashes float NULL,
CONSTRAINT PK_Results2 PRIMARY KEY (LRSID, LLL_SequentialOccurance, HighwayClassCode, BeginLogMile, EndLogMile, SegmentLength)
); 

INSERT INTO Results2
SELECT Results.[LRSID]
, Results.[LLL_SequentialOccurance]
, RefHighwayClass.[HighwayClass]
, Results.[HighwayClassCode]
, Results.[AADT]
, Results.[BeginLogMile]
, Results.[EndLogMile]
, Results.[SegmentLength]
, Results.[RouteNames]
, Results.[Parishes]
, Results.[DOTDDistricts]
, Results.[Fatal]
, Results.[Serious]
, Results.[Minor]
, Results.[Possible]
, Results.[PDO]
, Results.[TotalCrashes]
FROM Results LEFT JOIN [dbo].[RefHighwayClass]
ON results.HighwayClassCode = [dbo].[RefHighwayClass].HighwayClassCode;
'''.replace('\n', ' ').replace('\t', ' ')

# Query 2
QueryStr = '''
select * from results2;
'''.replace('\n',' ').replace('\t', ' ')

# Execution of Queries and Preliminary Dropping of Pre-Existing Tables
cursor.execute('drop table CollapsedSegments2022 if exists;')
cursor.execute('drop table Results if exists;')
cursor.execute('drop table Results2 if exists;')
cursor.execute(NSstring)
qr = cursor.execute(QueryStr)

# Reformat Output into a DataFrame
cols = []
for i,_ in enumerate(qr.description):
    cols.append(qr.description[i][0])
rows = qr.fetchall()
DB_output = pd.DataFrame(np.array(rows), columns=cols)

# Drop all Tables and Close out the Cursor (very important to close out or else MS will be affected)
cursor.execute('alter table CollapsedSegments2022 drop CONSTRAINT PK_CollapsedSegments2022;')
cursor.execute('drop table CollapsedSegments2022;')
cursor.execute('alter table Results drop CONSTRAINT PK_Results;')
cursor.execute('drop table Results;')
cursor.execute('alter table Results2 drop CONSTRAINT PK_Results2;')
cursor.execute('drop table Results2;')
cursor.commit()
cursor.close()

# View Collapsed Segments
DB_output

# for the time being, manually upload collapsed segment data
# DB_output = pd.read_excel('segments_1025.xlsx')

DB_output.info()

df = DB_output.copy()

# Changing Data Types
# int
df = df.astype(dict.fromkeys(df.columns[11:-1], int))

#float
df['BeginLogMile'] = df['BeginLogMile'].astype(float)
df['EndLogMile'] = df['EndLogMile'].astype(float)
df['SegmentLength'] = df['SegmentLength'].astype(float)
df['AADT'] = df['AADT'].astype(float)

#string
df['LRSID'] = df['LRSID'].astype(str)
df['LLL_SequentialOccurance'] = df['LLL_SequentialOccurance'].astype(str)
df['HighwayClassCode'] = df['HighwayClassCode'].astype(str)
df['Parishes'] = df['Parishes'].astype(str)
df['DOTDDistricts'] = df['DOTDDistricts'].astype(str)

# rename the highway class field for joining later
# change data type to string
df.rename(columns={'HighwayClassCode': 'HwyClass'}, inplace=True)
df['HwyClass'] = df['HwyClass'].astype(str)

# import SPF values
HC = pd.read_excel("def_Hwy_Class_CD_2023.xlsx")

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
hpsi_list.to_excel("All_HPSI_TESTINGGG.xlsx")

# Prining PSI List
# first order the dataframe by 'HwyClss'
all_psi_list = r.copy()
all_psi_list = all_psi_list.sort_values(by=['HwyClass'], ascending=[True])
# all_psi_list.to_excel("All_PSI_2022_updated.xlsx")