drop table if exists CollapsedLocalSegments2022;
drop table if exists Results;
drop table if exists Results2;
drop table if exists #localRoads;
--delete from CollapsedLocalSegments2022;

CREATE TABLE CollapsedLocalSegments2022(
	LRSID nvarchar(225) NOT NULL,
	LRSIDParishCode nvarchar(50) NOT NULL,
	HighwayClassCode nvarchar(20) NOT NULL,
	AADT float NULL,
    BeginLogMile float NOT NULL,
    EndLogMile float NOT NULL,
    SegmentLength float NOT NULL,
	SubsectionCount int NULL,
    RouteNames nvarchar(500) NULL,
	ParishCodes nvarchar (50) NULL,
    Parishes nvarchar(500) NULL,
	DOTDDistricts nvarchar(50) NULL,
	--CONSTRAINT PK_Results PRIMARY KEY (LRSID, LRSIDParishCode, HighwayClassCode, BeginLogMile, EndLogMile, SegmentLength)
); 


	SELECT  LRSID
	,HighwayClassCode
	       ,BeginLogMile
	       ,EndLogMile
	       ,SegmentLength
	       ,LocationID
	       ,AADT
	       ,RouteNameFull
	       ,DOTDDistrict
	       ,R.ParishCode
		   ,ParishDescription
	       ,[YEAR]
	       ,HWYSectionsSK
		   , SUBSTRING(r.LRSID, 1, 3) AS 'LRSIDParishCode'
			into #localRoads
	FROM dbo.refroadwaydata R
	inner join (select * from openquery([Prod-SQLDW],'select ParishCode, ParishDescription from [EcrashDW].dbo.RefParish')) as RefParish
		on R.ParishCode = RefParish.ParishCode
	WHERE [YEAR] = 2022
	AND LRSID IS NOT NULL
	AND ControlSection IS NULL 
	AND RoadClassificationCode IN ('103', '104')
	AND LRSID like '______________[1]___';

with tb AS (
SELECT *,
CASE 
        WHEN AADT IS NULL
             AND LAG(AADT) OVER (PARTITION BY LRSID, HighwayClassCode ORDER BY BeginLogMile, endlogmile) IS NOT NULL
             AND LAG(EndLogMile) OVER (PARTITION BY LRSID, HighwayClassCode ORDER BY BeginLogMile, endlogmile) = BeginLogMile
        THEN
            (Lag(AADT) OVER (PARTITION BY LRSID, HighwayClassCode ORDER BY BeginLogMile, endlogmile))
		WHEN AADT IS NULL
             AND LEAD(AADT) OVER (PARTITION BY LRSID, HighwayClassCode ORDER BY BeginLogMile, endlogmile) IS NOT NULL
             AND LEAD(beginlogMile) OVER (PARTITION BY LRSID, HighwayClassCode ORDER BY BeginLogMile, endlogmile) = endlogmile
        THEN
            (lead(AADT) OVER (PARTITION BY LRSID, HighwayClassCode ORDER BY BeginLogMile, endlogmile))
        ELSE
            AADT
    END AS ImputedAADT
FROM #LocalRoads),

tb1 AS (
SELECT LRSID
,LRSIDParishCode
,BeginLogMile
,EndLogMile
,ImputedAADT as "AADT"
,SegmentLength
,highwayclasscode
,RouteNameFull
,parishcode
,ParishDescription
,DOTDDistrict
,CASE WHEN ( CASE
	WHEN isnull(nullif(ImputedAADT,0),0) = 0 THEN 0  ELSE 
	Abs(( (Lead(ImputedAADT ,1) OVER (partition BY LRSID ,highwayclasscode ORDER BY [BeginLogMile], endLogmile) - ImputedAADT) / CONVERT(FLOAT ,ImputedAADT))) END ) < .050 
	AND Lead(BeginLogMile ,1) OVER (partition BY LRSID ORDER BY [BeginLogMile], endLogmile) = EndLogMile 
	AND Lead(highwayclasscode ,1) OVER (partition BY LRSID ORDER BY [BeginLogMile], endLogmile) = highwayclasscode 
	THEN 'yes'  ELSE 'no' 
	END AS forward
,CASE WHEN ( CASE
	WHEN isnull(nullif(ImputedAADT,0),0) = 0 THEN 0  ELSE 
	Abs(( (Lag(ImputedAADT ,1) OVER (partition BY LRSID ,highwayclasscode ORDER BY [BeginLogMile], endLogmile) - ImputedAADT) / CONVERT(FLOAT ,ImputedAADT))) END ) < .050 
	AND Lag(EndLogMile ,1) OVER (partition BY LRSID ORDER BY [BeginLogMile], endLogmile) = BeginLogMile 
	AND Lag(highwayclasscode ,1) OVER (partition BY LRSID ORDER BY [BeginLogMile], endLogmile) = highwayclasscode 
	THEN 'yes'  ELSE 'no' 
	END AS behind
FROM tb
), 

tb2 AS (
SELECT LRSID
,LRSIDParishCode
,BeginLogMile
,EndLogMile
,AADT
,SegmentLength
,highwayclasscode
,RouteNameFull
,parishcode
,ParishDescription
,DOTDDistrict
,forward
,behind
,CASE WHEN behind = 'no' AND forward = 'yes' THEN 'start'
	    WHEN behind = 'yes' AND forward = 'no' THEN 'end'  ELSE '' END AS 'segpos'
FROM tb1
),

tb3 AS (
SELECT  *
,grouping = Sum ( CASE WHEN segpos = 'start' THEN 1 WHEN forward = 'no' AND behind = 'no' THEN 1 ELSE 0 END ) OVER(partition BY LRSID ORDER BY BeginLogMile, endlogmile)
FROM tb2
),

grouped as (
SELECT  Row_number() OVER (ORDER BY LRSID ,MIN(BeginLogMile)) AS id
,LRSID 
,LRSIDParishCode
,MIN(BeginLogMile) AS 'LogmileFrom'
,MAX(EndLogMile) AS 'LogmileTo'
,SUM(SegmentLength) AS 'SectionLength'
,highwayclasscode AS 'HwyClass'
,AVG(AADT) AS 'AvgAADT'
,Concat('[' ,String_agg(CONVERT(NVARCHAR(max) ,Concat('"' ,Replace(RouteNameFull,'"','') ,'"')) ,',') ,']') AS RouteNameFull
,Concat('[' ,String_agg(CONVERT(NVARCHAR(max) ,Concat('"' ,parishcode ,'"')) ,',') ,']')    AS parishcode
,Concat('[' ,String_agg(CONVERT(NVARCHAR(max) ,Concat('"' ,ParishDescription ,'"')) ,',') ,']')    AS ParishDescription
,Concat('[' ,String_agg(CONVERT(NVARCHAR(max) ,Concat('"' ,DOTDDistrict ,'"')) ,',') ,']')  AS DOTDDistrict
,COUNT(*) AS 'SubsectionCount'
FROM tb3
GROUP BY LRSID, LRSIDParishCode, highwayclasscode, [grouping]
),

groupedDistinct as (
	select * from grouped
	CROSS APPLY (
		SELECT DISTINCT Code
		FROM OPENJSON(ParishCode) WITH (
			Code varchar(50) '$'
		)
		FOR JSON PATH
	)x (ParishCodes)
	CROSS APPLY (
		SELECT DISTINCT Code
		FROM OPENJSON(DOTDDistrict) WITH (
			Code varchar(50) '$'
		)
		FOR JSON PATH
	)y (Districts)
	CROSS APPLY (
		SELECT DISTINCT Name
		FROM OPENJSON(RouteNameFull) WITH (
			Name varchar(50) '$'
		)
		FOR JSON PATH
	)z (RouteNames)
	CROSS APPLY (
		SELECT DISTINCT Name
		FROM OPENJSON(ParishDescription) WITH (
			Name varchar(50) '$'
		)
		FOR JSON PATH
	)b (ParishDescriptions)
),

groupedwithData as  (
SELECT  LRSID
		,LRSIDParishCode
		,hwyclass
       ,avgaadt
       ,logmilefrom
       ,logmileto
       ,sectionlength
       ,subsectioncount
	   ,REPLACE(REPLACE(RouteNames,'{"Name":','' ),'"}','"' ) as RouteNames
		,REPLACE(REPLACE(ParishCodes,'{"Code":','' ),'"}','"' ) as ParishCodes
		,REPLACE(REPLACE(ParishDescriptions,'{"Name":','' ),'"}','"' ) as Parishes
		,REPLACE(REPLACE(Districts,'{"Code":','' ),'"}','"' ) as DOTDDistricts
FROM groupedDistinct
)

insert into CollapsedLocalSegments2022 ([LRSID], [LRSIDParishCode], [HighwayClassCode], [AADT], [BeginLogMile], [EndLogMile], 
[SegmentLength], [SubsectionCount], [RouteNames], [ParishCodes], [Parishes], [DOTDDistricts])
/*([LRSID], [LRSIDParishCode], [HighwayClassCode], 
[AADT], [BeginLogMile], [EndLogMile], [SegmentLength], [SubsectionCount], [RouteNames], [ParishCodes], [Parishes], [DOTDDistricts])*/
Select * from GroupedwithData;

--select * from [Prod-SQLDW].[EcrashDW].dbo.refroadwaydata
--where lrsid = '001100378600191010'


--checking the district combinations
with collapsedinQuestion as (
select * 
from CollapsedLocalSegments2022
CROSS APPLY OPENJSON(DOTDDistricts, '$') WITH (
   District nvarchar(50) '$'
) z
)

select * from collapsedinQuestion x
where not exists (
select * from [Prod-SQLDW].[EcrashDW].dbo.refroadwaydata y
where x.LRSID = y.lrsid and x.District = y.DOTDDistrict);

--checking the parishcode combinations
with collapsedinQuestion as (
select * 
from CollapsedLocalSegments2022
CROSS APPLY OPENJSON(ParishCodes, '$') WITH (
   code nvarchar(50) '$'
) z
)

select * from collapsedinQuestion x
where not exists (
select * from [Prod-SQLDW].[EcrashDW].dbo.refroadwaydata y
where x.LRSID = y.lrsid and x.code = y.ParishCode);





CREATE TABLE Results (
	ID int IDentity(1,1) Primary Key,
	LRSID nvarchar(225) NOT NULL,
	LRSIDParishCode nvarchar(225) NOT NULL,
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
	TotalCrashes float NULL
	--CONSTRAINT PK_Results PRIMARY KEY (LRSID, LRSIDParishCode, HighwayClassCode, BeginLogMile, EndLogMile, SegmentLength)
); 

with crashes as 
(
SELECT 
statecasenumber,
CrashSeverityCode, 
CASE WHEN fc.CrashSeverityCode = '100' THEN 1 ELSE 0 END AS 'Fatal',
CASE WHEN fc.CrashSeverityCode = '101' THEN 1 ELSE 0 END AS 'Serious',
CASE WHEN fc.CrashSeverityCode = '102' THEN 1 ELSE 0 END AS 'Minor',
CASE WHEN fc.CrashSeverityCode = '103' THEN 1 ELSE 0 END AS 'Possible',
CASE WHEN fc.CrashSeverityCode = '104' THEN 1 ELSE 0 END AS 'PropertyDamageOnly',
r.HighwayClassCode,
r.LRSID,
HWYSectionsAK, 
fc.LogMile
FROM (select * from OpenQuery([Prod-SQLDW], 'select statecaseNumber, crashSeverityCode, HWYSectionsAK, LogMile from [EcrashDW].dbo.FactCrash
WHERE DateSK BETWEEN 20180101 AND 20221231
AND IsIntersection = ''NO'' 
and LogMile IS not NULL ')) as fc 
right join [Prod-SQLDW].[EcrashDW].dbo.RefRoadwayData r on fc.HWYSectionsAK = r.HWYSectionsSK
where r.LRSID like '______________[12]___'
)

insert into Results([LRSID],[LRSIDParishCode], [HighwayClassCode], [AADT], [BeginLogMile], 
[EndLogMile], [SegmentLength], [RouteNames], [Parishes], [DOTDDistricts], [Fatal], [Serious], [Minor], [Possible], [PDO], [TotalCrashes])

select a.LRSID,
a.LRSIDParishCode,
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
from CollapsedLocalSegments2022 a
left join crashes b
on STUFF(a.LRSID, 15,1,'_') = STUFF(b.LRSID, 15,1,'_') and b.LogMile between a.BeginLogMile and a.EndLogMile --and a.HighwayClassCode = b.HighwayClassCode
group by a.LRSID, a.LRSIDParishCode, a.BeginLogMile, a.EndLogMile, a.HighwayClassCode, a.AADT, a.RouteNames, a.Parishes, a.DOTDDistricts, a.SegmentLength
order by a.LRSID, a.BeginLogMile;



CREATE TABLE Results2 (
	ID int IDentity(1,1) Primary Key,
    LRSID nvarchar(225) NOT NULL,
	LRSIDParishCode nvarchar(225) NOT NULL,
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
	TotalCrashes float NULL
	--CONSTRAINT PK_Results2 PRIMARY KEY (LRSID, LRSIDParishCode, HighwayClassCode, BeginLogMile, EndLogMile, SegmentLength)
); 

INSERT INTO Results2
SELECT Results.[LRSID]
, Results.[LRSIDParishCode]
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
FROM Results LEFT JOIN [Prod-SQLDW].[EcrashDW].[dbo].[RefHighwayClass]
ON results.HighwayClassCode = [RefHighwayClass].HighwayClassCode;

select Results2.*, 
RefParish.ParishDescription AS 'ParishOwnership',
RefParish.District_DOTD AS 'DOTDDistrictOwnership'
from results2 join RefParish on results2.LRSIDParishCode = RefParish.FIPS_Code
