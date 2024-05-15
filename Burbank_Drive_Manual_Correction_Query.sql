-- MANUAL CALCULATION FOR BURBANK DRIVE
-- BASEMAP ERROR
-- MUST MANUALLY CORRECT THIS RECORD EITHER IN 
-- SQL TABLE or PYTHON

WITH A AS (
SELECT LRSID,
	SUM(CASE 
		WHEN FC.CRASHSEVERITYCODE = '100' THEN 1 ELSE 0 END) AS 'FATAL',
	SUM(CASE
		WHEN FC.CrashSeverityCode = '101' THEN 1 ELSE 0 END) AS 'SERIOUS',
	SUM(CASE 
		WHEN FC.CrashSeverityCode = '102' THEN 1 ELSE 0 END) AS 'MINOR',
	SUM(CASE 
		WHEN FC.CrashSeverityCode = '103' THEN 1 ELSE 0 END) AS 'POSSIBLE', 
	SUM(CASE 
		WHEN FC.CrashSeverityCode = '104' THEN 1 ELSE 0 END) AS 'PDO'
FROM FactCrash FC JOIN RefRoadwayData R ON FC.HWYSectionsAK = R.HWYSectionsSK
WHERE FC.CrashYear BETWEEN 2018 AND 2022 
AND R.LRSID = '033900744101591001'
AND FC.LogMile BETWEEN '0' AND '0.795'
AND R.ControlSection IS NULL 
AND FC.IsIntersection = 'NO'
GROUP BY R.LRSID
)

UPDATE [dbo].[Results2]
SET [dbo].[Results2].[Fatal] = A.FATAL,
	[dbo].[Results2].[Serious] = A.SERIOUS,
	[dbo].[Results2].[Minor] = A.MINOR,
	[dbo].[Results2].[Possible] = A.POSSIBLE,
	[dbo].[Results2].[PDO] = A.PDO,
	[dbo].[Results2].[TotalCrashes] = 
	 A.[FATAL] + A.[SERIOUS] + A.[MINOR] + A.[POSSIBLE] + A.[PDO]
FROM A
INNER JOIN [dbo].[Results2]
ON A.LRSID = [dbo].[Results2].[LRSID] 
WHERE [dbo].[Results2].[BeginLogMile] = 0
AND [dbo].[Results2].[EndLogMile] = 0.795;

--CHECKING TO ENSURE RECORD WAS PROPERLY UPDATED
select * 
from [Results2] 
where LRSID = '033900744101591001';