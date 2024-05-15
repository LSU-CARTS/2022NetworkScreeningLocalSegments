WITH A AS (
SELECT *
FROM [2022NetworkScreeningLocalSegments]
), 

B AS (
SELECT HIGHWAYCLASS,
HIGHWAYCLASSCODE,
CASE 
	WHEN HighwayClass LIKE '%DIVIDED%' THEN '[1,2]'
--	WHEN HighwayClass LIKE '%INTERSTATE%' THEN '[1,2]'
	ELSE '[1]'
END AS 'DIRECTION'
from RefHighwayClass
) 

SELECT A.*, B.DIRECTION
FROM A JOIN B ON A.HighwayClassCode = B.HighwayClassCode;
