WITH A AS (
SELECT *
FROM [Results2]
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
),

C AS (
SELECT P.ParishDescription AS 'OwnershipParish', 
P.FIPS_Code
FROM RefParish P
)

SELECT A.*, B.DIRECTION AS 'Direction', C.OwnershipParish
FROM A JOIN B ON A.HighwayClassCode = B.HighwayClassCode
	LEFT JOIN C ON A.LRSIDParishCode = C.FIPS_Code;
