-- query 1
CREATE VIEW Task1 AS 
SELECT To_Char("date", 'Day') AS shiftday, location FROM vaccinations 
WHERE date='2021-05-10'::date;

SELECT staffmembers.ssno, name, phone, role, vaccinationstatus, hospital 
FROM staffmembers JOIN shifts ON staffmembers.ssno=shifts.ssno 
WHERE shiftday IN (SELECT regexp_replace(shiftday, '\s+$', '') FROM Task1) 
AND station IN (SELECT location FROM Task1);

-- query 2
SELECT DISTINCT staffmembers.ssno, name 
FROM staffmembers JOIN shifts ON staffmembers.ssno=shifts.ssno 
WHERE staffmembers.role='doctor' AND shifts.shiftday='Wednesday' 
AND staffmembers.hospital IN (
    SELECT name 
    FROM vaccinationstations 
    WHERE address LIKE '%%HELSINKI%%'
);

-- query 3_1
SELECT vaccinebatch.batchID, vaccinebatch.location AS currentlocation, transportationlog.arrival AS lastlocation 
FROM vaccinebatch FULL OUTER JOIN (
    transportationlog JOIN (
        SELECT batchid, MAX(datearr) 
        FROM transportationlog 
        GROUP BY(batchid)
    ) 
    AS lastlocation 
    ON (transportationlog.batchid=lastlocation.batchid AND transportationlog.datearr=lastlocation.max)
) 
ON (vaccinebatch.batchid=transportationlog.batchid);

-- query 3_2
SELECT vaccinebatch.batchID, transportationlog.arrival AS lastlocation, phone 
FROM vaccinebatch FULL OUTER JOIN (
    transportationlog JOIN (
        SELECT batchid, MAX(datearr) 
        FROM transportationlog 
        GROUP BY(batchid)
    ) 
    AS lastlocation 
    ON (transportationlog.batchid=lastlocation.batchid AND transportationlog.datearr=lastlocation.max)
) 
ON (vaccinebatch.batchid=transportationlog.batchid) 
JOIN vaccinationstations 
ON (transportationlog.arrival=vaccinationstations.name) 
WHERE vaccinebatch.location!=transportationlog.arrival;

-- query 4
SELECT ssno, batchid, type, vaccinations.date, vaccinations.location 
FROM vaccinations 
JOIN vaccinebatch USING (batchid) 
JOIN vaccinepatients ON (vaccinepatients.date=vaccinations.date AND vaccinepatients.location=vaccinations.location) 
JOIN diagnosis ON(diagnosis.patient=vaccinepatients.ssno) 
WHERE symptom IN (SELECT name FROM symptoms WHERE criticality=1) AND diagnosis.date>'2021-05-10;'

-- query 5
CREATE VIEW Task5 AS
SELECT *, CASE WHEN EXISTS (
    SELECT COUNT(date), ssno 
    FROM vaccinepatients 
    WHERE vaccinepatients.ssno=patients.ssno 
    GROUP BY(ssno) HAVING COUNT(date)=2
) 
THEN CAST(1 AS INT) ELSE CAST(0 AS INT) END AS vaccinestatus FROM patients;

SELECT * FROM Task5;

-- query 6
SELECT * FROM (
    SELECT SUM(amount) AS v01, location FROM vaccinebatch WHERE type='V01' GROUP BY(location)
) AS v1 
FULL OUTER JOIN (
    SELECT SUM(amount) AS v02, location FROM vaccinebatch WHERE type='V02' GROUP BY(location)
) AS v2 USING(location) 
FULL OUTER JOIN (
    SELECT SUM(amount) AS v03, location FROM vaccinebatch WHERE type='V03' GROUP BY(location)
) AS v3 USING(location) 
FULL OUTER JOIN (
    SELECT SUM(amount) AS totalvaccines, location FROM vaccinebatch GROUP BY(location)
) AS sum USING(location);

-- query 7
CREATE VIEW onedose AS 
SELECT DISTINCT(ssno), t1.date, t1.location, type, symptom, diagnosis.date AS diagnosisdate 
FROM vaccinepatients AS t1 
JOIN vaccinepatients AS t2 USING(ssno) 
FULL OUTER JOIN diagnosis ON(ssno=patient) 
JOIN vaccinations ON(t1.date=vaccinations.date AND t1.location=vaccinations.location) 
JOIN vaccinebatch USING(batchid) 
WHERE t1.date=t2.date AND (diagnosis.date>=t1.date OR diagnosis.date IS NULL);
                
CREATE VIEW twodosefirst AS 
SELECT DISTINCT(ssno), t1.date, t1.location, type, symptom, diagnosis.date AS diagnosisdate 
FROM vaccinepatients AS t1 
JOIN vaccinepatients AS t2 USING(ssno) 
FULL OUTER JOIN diagnosis ON(ssno=patient) 
JOIN vaccinations ON(t1.date=vaccinations.date AND t1.location=vaccinations.location)
JOIN vaccinebatch USING(batchid) 
WHERE t1.date<t2.date AND (diagnosis.date>=t1.date OR diagnosis.date IS NULL);
                
CREATE VIEW twodosesecond AS 
SELECT DISTINCT(ssno), t2.date, t2.location, type, symptom, diagnosis.date AS diagnosisdate 
FROM vaccinepatients AS t1 JOIN vaccinepatients AS t2 USING(ssno) 
FULL OUTER JOIN diagnosis ON(ssno=patient) 
JOIN vaccinations ON(t2.date=vaccinations.date AND t2.location=vaccinations.location) 
JOIN vaccinebatch USING(batchid) 
WHERE t1.date<t2.date AND (diagnosis.date>=t2.date OR diagnosis.date IS NULL);
                
CREATE VIEW final AS 
SELECT DISTINCT ssno, type, symptom 
FROM (
    SELECT * FROM onedose AS p1 
    UNION 
    SELECT * FROM twodosefirst AS p2 UNION SELECT * FROM twodosesecond AS p3
) AS p;
                    
CREATE VIEW v1 AS SELECT * FROM final WHERE type='V01';

CREATE VIEW v2 AS SELECT * FROM final WHERE type='V02';
                
CREATE VIEW v3 AS SELECT * FROM final WHERE type='V03';
                
SELECT * 
FROM (
    SELECT symptom, COUNT(symptom) AS v1count, ROUND(COUNT(symptom)*1.0/(SELECT COUNT(DISTINCT ssno) FROM v1),2) AS v1frequency 
    FROM v1 WHERE symptom IS NOT NULL 
    GROUP BY(symptom)
) AS v1 
FULL OUTER JOIN (
    SELECT symptom, COUNT(symptom) AS v2count, ROUND(COUNT(symptom)*1.0/(SELECT COUNT(DISTINCT ssno) FROM v2),2) AS v2frequency 
    FROM v2 WHERE symptom IS NOT NULL 
    GROUP BY(symptom)
) AS v2 USING(symptom) 
FULL OUTER JOIN (
    SELECT symptom, COUNT(symptom) AS v3count, ROUND(COUNT(symptom)*1.0/(SELECT COUNT(DISTINCT ssno) FROM v3),2) AS v3frequency 
    FROM v3 WHERE symptom IS NOT NULL 
    GROUP BY(symptom)
) AS v3 USING(symptom);
