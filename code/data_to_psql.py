import psycopg2
from psycopg2 import Error
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from pathlib import Path


def main():
    DATADIR = str(Path(__file__).parent.parent) # for relative path 
    print(DATADIR)
    database="grp5_vaccinedist"   
    user='grp05'       
    password='cXdyKBK6' 
    host='dbcourse2022.cs.aalto.fi'
    # use connect function to establish the connection
    try:
        # Connect the postgres database from your local machine using psycopg2
        connection = psycopg2.connect(
                                        database=database,              
                                        user=user,       
                                        password=password,   
                                        host=host
                                    )
        connection.autocommit = True

        # Create a cursor to perform database operations
        cursor = connection.cursor()
        # Print PostgreSQL details
        print("PostgreSQL server information")
        print(connection.get_dsn_parameters(), "\n")
        # Executing a SQL query
        cursor.execute("SELECT version();")
        # Fetch result
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")


        # Step 1: COnnect to db using SQLAlchemy create_engine 
        
        DIALECT = 'postgresql+psycopg2://'
        database ='grp5_vaccinedist'
        db_uri = "%s:%s@%s/%s" % (user, password, host, database)
        print(DIALECT+db_uri)
        engine = create_engine(DIALECT + db_uri)
        sql_file1  = open(DATADIR + '/code/create_tables.sql')
        psql_conn  = engine.connect()


        # Step 2 (Option 2): CREATE TABLE engine connection & fill in tables with Pandas Dataframe to_sql
        
        print ("\n\nUsing pandas dataframe to read sql queries and fill table")
        psql_conn.execute(
            'DROP TABLE IF EXISTS manufacturer CASCADE;'
        )
        psql_conn.execute(
            'DROP TABLE IF EXISTS vaccinetype CASCADE;'
        )
        psql_conn.execute(
            'DROP TABLE IF EXISTS vaccinebatch CASCADE;'
        )
        psql_conn.execute(
            'DROP TABLE IF EXISTS vaccinationstations CASCADE;'
        )
        psql_conn.execute(
            'DROP TABLE IF EXISTS transportationlog CASCADE;'
        )
        psql_conn.execute(
            'DROP TABLE IF EXISTS staffmembers CASCADE;'
        )
        psql_conn.execute(
            'DROP TABLE IF EXISTS shifts CASCADE;'
        )
        psql_conn.execute(
            'DROP TABLE IF EXISTS vaccinations CASCADE;'
        )
        psql_conn.execute(
            'DROP TABLE IF EXISTS patients CASCADE;'
        )
        psql_conn.execute(
            'DROP TABLE IF EXISTS vaccinepatients CASCADE;'
        )
        psql_conn.execute(
            'DROP TABLE IF EXISTS symptoms CASCADE;'
        )
        psql_conn.execute(
            'DROP TABLE IF EXISTS diagnosis CASCADE;'
        )
        
        
        psql_conn.execute(
            'CREATE TABLE vaccinetype ('
            'id VARCHAR(10) NOT NULL,'
            'name VARCHAR(50) NOT NULL,'
            'doses INT NOT NULL CHECK (doses IN (1,2)),'
            'tempmin INT NOT NULL,'
            'tempmax INT NOT NULL,'
            'PRIMARY KEY (id)'
            ');'
        )

        psql_conn.execute(
            'CREATE TABLE IF NOT EXISTS manufacturer ('
            'id VARCHAR(10) NOT NULL,'
            'country VARCHAR(100) NOT NULL,'
            'phone VARCHAR(20) NOT NULL,'
            'vaccine VARCHAR(10) NOT NULL,'
            'PRIMARY KEY (id),'
            'FOREIGN KEY (vaccine) REFERENCES vaccinetype(id)'
            ');'
        )

        psql_conn.execute(
            'CREATE TABLE vaccinationstations ('
            'name VARCHAR(100) NOT NULL,'
            'address VARCHAR(200) NOT NULL,'
            'phone VARCHAR(20) NOT NULL,'
            'PRIMARY KEY (name)'
            ');'
        )

        psql_conn.execute(
            'CREATE TABLE vaccinebatch ('
            'batchid VARCHAR(10) NOT NULL,'
            'amount INT NOT NULL,'
            'type VARCHAR(10) NOT NULL,'
            'manufacturer VARCHAR(10) NOT NULL,'
            'manufdate DATE NOT NULL,'
            'expiration DATE NOT NULL,'
            'location VARCHAR(100) NOT NULL,'
            'PRIMARY KEY (batchid),'
            'FOREIGN KEY (type) REFERENCES vaccinetype(id),'
            'FOREIGN KEY (manufacturer) REFERENCES manufacturer(id),'
            'FOREIGN KEY (location) REFERENCES vaccinationstations(name)'
            ');'
        )

        psql_conn.execute(
            'CREATE TABLE transportationlog ('
            'batchid VARCHAR(10) NOT NULL,'
            'arrival VARCHAR(100) NOT NULL,'
            'departure VARCHAR(100) NOT NULL,'
            'datearr DATE NOT NULL,'
            'datedep DATE NOT NULL,'
            'PRIMARY KEY (batchid, datearr),'
            'FOREIGN KEY (arrival) REFERENCES vaccinationstations(name),'
            'FOREIGN KEY (departure) REFERENCES vaccinationstations(name)'
            ');'
        )

        psql_conn.execute(
            'CREATE TABLE staffmembers ('
            'ssno VARCHAR(20) NOT NULL,'
            'name VARCHAR(50) NOT NULL,'
            'dateofbirth DATE NOT NULL,'
            'phone VARCHAR(20) NOT NULL,'
            'role VARCHAR(20) NOT NULL,'
            'vaccinationstatus INT NOT NULL CHECK (vaccinationstatus IN (0,1)),'
            'hospital VARCHAR(100) NOT NULL,'
            'PRIMARY KEY (ssno),'
            'FOREIGN KEY (hospital) REFERENCES vaccinationstations(name)'
            ');'
        )

        psql_conn.execute(
            'CREATE TABLE shifts ('
            'station VARCHAR(100) NOT NULL,'
            'shiftday VARCHAR(20) NOT NULL,'
            'ssno VARCHAR(20) NOT NULL,'
            'PRIMARY KEY (station, shiftday, ssno),'
            'FOREIGN KEY (station) REFERENCES vaccinationstations(name)'
            ');'
        )

        psql_conn.execute(
            'CREATE TABLE vaccinations ('
            'date DATE NOT NULL,'
            'location VARCHAR(100) NOT NULL,'
            'batchid  VARCHAR(10) NOT NULL,'
            'PRIMARY KEY (date, location),'
            'FOREIGN KEY (location) REFERENCES vaccinationstations(name),'
            'FOREIGN KEY (batchid) REFERENCES vaccinebatch(batchid)'
            ');'
        )

        psql_conn.execute(
            'CREATE TABLE patients ('
            'ssno VARCHAR(20) NOT NULL,'
            'name VARCHAR(100) NOT NULL,'
            'dateofbirth  DATE  NOT NULL,'
            'gender VARCHAR(10) NOT NULL,'
            'PRIMARY KEY (ssno)'
            ');'
        )

        psql_conn.execute(
            'CREATE TABLE vaccinepatients ('
            'date DATE NOT NULL,'
            'location VARCHAR(100) NOT NULL,'
            'ssno VARCHAR(20) NOT NULL,'
            'PRIMARY KEY (ssno, date),'
            'FOREIGN KEY (location, date) REFERENCES vaccinations(location,date)'
            ');'
        )

        psql_conn.execute(
            'CREATE TABLE symptoms ('
            'name VARCHAR(100) NOT NULL,'
            'criticality INT NOT NULL CHECK (criticality IN (0,1)),'
            'PRIMARY KEY (name)'
            ');'
        )

        psql_conn.execute(
            'CREATE TABLE diagnosis ('
            'patient VARCHAR(20) NOT NULL,'
            'symptom VARCHAR(100) NOT NULL,'
            'date DATE NOT NULL,'
            'PRIMARY KEY (patient, symptom, date),'
            'FOREIGN KEY (symptom) REFERENCES symptoms(name),'
            'FOREIGN KEY (patient) REFERENCES patients(ssno)'
            ');'
        )
  
        # Step 2-1: read excel

        df1 = pd.read_excel('data/vaccinetype.xlsx')
        df2 = pd.read_excel('data/manufacturer.xlsx')
        df3 = pd.read_excel('data/vaccinebatch.xlsx')
        df4 = pd.read_excel('data/vaccinationstations.xlsx')
        df5 = pd.read_excel('data/transportationlog.xlsx')
        df6 = pd.read_excel('data/staffmembers.xlsx')
        df7 = pd.read_excel('data/shifts.xlsx')
        df8 = pd.read_excel('data/vaccinations.xlsx')
        df9 = pd.read_excel('data/patients.xlsx')
        df10 = pd.read_excel('data/vaccinepatients.xlsx')
        df11 = pd.read_excel('data/symptoms.xlsx')
        df12 = pd.read_excel('data/diagnosis.xlsx')


        # Step 2-2: the dataframe df is written into an SQL tables

        df1.to_sql('vaccinetype', con=psql_conn, if_exists='append', index=False)
        df2.to_sql('manufacturer', con=psql_conn, if_exists='append', index=False)
        df4.to_sql('vaccinationstations', con=psql_conn, if_exists='append', index=False)
        df3.to_sql('vaccinebatch', con=psql_conn, if_exists='append', index=False)
        df5.to_sql('transportationlog', con=psql_conn, if_exists='append', index=False)
        df6.to_sql('staffmembers', con=psql_conn, if_exists='append', index=False)
        df7.to_sql('shifts', con=psql_conn, if_exists='append', index=False)
        df8.to_sql('vaccinations', con=psql_conn, if_exists='append', index=False)
        df9.to_sql('patients', con=psql_conn, if_exists='append', index=False)
        df10.to_sql('vaccinepatients', con=psql_conn, if_exists='append', index=False)
        df11.to_sql('symptoms', con=psql_conn, if_exists='append', index=False)
        df12.to_sql('diagnosis', con=psql_conn, if_exists='append', index=False)

        # Step 2-3: writing queries

        sql1 = """
              CREATE VIEW Task1 AS 
              SELECT To_Char("date", 'Day') AS shiftday, location FROM vaccinations 
              WHERE date='2021-05-10'::date;

              SELECT staffmembers.ssno, name, phone, role, vaccinationstatus, hospital 
              FROM staffmembers JOIN shifts ON staffmembers.ssno=shifts.ssno 
              WHERE shiftday IN (SELECT regexp_replace(shiftday, '\s+$', '') FROM Task1) 
              AND station IN (SELECT location FROM Task1);
              """
        test1 = pd.read_sql(sql1, psql_conn)
        print()
        print("Staff members work on May 10, 2021:")
        print(test1)

        sql2 = """
                SELECT DISTINCT staffmembers.ssno, name 
                FROM staffmembers JOIN shifts ON staffmembers.ssno=shifts.ssno 
                WHERE staffmembers.role='doctor' AND shifts.shiftday='Wednesday' 
                AND staffmembers.hospital IN (
                    SELECT name 
                    FROM vaccinationstations 
                    WHERE address LIKE '%%HELSINKI%%'
                );
               """
        test2 = pd.read_sql(sql2, psql_conn)
        print()
        print("Doctors available on Wednesday in Helsinki:")
        print(test2)

        sql3_1 = """
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
               """
        test3_1 = pd.read_sql(sql3_1, psql_conn)
        print()
        print("Current and last locations of each vaccine batch:")
        print(test3_1)

        sql3_2 = """
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
               """
        test3_2 = pd.read_sql(sql3_2, psql_conn)
        print()
        print("Vaccine batches with inconsistent locations:")
        print(test3_2)
 
        sql4 = """
                SELECT ssno, batchid, type, vaccinations.date, vaccinations.location 
                FROM vaccinations 
                JOIN vaccinebatch USING (batchid) 
                JOIN vaccinepatients ON (vaccinepatients.date=vaccinations.date AND vaccinepatients.location=vaccinations.location) 
                JOIN diagnosis ON(diagnosis.patient=vaccinepatients.ssno) 
                WHERE symptom IN (SELECT name FROM symptoms WHERE criticality=1) AND diagnosis.date>'2021-05-10;'
               """
        test4 = pd.read_sql(sql4, psql_conn)
        print()
        print("Patients with critical symptoms diagnosed after May 10, 2021:")
        print(test4)

        sql5 = """
                CREATE VIEW Task5 AS
                SELECT *, CASE WHEN EXISTS (
                    SELECT COUNT(date), ssno 
                    FROM vaccinepatients 
                    WHERE vaccinepatients.ssno=patients.ssno 
                    GROUP BY(ssno) HAVING COUNT(date)=2
                ) 
                THEN CAST(1 AS INT) ELSE CAST(0 AS INT) END AS vaccinestatus FROM patients;

                SELECT * FROM Task5;
               """
        test5 = pd.read_sql(sql5, psql_conn)
        print()
        print("Patients' information and their vaccination status:")
        print(test5)
        
        sql6 = """
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
               """
        test6 = pd.read_sql(sql6, psql_conn)
        test6 = test6.fillna(0)
        test6["v03"] = test6["v03"].astype(int)
        print()
        print("Total number of vaccines stored in each hospital and clinics:")
        print(test6)
        
        sql7 = """
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
               """
        test7 = pd.read_sql(sql7, psql_conn)
        print()
        print("The average frequency of symptoms diagnosed after vaccinations:")
        print(test7)

   
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection):
            psql_conn.close()
            # cursor.close()
            connection.close()
            print()
            print("PostgreSQL connection is closed")
main()