import psycopg2
from psycopg2 import Error
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt


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
        print("\n")
        engine = create_engine(DIALECT + db_uri)
        psql_conn  = engine.connect()
 
    
    #------------------------------------
    # Project part 3
    #------------------------------------
        # Task 1

        qs1 = """
                SELECT ssno, gender, dateofbirth, symptom, date 
                FROM patients JOIN diagnosis ON(patients.ssno=diagnosis.patient)
              """
        dfqs1 = pd.read_sql(qs1, psql_conn)
        dfqs1.columns=['ssNO','gender','dateOfBirth','symptom','diagnosisDate']
        print("Task 1")
        print()
        print("PatientSymptoms dataframe:")
        print()
        print(dfqs1)
        print("\n")
        

        psql_conn.execute('DROP TABLE IF EXISTS patientsymptoms')
        psql_conn.execute(
            'CREATE TABLE patientsymptoms ('
            'index_label INT NOT NULL,'
            'ssno VARCHAR(20) NOT NULL,'
            'gender VARCHAR(10) NOT NULL,'
            'dateofbirth  DATE  NOT NULL,'
            'symptom VARCHAR(100) NOT NULL,'
            'diagnosisdate DATE NOT NULL,'
            'PRIMARY KEY (ssno)'
            ');'
        )
        dfqs1.to_sql('patientsymptoms',psql_conn,if_exists="replace", index=True)


        # Task 2

        qs2 = """
		    DROP VIEW IF EXISTS one;
		    DROP VIEW IF EXISTS two;

            CREATE VIEW one AS 
              SELECT ssno, t1.date, t1.location, type 
              FROM vaccinepatients AS t1 
              JOIN vaccinepatients AS t2 USING(ssno) 
              JOIN vaccinations AS vacc ON(t1.date=vacc.date AND t1.location=vacc.location) 
              JOIN vaccinebatch USING(batchid) WHERE t1.date=t2.date;

            CREATE VIEW two AS 
              SELECT ssno, t1.date, t1.location, type 
              FROM vaccinepatients AS t1 
              JOIN vaccinepatients AS t2 USING(ssno) 
              JOIN vaccinations AS vacc ON(t1.date=vacc.date AND t1.location=vacc.location) 
              JOIN vaccinebatch USING(batchid) WHERE t1.date!=t2.date;

            SELECT ssno, one.date AS date1, one.type AS type1, two.date AS date2, two.type AS type2 
            FROM one FULL OUTER JOIN two USING(ssno) FULL OUTER JOIN patients USING(ssno) 
            WHERE one.date < two.date OR two.date IS NULL;
              """
        dfqs2 = pd.read_sql(qs2, psql_conn)

        print("Task 2")
        print()
        print("PatientVaccineInfo dataframe:")
        print()
        print(dfqs2)
        print("\n")

        psql_conn.execute('DROP TABLE IF EXISTS patientvaccineinfo')
        psql_conn.execute(
            'CREATE TABLE patientvaccineinfo ('
            'ssno VARCHAR(20) NOT NULL,'
            'date1 DATE NOT NULL,'
            'type1 VARCHAR(10) NOT NULL,'
            'date2 DATE NOT NULL,'
            'type2 VARCHAR(10) NOT NULL,'
            'PRIMARY KEY (ssno)'
            ');'
        )
        dfqs2.to_sql('patientvaccineinfo', psql_conn, if_exists="replace",index=False)


        # Task 3

        dfqs3 = pd.read_sql("""SELECT * FROM patientsymptoms""", psql_conn)
        mask1 = dfqs3['gender'] == 'M'
        mask2 = dfqs3['gender'] == 'F'
        dfqs2M = dfqs3[mask1].reset_index(drop=True)
        dfqs2F = dfqs3[mask2].reset_index(drop=True)

        print("Task 3")
        print()
        print("Symptoms of male patients:")
        print()
        print(dfqs2M)
        print("\n")

        print("Symptoms of female patients:")
        print()
        print(dfqs2F)
        print("\n")

        grp_1 = dfqs2M.groupby(['symptom'], as_index=False).agg({'ssNO':"count"})
        grp_2 = dfqs2F.groupby(['symptom'], as_index=False).agg({'ssNO':"count"})
        grp_1 = grp_1.sort_values(ascending=False,by=['ssNO']).reset_index(drop=True)
        grp_2 = grp_2.sort_values(ascending=False, by=['ssNO']).reset_index(drop=True)
        grp_1.columns=['symptom','count']
        grp_2.columns=['symptom','count']

        print("Top three most common symptoms for males:")
        print()
        print(grp_1.head(3))
        print("\n")

        print("Top three most common symptoms for females:")
        print()
        print(grp_2.head(3))
        print("\n")


        # Task 4

        df_patients = pd.read_sql('''
                                  SELECT * FROM patients
                                  ''', psql_conn)
        

        now = pd.Timestamp('now')
        df_patients['dateofbirth'] = pd.to_datetime(df_patients['dateofbirth'], errors='coerce')
        df_patients['age'] = (now - df_patients['dateofbirth']).astype('<m8[Y]') 

        bins= [-1,9,19,39,59,200]
        labels = ['0-9','10-19','20-39','40-59','60+']
        df_patients['agegroup'] = pd.cut(df_patients['age'], bins=bins, labels=labels)
        df_patients = df_patients.drop(['age'], axis=1)
        
        print("Task 4")
        print()
        print("Patients dataframe:")
        print()
        print(df_patients)
        print("\n")


        # Task 5

        qs5 = """
                SELECT ssno, COUNT(date) as vaccinestatus 
                FROM vaccinepatients 
                GROUP BY(ssno);
               """
        df_vs = pd.read_sql(qs5, psql_conn)
        df_vaccinestatus = pd.merge(df_patients, df_vs, on='ssno', how='left')
        df_vaccinestatus["vaccinestatus"] = df_vaccinestatus["vaccinestatus"].fillna(0).astype(int)

        print("Task 5")
        print()
        print("Vaccine status dataframe:")
        print()
        print(df_vaccinestatus)
        print("\n")


        # Task 6
        
        df_6 = df_vaccinestatus.pivot_table(values='ssno',index='vaccinestatus', columns='agegroup', aggfunc='count')
        df_6 = df_6.div(df_6.sum()).fillna(0)
        
        print("Task 6")
        print()
        print("Percentage of vaccination status in each age group:")
        print()
        print(df_6)
        print("\n")


        # Task 7

        psql_conn.execute('DROP VIEW IF EXISTS onedose1 CASCADE')
        psql_conn.execute('DROP VIEW IF EXISTS twodosefirst1 CASCADE')
        psql_conn.execute('DROP VIEW IF EXISTS twodosesecond2 CASCADE')
        psql_conn.execute('DROP VIEW IF EXISTS final3 CASCADE')
        psql_conn.execute('DROP VIEW IF EXISTS v_1 CASCADE')
        psql_conn.execute('DROP VIEW IF EXISTS v_2 CASCADE')
        psql_conn.execute('DROP VIEW IF EXISTS v_3 CASCADE')

        qs7 = """
                CREATE VIEW onedose1 AS 
                SELECT DISTINCT(ssno), t1.date, t1.location, type, symptom, diagnosis.date AS diagnosisdate 
                FROM vaccinepatients AS t1 
                JOIN vaccinepatients AS t2 USING(ssno) 
                FULL OUTER JOIN diagnosis ON(ssno=patient) 
                JOIN vaccinations ON(t1.date=vaccinations.date AND t1.location=vaccinations.location) 
                JOIN vaccinebatch USING(batchid) 
                WHERE t1.date=t2.date AND (diagnosis.date>=t1.date OR diagnosis.date IS NULL);
                
                CREATE VIEW twodosefirst1 AS 
                SELECT DISTINCT(ssno), t1.date, t1.location, type, symptom, diagnosis.date AS diagnosisdate 
                FROM vaccinepatients AS t1 
                JOIN vaccinepatients AS t2 USING(ssno) 
                FULL OUTER JOIN diagnosis ON(ssno=patient) 
                JOIN vaccinations ON(t1.date=vaccinations.date AND t1.location=vaccinations.location)
                JOIN vaccinebatch USING(batchid) 
                WHERE t1.date<t2.date AND (diagnosis.date>=t1.date OR diagnosis.date IS NULL);
                
                CREATE VIEW twodosesecond2 AS 
                SELECT DISTINCT(ssno), t2.date, t2.location, type, symptom, diagnosis.date AS diagnosisdate 
                FROM vaccinepatients AS t1 JOIN vaccinepatients AS t2 USING(ssno) 
                FULL OUTER JOIN diagnosis ON(ssno=patient) 
                JOIN vaccinations ON(t2.date=vaccinations.date AND t2.location=vaccinations.location) 
                JOIN vaccinebatch USING(batchid) 
                WHERE t1.date<t2.date AND (diagnosis.date>=t2.date OR diagnosis.date IS NULL);
                
                CREATE VIEW final3 AS 
                SELECT DISTINCT ssno, type, symptom 
                FROM (
                    SELECT * FROM onedose1 AS p1 
                    UNION 
                    SELECT * FROM twodosefirst AS p2 UNION SELECT * FROM twodosesecond AS p3
                ) AS p;
                    
                CREATE VIEW v_1 AS SELECT * FROM final3 WHERE type='V01';

                CREATE VIEW v_2 AS SELECT * FROM final3 WHERE type='V02';
                
                CREATE VIEW v_3 AS SELECT * FROM final3 WHERE type='V03';
                
                SELECT symptom, 
                    CASE WHEN v1frequency >= 0.1 THEN 'very common'
                         WHEN v1frequency >= 0.05 AND v1frequency < 0.1 THEN 'common'
                         WHEN v1frequency > 0.0 AND v1frequency < 0.05 THEN 'rare'
                         WHEN v1frequency = 0.0 THEN '-'
                         ELSE '-'
                    END AS v01, 
                    CASE  WHEN v2frequency >= 0.1 THEN 'very common'
                          WHEN v2frequency >= 0.05 AND v2frequency < 0.1 THEN 'common'
                          WHEN v2frequency > 0.0 AND v2frequency < 0.05 THEN 'rare'
                          WHEN v2frequency = 0.0 THEN '-' 
                          ELSE '-'
                    END AS v02,     
                    CASE  WHEN v3frequency >= 0.1 THEN 'very common'
                          WHEN v3frequency >= 0.05 AND v3frequency < 0.1 THEN 'common'
                          WHEN v3frequency > 0.0 AND v3frequency < 0.05 THEN 'rare'
                          WHEN v3frequency = 0.0 THEN '-'                                           
                          ELSE '-'
                      END AS v03                                           
                FROM (
                    SELECT symptom, ROUND(COUNT(symptom)*1.0/(SELECT COUNT(DISTINCT ssno) FROM v_1),2) AS v1frequency
                    FROM v_1 WHERE symptom IS NOT NULL 
                    GROUP BY(symptom)
                ) AS v_1 
                FULL OUTER JOIN (
                    SELECT symptom, ROUND(COUNT(symptom)*1.0/(SELECT COUNT(DISTINCT ssno) FROM v_2),2) AS v2frequency
                    FROM v_2 WHERE symptom IS NOT NULL 
                    GROUP BY(symptom)
                ) AS v_2 USING(symptom) 
                FULL OUTER JOIN (
                    SELECT symptom, ROUND(COUNT(symptom)*1.0/(SELECT COUNT(DISTINCT ssno) FROM v_3),2) AS v3frequency             
                    FROM v_3 WHERE symptom IS NOT NULL 
                    GROUP BY(symptom)
                ) AS v_3 USING(symptom);
                """
        
        test7 = pd.read_sql(qs7, psql_conn)         
        df_7 = pd.DataFrame(test7, columns=['symptom', 'v01', 'v02', 'v03'])
        
        print("Task 7")
        print()
        print("Dataframe for symptoms from three vaccines:")
        print()
        print(df_7)
        print("\n")


        # Task 8
        
        psql_conn.execute('DROP VIEW IF EXISTS k01 CASCADE')
        qs8 = """
              CREATE VIEW k01 AS
              SELECT * FROM (
                SELECT vaccinepatients.date, COUNT(DISTINCT ssno) as countparticipants, vaccinepatients.location 
                FROM vaccinepatients
                GROUP BY(vaccinepatients.date,vaccinepatients.location)
              ) AS a1 
              FULL OUTER JOIN (
                SELECT vaccinations.date, vaccinations.location, vaccinations.batchid, vaccinebatch.amount 
                FROM vaccinations, vaccinebatch
                WHERE vaccinations.batchid = vaccinebatch.batchid AND vaccinations.location = vaccinebatch.location
              ) AS a2
              USING(date, location);
         
              SELECT date, location, (countparticipants * 100.00/k01.amount) AS expected
              FROM k01
              GROUP BY (date, location,expected);
               """
        
        test8 = pd.read_sql(qs8, psql_conn)
        df_8 = pd.DataFrame(test8, columns=['date', 'location', 'expected'])    
        df8_avg = df_8['expected'].mean()
        df8_std = df_8['expected'].std()
        df8_final = df8_avg + df8_std

        print("Task 8")
        print()
        print("Expected percentage of patients that will attend: " + str(df8_avg) + "%")
        print()
        print("Standard deviation of the percentage of attending patients: " + str(df8_std))
        print()
        print("The estimated amount of vaccines (as a percentage) that should be reserved for each vaccination to minimize waste: " + str(df8_final) + "%")       
        print("\n")


        # Task 9

        qs9 = """ 
                SELECT date, COUNT(DISTINCT ssno) AS total
                FROM vaccinepatients
                GROUP BY(date);
              """
        dfqs9 = pd.read_sql(qs9, psql_conn)

        dfqs9.groupby('date')['total'].sum().plot(kind='bar', title='Number of vaccinated patients with respect to date', xlabel='Vaccination date', ylabel='Number of vaccinated patients', rot = 0)
        plt.show()


        # Task 10

        qs10 = """
                DROP VIEW IF EXISTS task10;

                CREATE VIEW task10 AS 
                SELECT To_Char("date", 'Day') AS shiftday FROM vaccinations
                WHERE date < '2021-05-15'::date AND date > '2021-05-04'::date;

                SELECT patients.ssno, patients.name 
                FROM vaccinepatients JOIN patients ON vaccinepatients.ssno = patients.ssno
                WHERE vaccinepatients.location IN (SELECT location 
                                                   FROM vaccinations JOIN staffmembers ON vaccinations.location = staffmembers.hospital
                                                   WHERE staffmembers.ssno = '19740919-7140'
                                                   AND vaccinations.date < '2021-05-15' AND vaccinations.date > '2021-05-04')
                AND vaccinepatients.date IN (SELECT date 
                                             FROM vaccinations JOIN staffmembers ON vaccinations.location = staffmembers.hospital
                                             WHERE staffmembers.ssno = '19740919-7140'
                                             AND vaccinations.date < '2021-05-15' AND vaccinations.date > '2021-05-04')
                UNION
                SELECT staffmembers.ssno, staffmembers.name
                FROM staffmembers JOIN shifts ON staffmembers.ssno = shifts.ssno 
                WHERE shiftday IN (SELECT regexp_replace(shiftday, '\s+$', '') FROM task10) 
                AND station IN (SELECT location 
                                FROM vaccinations JOIN staffmembers ON vaccinations.location = staffmembers.hospital
                                WHERE staffmembers.ssno = '19740919-7140'
                                AND vaccinations.date < '2021-05-15' AND vaccinations.date > '2021-05-04')
                AND staffmembers.ssno != '19740919-7140';
                """
        dfqs10 = pd.read_sql(qs10, psql_conn)

        print("Task 10")
        print()
        print("Patients and staff members who may have met the nurse:")
        print()
        print(dfqs10)

   
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