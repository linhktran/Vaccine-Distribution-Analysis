CREATE TABLE vaccinetype (
    id VARCHAR(10) NOT NULL,
    name VARCHAR(50) NOT NULL,
    doses INT NOT NULL CHECK (doses IN (1,2)),
    tempmin INT NOT NULL,
    tempmax INT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE manufacturer (
    id VARCHAR(10) NOT NULL,
    country VARCHAR(100) NOT NULL,
    phone VARCHAR(20) NOT NULL,
    vaccine VARCHAR(10) NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (vaccine) REFERENCES vaccinetype(id)
);

CREATE TABLE vaccinationstations (
    name VARCHAR(100) NOT NULL,
    address VARCHAR(200) NOT NULL,
    phone VARCHAR(20) NOT NULL,
    PRIMARY KEY (name)
);

CREATE TABLE vaccinebatch (
    batchid VARCHAR(10) NOT NULL,
    amount INT NOT NULL,
    type VARCHAR(10) NOT NULL,
    manufacturer VARCHAR(10) NOT NULL,
    manufdate DATE NOT NULL,
    expiration DATE NOT NULL,
    location VARCHAR(100) NOT NULL,
    PRIMARY KEY (batchid),
    FOREIGN KEY (type) REFERENCES vaccinetype(id),
    FOREIGN KEY (manufacturer) REFERENCES manufacturer(id),
    FOREIGN KEY (location) REFERENCES vaccinationstations(name)
);

CREATE TABLE transportationlog (
    batchid VARCHAR(10) NOT NULL,
    arrival VARCHAR(100) NOT NULL,
    departure VARCHAR(100) NOT NULL,
    datearr DATE NOT NULL,
    datedep DATE NOT NULL,
    PRIMARY KEY (batchid, datearr),
    FOREIGN KEY (arrival) REFERENCES vaccinationstations(name),
    FOREIGN KEY (departure) REFERENCES vaccinationstations(name)
);

CREATE TABLE staffmembers (
    ssno VARCHAR(20) NOT NULL,
    name VARCHAR(50) NOT NULL,
    dateofbirth DATE NOT NULL,
    phone VARCHAR(20) NOT NULL,
    role VARCHAR(20) NOT NULL,
    vaccinationstatus INT NOT NULL,
    hospital VARCHAR(100) NOT NULL,
    PRIMARY KEY (ssno),
    FOREIGN KEY (hospital) REFERENCES vaccinationstations(name)
);

CREATE TABLE shifts (
  ssno VARCHAR(20) NOT NULL,
  hospital VARCHAR(100) NOT NULL,
  shiftday VARCHAR(20) NOT NULL,
  PRIMARY KEY (ssNo),
  FOREIGN KEY (hospital) REFERENCES vaccinationstations(name)
);

CREATE TABLE vaccinations (
  date DATE NOT NULL,
  location VARCHAR(100) NOT NULL,
  batchid  VARCHAR(10) NOT NULL,
  PRIMARY KEY (date, location),
  FOREIGN KEY (location) REFERENCES vaccinationstations(name),
  FOREIGN KEY (batchid) REFERENCES vaccinebatch(batchid)
);

CREATE TABLE patients (
  ssno VARCHAR(20) NOT NULL,
  name VARCHAR(100) NOT NULL,
  dateofbirth  DATE  NOT NULL,
  gender VARCHAR(10) NOT NULL,
  vaccinationstatus INT NOT NULL, 
  PRIMARY KEY (ssno)
);

CREATE TABLE vaccinepatients (
  ssno VARCHAR(20) NOT NULL,
  date DATE NOT NULL,
  location VARCHAR(100) NOT NULL,
  PRIMARY KEY (ssno, date),
  FOREIGN KEY (location) REFERENCES vaccinationstations(name),
  FOREIGN KEY (date) REFERENCES vaccinations(date)
);

CREATE TABLE symptoms (
  name VARCHAR(100) NOT NULL,
  criticality BOOLEAN NOT NULL CHECK (criticality IN (0,1)),
  PRIMARY KEY (name)
);

CREATE TABLE diagnosis (
  patient VARCHAR(20) NOT NULL,
  symptom VARCHAR(100) NOT NULL,
  date DATE NOT NULL,
  PRIMARY KEY (patient, symptom),
  FOREIGN KEY (symptom) REFERENCES symptoms(name),
  FOREIGN KEY (patient) REFERENCES patient(ssno)
);