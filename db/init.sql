DROP TABLE IF EXISTS animal_dimension;
CREATE TABLE animal_dimension (
    animal_key INT PRIMARY KEY,
    animal_id VARCHAR,
    animal_name VARCHAR,
    dob DATE,
    animal_type VARCHAR,
    sterilization_status VARCHAR,
    gender VARCHAR,
    breed VARCHAR,
    color VARCHAR
);

DROP TABLE IF EXISTS outcome_type_dimension;
CREATE TABLE outcome_type_dimension (
    outcome_type_key INT PRIMARY KEY,
    outcome_type VARCHAR
);

DROP TABLE IF EXISTS date_dimension;
CREATE TABLE date_dimension (
    date_key INT PRIMARY KEY,
    date_recorded DATE,
    day_of_week VARCHAR,
    month_recorded VARCHAR,
    quarter_recorded VARCHAR,
    year_recorded VARCHAR
);

DROP TABLE IF EXISTS outcomes_fact;
CREATE TABLE outcomes_fact (
    outcome_key SERIAL PRIMARY KEY,
    date_key INT REFERENCES date_dimension(date_key),
    animal_key INT REFERENCES animal_dimension(animal_key),
    outcome_type_key INT REFERENCES outcome_type_dimension(outcome_type_key)
);