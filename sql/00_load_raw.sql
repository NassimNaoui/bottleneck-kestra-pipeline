CREATE OR REPLACE TABLE raw_erp AS
SELECT *
FROM read_csv_auto(
    'staging/erp.csv',
    header = true,
    all_varchar = true,
    nullstr = ''
);

CREATE OR REPLACE TABLE raw_liaison AS
SELECT *
FROM read_csv_auto(
    'staging/liaison.csv',
    header = true,
    all_varchar = true,
    nullstr = ''
);

CREATE OR REPLACE TABLE raw_web AS
SELECT *
FROM read_csv_auto(
    'staging/web.csv',
    header = true,
    all_varchar = true,
    nullstr = ''
);

