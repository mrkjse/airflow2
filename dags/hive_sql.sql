CREATE TABLE transactions (
    Date date,
    Description varchar(200),
    Deposits double,
    Withdrawls double, 
    Balance double
) WITH (
    external_location = 'gs://mark-bucket-2021/partition'
)