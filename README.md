# Hive.Generate.DDL
## Generate Overview
This project will take s DB schema and generate the three things:

1. Create Table Scripts
  - Create an external table or hive user tables
  - RCFiles
  - Converts big number to java big integers

2. Load scripts
  - Supports insert into hive 8 and 9
  - Supports delta change loads
  - Will soon support partitioned delta loads
  - Load from local or HDFS
  - Converts Dates
  - Compresses Data

3. Test Data
  - Single or multi load test data.

## UDF Overview
This project contains the following UDFs

1. BigBigInt - This supports binary numbers that are bigger then BigInt and in fact they can store numbers of any size.
