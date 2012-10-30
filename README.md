# Hive.Generate.DDL
## Generate Overview
This project will take s DB schema and generate the following scripts

1. Create Hive Table
2. Create Temp Load Hive Table
3. Load local file to Temp Load Hive Table
4. Move records from Temp Table to Final Hive Table
5. Drop Temp Load Hive Table

It will also generate sample data for you to load into your tables.

## UDF Overview
This project contains the following UDFs

1. BigBigInt - This supports binary numbers that are bigger then BigInt and in fact they can store numbers of any size.
