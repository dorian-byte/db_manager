# App Installation and Usage Guide

## Prerequisites

- You must have **Postgres** installed, preferably version 14+.

## Database Configuration

By default, this app assumes the existence of a database named "postgres" in Postgres. 

However, if you wish to use a different database or configuration:
- Configure the `db_config` parameter to specify your own DB settings.

## Testing Data

This app comes with a `users.csv` file for testing purposes, and also to exemplify the structure the input csv data should follow.

## Running the App

Execute the following commands to run the app:

```
pip install -r requirements.txt
python data_processor.py
```

After executing the above commands, a `users` table will be created in the Postgres database. If no table name is given, the name of the csv file will be taken as the default name for the table being created.

## Viewing the Data

To view the data in the `users` table:

```
psql postgres
select * from users;
```
