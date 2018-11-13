# Superset
ILP Superset dashboards

# Installation 
1. Create a virtual env (superset-venv) with python 3.6.6
2. Install superset 
`pip install superset`
3. Create an admin user (you will be prompted to set a username, first and last name before setting a password)
`fabmanager create-admin --app superset`
4. Initialize the database `superset db upgrade`
5. Create default roles and permissions `superset init`
6. Run `python import_supersetdata.py hostname db_name user "password" 2` (This script should be run as a cron job so as to get data updated daily)
6. Run `superset import_datasources -p superset_2.yaml` to restore the db, table, table_columns and table metrices.
7. To start a development web server on port 8088, use -p to bind to another port, `superset runserver -d`
8. Login to Superset UI -> Manage -> Import Dashboards -> Choose the three dashboards for the repo.
9. Now you'll be able to see the data in the `Dashboards`.
