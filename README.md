# ETL pipeline for wind turbines data

    Solution on how to extract, load, process the daily comming data from a cluster of 
    turbines, and finally storing the results into a given database.

Used tools:
- to extract and process the data: Python and more specifically Pandas library
- to schedule the task for daily run: Airflow
- to store the data: PostgresDB
- to containerize: Docker

### In the end, after the data has been processed the following stats are obtained for power_output:
- minimal value 
- maximal value 
- mean value
- anomalies

Also the Null values has been replaced and the rows containing outliers have been removed.
# How to run the tool
1. Clone the project
2. In the project directory run the command in cmd/terminal: 
    ```docker-compose up airflow-init```
3. After the previous command finished run: ```docker-compose up```
4. Create a ```Database Connection```. Personally, I used DBeaver, but feel free to use whatever provider you desire.
   1. Select PostgresSQL
   2. HOST: ```localhost```
   3. Database: ```postgres```
   4. Username\Password: ```airflow```
   5. Port: ```5432```
5. After the Connection is set, go to the created connection-> databases and create a new database, called ```test```.
6. In browser, go to ```http://localhost:8080/``` which is the airflow web client. Log in with the credentials airflow/airflow.
7. Create connection to database, by going to Admin->Connections and click the plus button.
8. Enter the following values:
- Conn Id: ```postgres_localhost```
- Conn Type: ```postgres```
- Host: ```host.docker.internal``` (if you are running docker locally). ```localhost``` if not
- Schema: ```test```
- Login\Password: ```airflow```
- Port: 5432
9. After the configurations are set, we can go in the project folder and give the DAG, the data which will be processed.
We do that by creating a folder called ```data``` inside the dags folder, and pasting our csvs into the resulting folder.
10. Now we can go back to the airflow client and start the dag.

   Shortly, after the DAG is finished we can find our resulting data in PostgresDB.
