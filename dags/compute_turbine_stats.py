import pandas as pd
import psycopg2
import psycopg2.extras
import os
import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
from typing import Union


def create_connection():
    """
            Create connection with the Postgress DB via airflow hook.


            Returns
            -------
            PostgresHook.connection
    """
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    conn = hook.get_conn()
    return conn


def handle_missing_and_outliers_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filling NULL values from Dataframe and removes rows containing outliers values.

    Parameters
    ----------
    df : pd.Dataframe
        Original Dataframe which will be processed.
    Returns
    ----------
    df : pd.Dataframe
        Dataframe with no NULL values and removed outliers values.
    """
    # Parse through columns which contain missing values and replace the missing values with the column mean
    for column in df.columns[df.isnull().any(axis=0)]:
        df[column].fillna(df[column].mean(), inplace=True)

    # Remove outliers from given columns
    for col in ['wind_speed', 'wind_direction', 'power_output']:
        q_low = df[col].quantile(0.01)
        q_hi = df[col].quantile(0.99)

        df = df[(df[col] < q_hi) & (df[col] > q_low)]

    return df.reset_index().drop(columns=['index'])


def get_last_updated_date(conn) -> Union[datetime.date, None]:
    """
    Get the last updated date in the database.

    Parameters
    ----------
    conn : PostgresHook.connection
        Connector to Postgress DB

    Returns
    -------
    last_updated_dt : Union[datetime.date, None]
    """
    cursor = conn.cursor()
    try:
        cursor.execute("select max(timestamp) from prototype_results pr")
        last_updated_dt = cursor.fetchone()[0]
    except:
        last_updated_dt = None
    finally:
        cursor.close()
        conn.commit()
    logging.info(f"last_udated_date is: {last_updated_dt}")
    return last_updated_dt


def set_datetime_index(df: pd.DataFrame, dt: Union[datetime.date, None]) -> pd.DataFrame:
    """
    Convert the csv date column to datetime, keep only the new data (if dt exists) and set the column date as index.
    Parameters
    ----------
    df : pd.DataFrame
        Our turbine data from csv
    dt : Union[datetime.date, None]
        Last updated date

    Returns
    -------
    df: pd.DataFrame
        Processed Dataframe
    """
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

    # If records already exist in database, select the data that hasn't been processed
    if dt:
        df = df[df['timestamp'] >= (datetime.datetime(dt.year, dt.month, dt.day, 0, 0, 0) + datetime.timedelta(days=1))]
    df = df.set_index('timestamp')
    return df


def calculate_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute the min,max, mean for each turbine in a 24h window.
    Parameters
    ----------
    df : pd.Dataframe

    Returns
    -------
    df_final: pd.Dataframe
        Dataframe containing the statistics
    """
    # Group the data by 24h for each turbine and compute the minimum, maximum and mean for the resulting groups
    df_compact = df.groupby(['turbine_id', pd.Grouper(freq='24H')])['power_output'].agg(['min', 'max', 'mean'])

    # Unfold the data and rename the resulting columns
    df_expanded = df_compact.unstack('turbine_id')
    df_final = df_expanded.unstack().reset_index().rename(columns={'level_0': 'operation_type', 0: 'operation_result'})
    return df_final


def find_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Find the anomalies in the power_output values.
    Parameters
    ----------
    df : pd.Dataframe
        Dataframe with containing statistics for each turbine by 24h window.

    Returns
    -------
    df_merged : pd.Dataframe
        DataFrame containing the mean of power_output, with an added flag column which signals if the mean value
        is an anomaly.
    """
    # Take only the mean values and compute their mean & standard deviation
    df = df[df['operation_type'] == 'mean'].drop('operation_type', axis=1)
    df_deviation_group = df.groupby('turbine_id').agg({'operation_result': ['mean', 'std']}).reset_index()

    # Find the upper and lower anomaly value threshold
    df_deviation_group['upper_anomaly_value'] = (df_deviation_group[('operation_result', 'mean')] + \
                                                 2*df_deviation_group[('operation_result', 'std')])
    df_deviation_group['lower_anomaly_value'] = (df_deviation_group[('operation_result', 'mean')] - \
                                                 2*df_deviation_group[('operation_result', 'std')])

    # Select only the necessary columns and unnest them
    df_deviation = df_deviation_group[['turbine_id', 'upper_anomaly_value', 'lower_anomaly_value']].stack(
        level=1).swaplevel(0, axis=0)

    # Merge the anomalies values with the mean stats and find the instances which are anomalies
    df_merged = pd.merge(df, df_deviation, on='turbine_id')
    df_merged['anomaly'] = (df_merged['operation_result'] > df_merged['upper_anomaly_value']) | (
            df_merged['operation_result'] < df_merged['lower_anomaly_value'])

    df_merged = df_merged[['timestamp', 'turbine_id', 'operation_result', 'anomaly']].rename(
        columns={'operation_result': 'power_output_mean'})
    df_merged['operation_type'] = 'mean'
    return df_merged


def combine_results(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """

    Parameters
    ----------
    df1 : pd.Dataframe
        Contains min, max, mean of turbines.
    df2 : pd.Dataframe
        Contains mean and anomalies of turbines.

    Returns
    -------
    df_merged: pd.Dataframe
        Dataframe containing all the final stats (min, max, mean, anomalies)
    """
    df_merged = pd.merge(df1, df2,
                         on=['timestamp', 'turbine_id', 'operation_type'],
                         how='left').drop(columns='power_output_mean', axis=1)
    df_merged = df_merged[['timestamp', 'turbine_id', 'operation_type', 'operation_result', 'anomaly']]

    # Convert the flag column type to float. If not, this will result in an error
    # when trying to insert the data into DB
    df_merged['anomaly'] = df_merged['anomaly'].astype(float)
    return df_merged


def create_table(conn) -> None:
    """
    Create the table in Postgress if it doesn't exists already
    Parameters
    ----------
    conn :
        Postgress connection
    Returns
    -------
    """
    sql = """
           CREATE TABLE IF NOT EXISTS prototype_results (
               timestamp date not null,
               turbine_id integer not null,
               operation_type VARCHAR(200) not null,                
               operation_result double precision,
               anomaly real,
               primary key (timestamp,turbine_id,operation_type)
           )
           """
    cursor = conn.cursor()
    cursor.execute(sql)
    cursor.close()
    conn.commit()


def insert_rows(df: pd.DataFrame, conn) -> None:
    """
    Insert data into database, only if the key (timestamp,turbine_id,operation_type) is unique.
    Parameters
    ----------
    df : pd.DataFrame
        Dataframe containing the final results
    conn : PostgresHook.connection

    Returns
    -------
    """
    insert_query = f"""INSERT INTO prototype_results(
                            timestamp,
                            turbine_id,
                            operation_type,
                            operation_result,
                            anomaly
                            ) values (%s,%s,%s,%s,%s);       
                   """
    cursor = conn.cursor()
    try:
        psycopg2.extras.execute_batch(cur=cursor, sql=insert_query, argslist=df.values)
    except Exception as error:
        raise Exception(f"Error inserting new data: {error}")
    finally:
        cursor.close()
        conn.commit()


def start():
    postgres_conn = create_connection()
    create_table(postgres_conn)
    last_updated_dt = get_last_updated_date(postgres_conn)

    # Process each csv file
    for i in os.listdir('dags/data/'):
        df_original = pd.read_csv('dags/data/' + i)

        # Prepare the data
        df = handle_missing_and_outliers_values(df_original.copy())
        df = set_datetime_index(df, last_updated_dt)

        # Compute the basic stats (min, max, mean)
        df_stats = calculate_stats(df)
        logging.info(f"Processed Dataframe : {i}")

        # Compute the anomaly values and merge them with the rest of the stats
        df_anomalies = find_anomalies(df_stats)
        df_results = combine_results(df_stats, df_anomalies)

        # insert data into DB
        insert_rows(df_results, postgres_conn)

