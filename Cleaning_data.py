import pandas as pd
import sqlite3 as sql
import numpy as np
from sqlalchemy import create_engine


def retrieve_from_db():
    cnx = sql.connect('Canadian_Hurricane_Impact.db')
    cocorahs = pd.read_sql_query('SELECT * FROM Complete_impact', cnx)
    # metar = pd.read_sql_query('SELECT * FROM Filtered_Metar_Data', cnx)
    clim_data = pd.read_sql_query('SELECT * FROM Filtered_Climate_Data', cnx)
    cnx.close()
    return cocorahs, clim_data


def clean_climate(clim_data, cocorahs):
    dates['ObservationDate'] = pd.to_datetime(cocorahs['ObservationDate'])
    clim_data['Date/Time'] = pd.to_datetime(clim_data['Date/Time'])
    clim_data.rename(columns={'Date/Time': 'ObservationDate'}, inplace=True)
    clim_data['Hurricane_Name'] = clim_data['ObservationDate'].map(
        dates[['ObservationDate', 'Hurricane_Name']].set_index('ObservationDate').to_dict()['Hurricane_Name'])

    clim_data = clim_data[clim_data['Hurricane_Name'].notna()]
    return clim_data


def process_sql_using_pandas(dates, ):
    engine = create_engine("sqlite:////Canadian_Hurricane_Impact.db")
    conn = engine.connect().execution_options(stream_results=True)
    df = pd.DataFrame()
    for chunk_dataframe in pd.read_sql("SELECT * FROM Metar_Data", conn, chunksize=500000):
        chunk_dataframe['valid'] = pd.to_datetime(chunk_dataframe['valid'])
        df = pd.concat([df, chunk_dataframe[chunk_dataframe.valid.dt.strftime(
            '%y%m%d').isin(dates.ObservationDate.dt.strftime('%y%m%d'))]])
    conn.close()
    return df


def export_data(clim_data, metar, cocorahs):
    conn = sql.connect('Canadian_Hurricane_Impact_Completed.db')
    clim_data.to_sql('Climate_Data', conn, if_exists='replace', index=False)
    metar.to_sql('Metar_Data', conn, if_exists='replace', index=False)
    cocorahs.to_sql('CoCoRaHS_Data', conn, if_exists='replace', index=False)
    conn.close()
    return


def main():
    cocorahs, clim_data = retrieve_from_db()
    clim_data = clim_data.convert_dtypes()
    cocorahs = cocorahs.convert_dtypes()
    clim_data = clean_climate(clim_data, cocorahs)
    metar = process_sql_using_pandas(cocorahs)
    metar = metar.convert_dtypes()
    export_data(clim_data, metar, cocorahs)

    # clim_data.info(memory_usage='deep', verbose=False)
    # # metar.info(memory_usage='deep', verbose=False)
    # # cocorahs.info(memory_usage='deep', verbose=False)


if __name__ == "__main__":
    main()
