import pandas as pd
import numpy as np
import sqlite3 as sql


def main():
    ReportType = 'Daily'
    Format = 'csv'
    StartDate = '9/04/2010'
    EndDate = '9/06/2010'
    Country = 'can'
    province = ['NB', 'NS', 'NL', 'PE']

    hurr_url = f"""https://data.cocorahs.org/export/exportreports.aspx?ReportType={ReportType}&
    Format={Format}&ReportDateType=reportdate&StartDate={StartDate}&EndDate={EndDate}&
    country={Country}&province={",".join(province)}"""

    hurr = pd.read_csv(hurr_url)
    hurr.drop(columns=['NewSnowDepth', 'NewSnowSWE',
              'TotalSnowDepth', 'TotalSnowSWE'], axis=1, inplace=True)
    hurr['ReportType'] = ReportType
    hurr['Hurricane_Name'] = 'EARL'

    hurr['TotalPrecipAmt'] = hurr['TotalPrecipAmt'] * 25.4

    cnx = sql.connect("Canadian_Hurricane_Impact.db")  # input Database Name
    hurr.to_sql('Complete_Impact', cnx, if_exists='append',
                index=False)  # Exports data to an SQL Lite Database
    cnx.close()


if __name__ == "__main__":
    main()
