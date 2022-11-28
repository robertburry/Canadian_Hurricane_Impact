import sqlite3 as sql
import pandas as pd
import numpy as np
import get_eccc_data as ec
from multiprocessing import Pool


def nl_ids(stations):
    NL_test = stations[(stations['Province'].isin(['NEWFOUNDLAND'])) &
                       (stations['DLY Last Year'] - stations['DLY First Year'] >= 8) &
                       (stations['DLY Last Year'] >= 2021)]
    return NL_test['Climate ID'].unique().tolist()


def ns_ids(stations):
    NS_test = stations[(stations['Province'].isin(['NOVA SCOTIA'])) &
                       (stations['DLY Last Year'] - stations['DLY First Year'] >= 8) &
                       (stations['DLY Last Year'] >= 2021)]
    return NS_test['Climate ID'].unique().tolist()


def nb_ids(stations):
    NB_test = stations[(stations['Province'].isin(['NEW BRUNSWICK'])) &
                       (stations['DLY Last Year'] - stations['DLY First Year'] >= 8) &
                       (stations['DLY Last Year'] >= 2021)]
    return NB_test['Climate ID'].unique().tolist()


def pe_ids(stations):
    PE_test = stations[(stations['Province'].isin(['PRINCE EDWARD ISLAND'])) &
                       (stations['DLY Last Year'] - stations['DLY First Year'] >= 8) &
                       (stations['DLY Last Year'] >= 2021)]
    return PE_test['Climate ID'].unique().tolist()


def qc_ids(stations):
    QC_test = stations[(stations['Province'].isin(['QUEBEC'])) &
                       (stations['DLY Last Year'] - stations['DLY First Year'] >= 8) &
                       (stations['DLY Last Year'] >= 2021)]
    return QC_test['Climate ID'].unique().tolist()


def on_ids(stations):
    ON_test = stations[(stations['Province'].isin(['ONTARIO'])) &
                       (stations['DLY Last Year'] - stations['DLY First Year'] >= 8) &
                       (stations['DLY Last Year'] >= 2021)]
    return ON_test['Climate ID'].unique().tolist()


def nl_pull(ids):
    df = pd.DataFrame()
    try:
        for yrs in range(2014, 2022):
            df = pd.concat([df, ec.import_climate_data(
                [yrs], ids, 'NL', freq='Daily', mths=range(5, 12))])
        return df

    except:
        print(f'Bad data with: {ids} {yrs}')

    finally:
        pass


def ns_pull(ids):
    df = pd.DataFrame()
    try:
        for yrs in range(2014, 2022):
            df = pd.concat([df, ec.import_climate_data(
                [yrs], ids, 'NS', freq='Daily', mths=range(5, 12))])
        return df

    except:
        print(f'Bad data with: {ids} {yrs}')

    finally:
        pass


def nb_pull(ids):
    df = pd.DataFrame()
    try:
        for yrs in range(2014, 2022):
            df = pd.concat([df, ec.import_climate_data(
                [yrs], ids, 'NB', freq='Daily', mths=range(5, 12))])
        return df

    except:
        print(f'Bad data with: {ids} {yrs}')

    finally:
        pass


def pe_pull(ids):
    df = pd.DataFrame()
    try:
        for yrs in range(2014, 2022):
            df = pd.concat([df, ec.import_climate_data(
                [yrs], ids, 'PE', freq='Daily', mths=range(5, 12))])
        return df

    except:
        print(f'Bad data with: {ids} {yrs}')

    finally:
        pass


def qc_pull(ids):
    df = pd.DataFrame()
    try:
        for yrs in range(2014, 2022):
            df = pd.concat([df, ec.import_climate_data(
                [yrs], ids, 'QC', freq='Daily', mths=range(5, 12))])
        return df

    except:
        print(f'Bad data with: {ids} {yrs}')

    finally:
        pass


def on_pull(ids):
    df = pd.DataFrame()
    try:
        for yrs in range(2014, 2022):
            df = pd.concat([df, ec.import_climate_data(
                [yrs], ids, 'ON', freq='Daily', mths=range(5, 12))])
        return df

    except:
        print(f'Bad data with: {ids} {yrs}')

    finally:
        pass


def export_data(data):
    conn = sql.connect('Canadian_Hurricane_Impact_Completed.db')
    data.to_sql('Climate_Data', conn, if_exists='replace', index=False)


def main():
    stations = ec.import_station_list()
    nl_id = nl_ids(stations)
    nb_id = nb_ids(stations)
    ns_id = ns_ids(stations)
    qc_id = qc_ids(stations)
    on_id = on_ids(stations)

    nl_data = pd.DataFrame()
    ns_data = pd.DataFrame()
    nb_data = pd.DataFrame()
    pe_data = pd.DataFrame()
    qc_data = pd.DataFrame()
    on_data = pd.DataFrame()

    pool = Pool()
    for r in pool.map(nl_pull, nl_test_ids):
        nl_data = pd.concat([nl_data, r])

    for r in pool.map(nb_pull, nb_test_ids):
        nb_data = pd.concat([nb_data, r])

    for r in pool.map(ns_pull, ns_test_ids):
        ns_data = pd.concat([ns_data, r])

    for r in pool.map(pe_pull, pe_test_ids):
        pe_data = pd.concat([pe_data, r])

    for r in pool.map(on_pull, on_test_ids):
        on_data = pd.concat([on_data, r])

    for r in pool.map(qc_pull, qc_test_ids):
        qc_data = pd.concat([qc_data, r])

    Full = pd.concat([nl_data, ns_data, nb_data, pe_data, on_data, qc_data])
    export_data(Full)


if __name__ == "__main__":
    main()
