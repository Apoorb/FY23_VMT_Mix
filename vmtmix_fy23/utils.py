"""
Common functions that will be used by all modules.
Created by: Apoorba Bibeka
Crated on: 2/6/2023
"""
from pathlib import Path
import pandas as pd
import re
import inflection
from functools import wraps
from time import time
from sqlalchemy import create_engine
import mysql.connector as mariadb
import sys
import numpy as np

# Set path to the code and datasets.
path_prj_code = Path(r"C:\Users\a-bibeka\PycharmProjects\FY23_VMT_Mix")
path_data = Path(
    r"E:\Texas A&M Transportation Institute"
    r"\TxDOT_TPP_Projects - Task 5.3 Activity Forecasting Factors\Data"
)
path_inp = Path.joinpath(path_data, "input")
path_county_shp = Path.joinpath(
    path_inp, "Shape_files", "Texas_County_Boundaries", "County.shp"
)
path_urbanized_shp = Path.joinpath(
    path_inp, "Shape_files", "TxDOT_Urbanized_Areas", "Urbanized_Area.shp"
)
path_txdot_districts_shp = Path.joinpath(
    path_inp, "Shape_files", "TxDOT_Districts", "TxDOT_Districts.shp"
)
path_txdot_fy22 = Path.joinpath(path_inp, "fy22_txdot")
path_tx_hpms_2018 = Path.joinpath(path_inp, "tx_hpms_2018")
path_faf = Path.joinpath(path_inp, "faf")
path_interm = Path.joinpath(path_data, "intermediate")
path_output = Path.joinpath(path_data, "output")
path_fig_dir = Path.joinpath(path_interm, "figures")

paths = [
    path_prj_code,
    path_data,
    path_inp,
    path_county_shp,
    path_urbanized_shp,
    path_txdot_districts_shp,
    path_txdot_fy22,
    path_tx_hpms_2018,
    path_faf,
    path_interm,
    path_output,
    # path_fig_dir,
]

for path_ in paths:
    assert path_.exists(), f" {path_} Path does not exsit"


class ChainedAssignent:
    """
    This class ChainedAssignment is used to control the behavior of chained assignment
    in pandas. It can be used as a context manager to temporarily change the
    chained_assignment option in pandas and then revert to the original value when
    the context is exited.

    chained: A string that specifies the behavior of chained assignment. Acceptable
    values are None, "warn", and "raise". If None, no warning or exception will be
    raised when a chained assignment is encountered. If "warn", a warning will be raised.
    If "raise", an exception will be raised.

    saved_swcw: A variable that stores the original value of the chained_assignment
    option in pandas.

    The __enter__ method sets the chained_assignment option to the value specified by
    the chained argument and returns the context manager instance.

    The __exit__ method sets the chained_assignment option back to the value stored in
    saved_swcw.

    This class allows for a more convenient and readable way to control the behavior of
    chained assignment in pandas, compared to setting and resetting the option manually
    in separate statements.
    """

    def __init__(self, chained=None):
        acceptable = [None, "warn", "raise"]
        assert chained in acceptable, "chained must be in " + str(acceptable)
        self.swcw = chained

    def __enter__(self):
        self.saved_swcw = pd.options.mode.chained_assignment
        pd.options.mode.chained_assignment = self.swcw
        return self

    def __exit__(self, *args):
        pd.options.mode.chained_assignment = self.saved_swcw


def get_snake_case_dict(columns):
    """Get columns in snake_case."""
    return {col: re.sub(r"\W+", "_", inflection.underscore(col)) for col in columns}


def timing(f):
    """
    timing(f) is a decorator that measures and prints execution time of a function `f`.
    It takes a function as an argument and returns a wrapped version with timing
    functionality.
    """

    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        print("func:{} with argumets {} took: {} sec".format(f.__name__, kw, te - ts))

        return result

    return wrap


def get_engine_to_output_to_db(db):
    """
    Get engine to output data to out_database using pd.to_sql().
    """
    engine = create_engine(f"mariadb+mariadbconnector://root:moves@127.0.0.1:3306/{db}")
    return engine


def connect_to_server_db(database_nm, user_nm="moves", port_=3308):
    """
    Function to connect to a particular database on the server.
    Returns
    -------
    conn_: mariadb.connection
        Connection object to access the data in MariaDB Server.
    """
    # Connect to MariaDB Platform
    try:
        conn_ = mariadb.connect(
            user=user_nm,
            password="moves",
            host="127.0.0.1",
            port=port_,
            database=database_nm,
        )
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)
    return conn_


def create_sut_fueltype_map():
    """
    Create Mapping for SUT type and Fuel type. 23 combination.
    """
    sut = {
        11: "MC",
        21: "PC",
        31: "PT",
        32: "LCT",
        41: "OBus",
        42: "TBus",
        43: "SBus",
        51: "RT",
        52: "SuShT",
        53: "SuLhT",
        54: "MH",
        61: "CShT",
        62: "CLhT",
    }
    fueltype = {1: "Gas", 2: "Diesel"}
    sut_df = pd.DataFrame({"sut": sut.keys()})
    sut_df_1 = (
        sut_df.assign(
            fueltype=lambda df: np.select(
                [df.sut == 11, df.sut != 11], [{1}, {1, 2}], np.nan
            )
        )
        .explode("fueltype")
        .reset_index(drop=True)
        .assign(
            sut_lab=lambda df: df.sut.map(sut),
            fueltype_lab=lambda df: df.fueltype.map(fueltype),
            sut_fueltype_lab=lambda df: df.sut_lab + "_" + df.fueltype_lab,
        )
        .filter(items=["sut", "fueltype", "sut_fueltype_lab"])
    )
    sut_df_1 = sut_df_1.rename(
        columns={"sut": "sourceTypeID", "fueltype": "fuelTypeID"}
    )
    return sut_df_1


if __name__ == "__main__":
    connect_to_server_db("movesdb20220105")
    conn = connect_to_server_db(database_nm="movesdb20220105")
    with conn:
        sql = "SELECT * FROM samplevehiclepopulation"
        mvs303samvehpop_ = pd.read_sql(sql, con=conn)

    db = "movesdb20220105"
    engine = create_engine(f"mariadb+mariadbconnector://root:moves@127.0.0.1:3308/{db}")
    mvs303samvehpop_ = pd.read_sql(sql, con=engine)
