"""
Pre-process the data received from TxDOT. This data contains MVC and PERM counter data.
Check layers in STAR II (1^) to get more info.
1^ https://txdot.public.ms2soft.com/tcds/tsearch.asp?loc=Txdot&mod=TCDS
Created by: Apoorb
Created/ Modified on: 02/14/2023
"""
import pandas as pd
import numpy as np
from pathlib import Path
import geopandas as gpd
import pyarrow.parquet as pq
import pyarrow as pa
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname("__file__"), "..")))
from vmtmix_fy23.utils import (
    ChainedAssignent,
    path_txdot_fy22,
    path_county_shp,
    get_snake_case_dict,
    timing
)

switchoff_chainedass_warn = ChainedAssignent()


def clean_perm_countr() -> pd.DataFrame:
    """
    Clean and preprocess the PERM_CLASS_BY_HR_2013_2021 dataset, which contains hourly
    count data from the Texas Department of Transportation. The
    function performs the following operations on the dataset:

    - Reads the CSV file into a pandas DataFrame
    - Renames the columns of the DataFrame to snake_case
    - Asserts that the start_time column has a uniform format of 1900-01-01 HH:MM and that all hours from 0 to 23 are present in the column
    - Converts the start_date and start_time columns to a single datetime column named start_datetime
    - Drops the start_date and start_time columns
    - Converts the local_id and master_local_id columns to string type
    - Uses the get_sta_pre_id_suf_cmb() function to concatenate the station, precinct, ID, and suffix columns of the local_id column
    - Uses the add_mvs_rdtype_to_perm() function to add a vehicle type column to the DataFrame

    Returns
    ----------
    perm_countr_1: pd.DataFrame
        The cleaned and preprocessed DataFrame containing hourly count data for vehicle
        permits issued by the Texas Department of Transportation.
    """
    path_perm_countr_csv = Path.joinpath(
        path_txdot_fy22, "PERM_CLASS_BY_HR_2013_2021.csv"
    )
    perm_countr = pd.read_csv(path_perm_countr_csv)
    perm_countr.rename(columns=get_snake_case_dict(perm_countr), inplace=True)
    assert all(pd.to_datetime(perm_countr["start_time"]).dt.year.unique() == [1900])
    assert all(pd.to_datetime(perm_countr["start_time"]).dt.month.unique() == [1])
    assert all(pd.to_datetime(perm_countr["start_time"]).dt.day.unique() == [1])
    assert all(
        pd.to_datetime(perm_countr["start_time"]).dt.hour.unique() == list(range(0, 24))
    ), "Hours min is not 0, hour max in not 23, or some hours are missing."
    assert all(
        pd.to_datetime(perm_countr["start_time"]).dt.minute.unique() == [0, 15, 30, 45]
    )
    hours = (
        pd.to_datetime(perm_countr["start_time"])
        .dt.hour.astype(str)
        .str.pad(2, side="left", fillchar="0")
    )
    minutes = (
        pd.to_datetime(perm_countr["start_time"])
        .dt.minute.astype(str)
        .str.pad(2, side="left", fillchar="0")
    )
    perm_countr["start_datetime"] = pd.to_datetime(
        (perm_countr.start_date + " " + hours + ":" + minutes), format="%Y-%m-%d %H:%M"
    )
    perm_countr = perm_countr.drop(columns=["start_date", "start_time"])
    perm_countr[["local_id", "master_local_id"]] = perm_countr[
        ["local_id", "master_local_id"]
    ].astype(str)
    perm_countr["sta_pre_id_suf_fr"] = perm_countr["local_id"]
    perm_countr_1 = add_mvs_rdtype_to_perm(perm_countr)
    return perm_countr_1


def clean_mvc_countr(mvc_file):
    """
    The function clean_mvc_countr takes a file path mvc_file as input and returns a
    pandas dataframe after cleaning and processing. `mvc_file` contains the Manual
    Vehicle Count data from TxDOT. The function performs the following operations on the
    dataset:

    - The function expects the input file to be in excel format.
    - The function drops the 'start_date' and 'start_time' columns from the input file.
    - The 'latitude' and 'longitude' columns are converted to float type.
    - The 'end_date' column is converted to datetime format with format="%m/%d/%Y".
    - The 'start_datetime' column is created by combining 'start_date' and 'start_time' columns and converted to datetime format with format="%m/%d/%Y %I:%M:%S %p".
    - The 'location_id' column is created by combining values from the 'station_id', 'pre_dir', 'street', 'suf_dir', and 'cmb_dir' columns using the function 'get_sta_pre_id_suf_cmb'.

    Parameters
    ----------
    mvc_file: str
        The name of the file to read and clean.
    Returns
    -------
    mvc_countr_fil: pd.DataFrame
        A cleaned pandas dataframe containing information from the input file. The dataframe has the following columns:
        - latitude
        - longitude
        - end_date
        - start_datetime
        - location_id
    """
    path_mvc_countr = Path.joinpath(path_txdot_fy22, mvc_file)
    mvc_countr = pd.read_csv(path_mvc_countr)
    mvc_countr.rename(columns=get_snake_case_dict(mvc_countr), inplace=True)
    mvc_countr.longitude.astype(str).str.isnumeric().sum()
    garbage = mvc_countr.loc[lambda df: df.longitude == "--"]
    mvc_countr_fil = mvc_countr.loc[lambda df: ~(df.longitude == "--")]
    with switchoff_chainedass_warn:
        mvc_countr_fil[["latitude", "longitude"]] = mvc_countr_fil[
            ["latitude", "longitude"]
        ].astype(float)
        mvc_countr_fil["end_date"] = pd.to_datetime(
            mvc_countr_fil["end_date"], format="%m/%d/%Y"
        )
        mvc_countr_fil["start_datetime"] = pd.to_datetime(
            mvc_countr_fil.start_date + " " + mvc_countr_fil.start_time,
            format="%m/%d/%Y %I:%M:%S %p",
        )
    mvc_countr_fil = mvc_countr_fil.drop(columns=["start_date", "start_time"])
    mvc_countr_fil = get_sta_pre_id_suf_cmb(data_=mvc_countr_fil, sub_col="location_id")
    return mvc_countr_fil


def add_mvs_rdtype_to_mvc_new(mvc_countr_new_):
    """Add MOVES road type to the MVC data."""
    mvc_countr_new_ = mvc_countr_new_.assign(
        func_class=lambda df: df.func_class.astype(int),
        mvs_rdtype=lambda df: np.select(
            [
                (df.area_type.isin(["R"])) & (df.func_class.isin([1, 2])),
                (df.area_type.isin(["R"])) & (df.func_class.isin([3, 4, 5, 6, 7])),
                (df.area_type.isin(["U"])) & (df.func_class.isin([1, 2])),
                (df.area_type.isin(["U"])) & (df.func_class.isin([3, 4, 5, 6, 7])),
            ],
            [2, 3, 4, 5],
            np.nan,
        ),
    )
    return mvc_countr_new_


def add_mvs_rdtype_to_perm(perm_countr_):
    """Add MOVES road type to the ATR data."""
    perm_countr_ = perm_countr_.rename(
        columns={"functional_class": "func_class"}
    ).assign(
        func_class=lambda df: df.func_class.astype(int),
        mvs_rdtype=lambda df: np.select(
            [
                (df.rural_urban.isin(["R"])) & (df.func_class.isin([1, 2])),
                (df.rural_urban.isin(["R"])) & (df.func_class.isin([3, 4, 5, 6, 7])),
                (df.rural_urban.isin(["U"])) & (df.func_class.isin([1, 2])),
                (df.rural_urban.isin(["U"])) & (df.func_class.isin([3, 4, 5, 6, 7])),
            ],
            [2, 3, 4, 5],
            np.nan,
        ),
    )
    return perm_countr_


def save_raw_data_as_parquet(
    mvc_countr_,
    perm_countr_,
    mvc_out_fi,
    perm_out_fi
):
    """Clean the raw Permanent and MVC counter data save them as parquet for quick
    loading."""
    if mvc_countr_ is not None:
        table_mvc_countr = pa.Table.from_pandas(mvc_countr_, preserve_index=False)
        path_mvc_countr_pq = Path.joinpath(path_txdot_fy22, mvc_out_fi)
        pq.write_table(table_mvc_countr, path_mvc_countr_pq)

    if perm_countr_ is not None:
        table_perm_countr = pa.Table.from_pandas(perm_countr_, preserve_index=False)
        path_perm_countr_pq = Path.joinpath(path_txdot_fy22, perm_out_fi)
        pq.write_table(table_perm_countr, path_perm_countr_pq)


def get_sta_pre_id_suf_cmb(data_, sub_col):
    """Return concatenated station identifiers."""
    # FixMe: Only keep unique stations. If the data has "ALL", "EB", and "WB". 
    # Just keep "EB" and "WB".
    ################
    data_[["loc_id", "dir"]] = data_[sub_col].str.split("_", expand=True)
    data_["dir"] = data_["dir"].fillna("ALL")
    data_["year_"] = data_.start_datetime.dt.year
    stations = data_[["year_", "loc_id", "dir"]].drop_duplicates()
    stations.sort_values(["loc_id", "dir", "year_"], ignore_index=True, inplace=True)
    stations["has_ALL"] = stations.dir == 'ALL'
    stations["has_dir"] = stations.dir != 'ALL'
    stations_cnt = stations.groupby(
        ["year_", "loc_id"], as_index=False).agg(
            cnt_ALL=("has_ALL", "sum"), cnt_dir=("has_dir", "sum")
            ).sort_values(["loc_id", "year_"], ignore_index=True, inplace=False)
    
    len(stations_cnt)
            
            
    stations_cnt_1 = stations_cnt.loc[stations_cnt.cnt_ALL == 0]
    stations_cnt_2 = stations_cnt.loc[stations_cnt.cnt_dir == 0]
    stations_cnt_3 = stations_cnt.loc[
        (stations_cnt.cnt_ALL != 0) & (stations_cnt.cnt_dir != 0)]
    assert (
        len(stations_cnt) == len(stations_cnt_1) + len(stations_cnt_2) + len(stations_cnt_3)
        ), "The three sub-dataframes are not exclusive and exhaustive."
    assert (
        len(stations_cnt.loc[(stations_cnt.cnt_ALL == 0) & (stations_cnt.cnt_dir == 0)]) ==0
        ), "Either directional or total counts should be present."

    # The location ids + years in stations_cnt_1 and stations_cnt_2 are good as it has
    # either directional or total data, respectively.
    # The location ids + years in stations_cnt_3 need to handled, as 
    # Do some debuggin on PC counts for stations_cnt_3 location_id and year

    station_pivot = pd.pivot_table(
        data_, index=["year_", "loc_id"],
        values="class2", columns="dir",
        aggfunc=np.sum, fill_value=0
    ).reset_index()
    dir_cols = [col for col in station_pivot.columns if col not in["ALL", "year_"]]
    station_pivot["ALL_from_dir"] = station_pivot[dir_cols].sum(axis=1)
    station_pivot["Diff_Cnts"] = station_pivot.ALL - station_pivot.ALL_from_dir
    non_zero_loc_ids = stations_cnt_3.loc_id.unique()
    station_pivot_check = station_pivot.merge(stations_cnt_3, on=["loc_id", "year_"])
    assert len(station_pivot_check.loc[lambda df: df.Diff_Cnts > 0]) == 2, (
        "based on 2021 TxDOT data, only two instances should be there of Total PC (ALL)"
        " volume exceeding directional volume")
    # Handle duplicate stations.
    unq_sta_df = pd.concat([
        stations_cnt_1.filter(items=["year_", "loc_id"]).assign(use_ALL=False),
        stations_cnt_2.filter(items=["year_", "loc_id"]).assign(use_ALL=True),
        stations_cnt_3.filter(items=["year_", "loc_id"]).assign(use_ALL=False)
        ])

    data_["sta_pre_id_suf_fr"] = data_["loc_id"]
    data_["is_ALL"] = data_["dir"] == "ALL"
    data_1_ = data_.merge(unq_sta_df, on=["year_", "loc_id"])
    # keep_rows is XNOR gate.
    data_1_["keep_rows"] = data_1_.is_ALL == data_1_.use_ALL
    data_2_ = data_1_.loc[data_1_.keep_rows]
    
    assert (
        set(data_2_.loc_id.unique()).symmetric_difference(set(unq_sta_df.loc_id)) == set()
        ), "After above filtering some data was lost."
    return data_2_


@timing
def raw_dt_prc(
        MVC_file="MVC_2013_21_received_on_030922",
        PERM_file="PERM_CLASS_BY_HR_2013_2021"
):
    """
    Process the raw MVC and permanent counter data to fix date time format, station id,
    map road types to MOVES, and save data to parquet for faster loading.
    """
    # Set Paths
    # ----------------------------------------------------------------------------------
    path_perm_countr_pq = Path.joinpath(
        path_txdot_fy22, PERM_file + ".parquet"
    )
    path_mvc_countr_pq = Path.joinpath(
        path_txdot_fy22, MVC_file + ".parquet"
    )
    # Read Data
    # ----------------------------------------------------------------------------------
    # Read and Process County Data
    # -----------------------------
    gdf_county = gpd.read_file(path_county_shp)
    gdf_county = gdf_county.rename(columns=get_snake_case_dict(gdf_county))
    gdf_county_1 = gdf_county.filter(items=["txdot_dist", "cnty_nm"]).rename(
        columns={"cnty_nm": "county"}
    )
    # Read and Process MVC Data
    # --------------------------
    if not Path.exists(path_mvc_countr_pq):
        mvc_countr_fil = clean_mvc_countr(
            mvc_file=MVC_file + ".csv"
        )
        mvc_countr_fil = mvc_countr_fil.merge(gdf_county_1, on="county", how="left")
        mvc_countr_fil = add_mvs_rdtype_to_mvc_new(mvc_countr_fil)
        save_raw_data_as_parquet(
            mvc_countr_=mvc_countr_fil,
            perm_countr_=None,
            mvc_out_fi=MVC_file + ".parquet",
            perm_out_fi=None
        )
    # Read and Process ATR Data
    # --------------------------
    if not Path.exists(path_perm_countr_pq):
        perm_countr = clean_perm_countr()
        save_raw_data_as_parquet(
            mvc_countr_=None,
            perm_countr_=perm_countr,
            mvc_out_fi=None,
            perm_out_fi=PERM_file+".parquet")


if __name__ == "__main__":
    raw_dt_prc()
    print(
        "----------------------------------------------------------------------------\n"
        "Finished Processing i_raw_dt_prc.py\n"
        "----------------------------------------------------------------------------\n"
    )
    # # Read data
    # perm_countr = pq.read_table(path_perm_countr_pq).to_pandas()
    # mvc_countr = pq.read_table(path_mvc_countr_pq).to_pandas()
    # txdist_tmp = gpd.read_file(path_txdot_districts_shp)
    # txdist = txdist_tmp[["DIST_NBR", "DIST_NM"]].rename(
    #     columns={"DIST_NBR": "txdot_dist", "DIST_NM": "district"}
    # )
