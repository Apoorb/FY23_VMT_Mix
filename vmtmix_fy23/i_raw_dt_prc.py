"""
Explore the data received from TxDOT in Dec 2021. This data contains MVC and PERM
counter data. Check layers in STAR II (1^) to get more info.
1^ https://txdot.public.ms2soft.com/tcds/tsearch.asp?loc=Txdot&mod=TCDS
"""
import pandas as pd
import numpy as np
from pathlib import Path
import geopandas as gpd
import pyarrow.parquet as pq
import pyarrow as pa
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname("__file__"), "..")))
from vmtmix_fy23.utils import (
    ChainedAssignent,
    path_txdot_fy22,
    path_txdot_districts_shp,
    path_interm,
    path_county_shp,
    get_snake_case_dict,
)

switchoff_chainedass_warn = ChainedAssignent()


def clean_perm_countr():
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
    perm_countr = get_sta_pre_id_suf_cmb(data_=perm_countr, sub_col="local_id")
    perm_countr_1 = add_mvs_rdtype_to_perm(perm_countr)

    return perm_countr_1


def clean_mvc_countr(mvc_file):
    path_mvc_countr_xlsx = Path.joinpath(path_txdot_fy22, mvc_file)
    mvc_countr = pd.read_excel(path_mvc_countr_xlsx)
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


def add_mvs_rdtype_to_mvc(mvc_countr_):
    mvc_countr_ = mvc_countr_.assign(
        func_class=lambda df: df.func_class.astype(int),
        mvs_rdtype=lambda df: np.select(
            [
                (df.area_type.isin(["Rural"])) & (df.func_class.isin([1, 2])),
                (df.area_type.isin(["Rural"])) & (df.func_class.isin([3, 4, 5, 6, 7])),
                (df.area_type.isin(["Large Urban", "Small Urban", "Urbanized"]))
                & (df.func_class.isin([1, 2])),
                (df.area_type.isin(["Large Urban", "Small Urban", "Urbanized"]))
                & (df.func_class.isin([3, 4, 5, 6, 7])),
            ],
            [2, 3, 4, 5],
            np.nan,
        ),
    )
    return mvc_countr_


def add_mvs_rdtype_to_mvc_new(mvc_countr_new_):
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
    mvc_out_fi="updatedMVC 2013_2021.parquet",
    perm_out_fi="PERM_CLASS_BY_HR_2013_2021.parquet",
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
    ids_df = data_.loc[:, [sub_col]].drop_duplicates().reset_index(drop=True)
    ids_df[["sta_pre", "sta_id", "sta_suf_fr"]] = ids_df[sub_col].str.extract(
        r"(\D+)(\d{1,4})(\w*)", expand=True
    )
    ids_df["sta_id"] = ids_df.sta_id.fillna(ids_df[sub_col])
    ids_df["sta_pre"] = ids_df["sta_pre"].str.replace(" ", "")
    ids_df["fr_bool"] = ids_df["sta_suf_fr"].str.contains("FR")
    ids_df["fr"] = ids_df.fr_bool.map({True: "FR", False: ""})
    ids_df["sta_suf"] = ids_df["sta_suf_fr"].str.replace(" ", "").str.replace("FR", "")
    with switchoff_chainedass_warn:
        ids_df["sta_pre_id_suf_fr"] = (
            ids_df[["sta_pre", "sta_id", "sta_suf", "fr"]]
            .astype(str)
            .agg("-".join, axis=1)
        )
    data_1_ = data_.merge(ids_df, on=sub_col, how="left")
    return data_1_


def main():
    # Set Paths
    path_perm_countr_pq = Path.joinpath(
        path_txdot_fy22, "PERM_CLASS_BY_HR_2013_2021.parquet"
    )
    path_mvc_countr_pq = Path.joinpath(
        path_txdot_fy22, "MVC_2013_21_received_on_030922.parquet"
    )
    gdf_county = gpd.read_file(path_county_shp)
    gdf_county = gdf_county.rename(columns=get_snake_case_dict(gdf_county))
    gdf_county_1 = gdf_county.filter(items=["txdot_dist", "cnty_nm"]).rename(
        columns={"cnty_nm": "county"}
    )
    if not Path.exists(path_mvc_countr_pq):
        mvc_countr_fil = clean_mvc_countr(
            mvc_file="MVC_2013_21_received_on_030922.xlsx"
        )
        mvc_countr_fil = mvc_countr_fil.merge(gdf_county_1, on="county", how="left")
        mvc_countr_fil = add_mvs_rdtype_to_mvc_new(mvc_countr_fil)
        save_raw_data_as_parquet(
            mvc_countr_=mvc_countr_fil,
            perm_countr_=None,
            mvc_out_fi="MVC_2013_21_received_on_030922.parquet",
        )
    if not Path.exists(path_perm_countr_pq):
        perm_countr = clean_perm_countr()
        save_raw_data_as_parquet(mvc_countr_=None, perm_countr_=perm_countr)

    # Read data
    perm_countr = pq.read_table(path_perm_countr_pq).to_pandas()
    mvc_countr = pq.read_table(path_mvc_countr_pq).to_pandas()
    txdist_tmp = gpd.read_file(path_txdot_districts_shp)
    txdist = txdist_tmp[["DIST_NBR", "DIST_NM"]].rename(
        columns={"DIST_NBR": "txdot_dist", "DIST_NM": "district"}
    )


if __name__ == "__main__":
    main()
    print(
        "----------------------------------------------------------------------------\n"
        "Finished Processing i_raw_dt_prc.py\n"
        "----------------------------------------------------------------------------\n"
    )
