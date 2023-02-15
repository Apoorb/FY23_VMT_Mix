"""
Use the following data to impute missing data:
- MVS303 default runs
- MVS 3 samplevehiclepopulation and sourcetypeagedistribution distribution data
- 2018 vehicle registration data.
Created by: Apoorb
Created on: 02/14/2022
"""
import pandas as pd
import numpy as np
import pathlib
from pathlib import Path
import geopandas as gpd
import pyarrow.parquet as pq
import pyarrow as pa
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname("__file__"), "..")))
from vmtmix_fy23.utils import (
    path_prj_code,
    ChainedAssignent,
    path_interm,
    path_inp,
    path_county_shp,
    path_fig_dir,
    get_snake_case_dict,
    connect_to_server_db,
)


def get_mvs303samvehpop():
    conn = connect_to_server_db(database_nm="movesdb20220105")
    with conn:
        sql = "SELECT * FROM samplevehiclepopulation"
        mvs303samvehpop_ = pd.read_sql(sql, con=conn)
    mvs303samvehpop_agg_ = mvs303samvehpop_.groupby(
        ["sourceTypeID", "modelYearID", "fuelTypeID"], as_index=False
    ).stmyFraction.sum()
    assert np.allclose(
        mvs303samvehpop_agg_.groupby(
            ["sourceTypeID", "modelYearID"]
        ).stmyFraction.sum(),
        1,
    )
    return mvs303samvehpop_agg_


def get_mvs303souagedist(anlyr_):
    conn = connect_to_server_db(database_nm="movesdb20220105")
    with conn:
        sql = "SELECT * FROM sourcetypeagedistribution"
        mvs303souagedis_ = pd.read_sql(sql, con=conn)
    mvs303souagedis_fil_ = mvs303souagedis_.loc[mvs303souagedis_.yearID.isin(anlyr_)]
    return mvs303souagedis_fil_


def get_mvs303defaultsutdist():
    conn = connect_to_server_db(database_nm="mvs303_1990_2000to2060_splits_out")
    with conn:
        sql = "SELECT * FROM movesactivityoutput"
        mvs303defact_ = pd.read_sql(sql, con=conn)
    conn = connect_to_server_db(database_nm="movesdb20220105")
    cur = conn.cursor()
    cur.execute("SHOW TABLES")
    cur.fetchall()
    with conn:
        sql = "SELECT * FROM sourceusetype"
        sourceusetype_ = pd.read_sql(sql, con=conn)
        sql = "SELECT * FROM hpmsvtype"
        hpmsvtype_ = pd.read_sql(sql, con=conn)
        sourceusetype_1_ = sourceusetype_.merge(
            hpmsvtype_, on=["HPMSVtypeID"], how="left"
        )

    assert all(
        mvs303defact_.groupby(
            ["yearID", "roadTypeID", "sourceTypeID", "activityTypeID"]
        ).activity.count()
        == 1
    )
    mvs303defact_hpms_ = (
        mvs303defact_.loc[lambda df: df.activityTypeID == 1]
        .merge(sourceusetype_1_, on="sourceTypeID", how="left")
        .sort_values(["HPMSVtypeID", "yearID", "roadTypeID", "sourceTypeID"])
        .filter(
            items=[
                "yearID",
                "roadTypeID",
                "HPMSVtypeName",
                "sourceTypeName",
                "HPMSVtypeID",
                "sourceTypeID",
                "activityTypeID",
                "activity",
            ]
        )
        .assign(
            hpms_activity=lambda df: (
                df.groupby(
                    [
                        "yearID",
                        "roadTypeID",
                        "HPMSVtypeName",
                        "HPMSVtypeID",
                        "activityTypeID",
                    ]
                )
            ).activity.transform(sum),
            activity_frac_hpms=lambda df: df.activity / df.hpms_activity,
        )
    )

    mvs303defact_hpms_1_ = mvs303defact_hpms_.copy(deep=True)
    mvs303defact_hpms_1_.sourceTypeID.unique()

    mvs303defact_hpms_1_.sourceTypeName.unique()
    mvs303defact_hpms_1_.loc[
        lambda df: df.sourceTypeName.isin(
            ["Single Unit Short-haul Truck", "Single Unit Long-haul Truck"]
        ),
        "sourceTypeName",
    ] = "Single Unit Truck"
    mvs303defact_hpms_1_.loc[
        lambda df: df.sourceTypeName == "Single Unit Truck", "sourceTypeID"
    ] = 5253
    mvs303defact_hpms_2_ = mvs303defact_hpms_1_.groupby(
        [
            "yearID",
            "roadTypeID",
            "HPMSVtypeName",
            "sourceTypeName",
            "HPMSVtypeID",
            "sourceTypeID",
            "activityTypeID",
        ],
        as_index=False,
    ).activity.sum()

    fil_suts = [
        "Passenger Truck",
        "Light Commercial Truck",
        "Other Buses",
        "Transit Bus",
        "School Bus",
        "Refuse Truck",
        "Single Unit Truck",
        "Motor Home",
    ]

    mvs303defact_hpms_2_.HPMSVtypeName.unique()
    mvs303defact_hpms_2_fil_ = mvs303defact_hpms_2_.loc[
        lambda df: df.sourceTypeName.isin(fil_suts)
    ].assign(
        hpms_activity=lambda df: (
            df.groupby(
                [
                    "yearID",
                    "roadTypeID",
                    "HPMSVtypeName",
                    "HPMSVtypeID",
                    "activityTypeID",
                ]
            )
        ).activity.transform(sum),
        activity_frac_hpms=lambda df: df.activity / df.hpms_activity,
    )
    mvs303defact_hpms_2_fil_offroad_ = mvs303defact_hpms_2_fil_.groupby(
        [
            "yearID",
            "HPMSVtypeName",
            "sourceTypeName",
            "HPMSVtypeID",
            "sourceTypeID",
            "activityTypeID",
        ],
        as_index=False,
    ).agg(activity=("activity", "sum"), roadTypeID=("activity", lambda x: "ALL"))
    mvs303defact_hpms_2_fil_offroad_ = mvs303defact_hpms_2_fil_offroad_.assign(
        hpms_activity=lambda df: (
            df.groupby(
                [
                    "yearID",
                    "roadTypeID",
                    "HPMSVtypeName",
                    "HPMSVtypeID",
                    "activityTypeID",
                ]
            )
        ).activity.transform(sum),
        activity_frac_hpms=lambda df: df.activity / df.hpms_activity,
    )

    mvs303defact_relvnt = pd.concat(
        [mvs303defact_hpms_2_fil_, mvs303defact_hpms_2_fil_offroad_]
    )

    assert all(
        mvs303defact_relvnt.groupby(
            ["yearID", "roadTypeID", "HPMSVtypeName", "HPMSVtypeID", "activityTypeID"]
        ).activity_frac_hpms.sum()
        == 1
    )
    modhpms_vtype_name_map = {
        "Buses": "Buses",
        "Light Duty Vehicles": "PT_LCT",
        "Single Unit Trucks": "SU_MH_RT_HDV",
    }
    mvs303defact_relvnt["modhpms_vtype_name"] = mvs303defact_relvnt.HPMSVtypeName.map(
        modhpms_vtype_name_map
    )
    mvs303defact_relvnt = mvs303defact_relvnt.rename(
        columns={
            "activity_frac_hpms": "activity_frac_modhpms",
            "sourceTypeName": "modsutname",
        }
    ).filter(
        items=[
            "yearID",
            "roadTypeID",
            "modhpms_vtype_name",
            "modsutname",
            "activity_frac_modhpms",
        ]
    )
    return mvs303defact_relvnt


def get_mvs303fueldist():
    anlyr = [1990] + list(range(2000, 2065, 5))
    mvs303samvehpop = get_mvs303samvehpop()
    mvs303souagedis = get_mvs303souagedist(anlyr_=anlyr)
    conn = connect_to_server_db(database_nm="movesdb20220105")
    with conn:
        sql = "SELECT * FROM sourceusetype"
        sourceusetype_ = pd.read_sql(sql, con=conn)
        sql = "SELECT * FROM fueltype"
        fueltype = pd.read_sql(sql, con=conn)

    mvs303souagedis["modelYearID"] = (
        mvs303souagedis["yearID"] - mvs303souagedis["ageID"]
    )
    mvs303souagedis_fueldist = mvs303souagedis.merge(
        mvs303samvehpop, on=["sourceTypeID", "modelYearID"], how="left"
    )
    mvs303souagedis_fueldist["weighted_stmyFraction"] = (
        mvs303souagedis_fueldist.ageFraction * mvs303souagedis_fueldist.stmyFraction
    )
    mvs303souagedis_fueldist_agg = mvs303souagedis_fueldist.groupby(
        ["sourceTypeID", "fuelTypeID", "yearID"], as_index=False
    ).weighted_stmyFraction.sum()

    mvs303souagedis_fueldist_agg_debug = mvs303souagedis_fueldist.groupby(
        ["sourceTypeID", "yearID"], as_index=False
    ).weighted_stmyFraction.sum()
    assert np.allclose(mvs303souagedis_fueldist_agg_debug.weighted_stmyFraction, 1)

    mvs303souagedis_fueldist_agg_fil = (
        mvs303souagedis_fueldist_agg.loc[lambda df: df.fuelTypeID.isin([1, 2])]
        .filter(items=["sourceTypeID", "yearID", "fuelTypeID", "weighted_stmyFraction"])
        .sort_values(["sourceTypeID", "yearID", "fuelTypeID"])
        .merge(sourceusetype_, on="sourceTypeID", how="left")
        .merge(fueltype[["fuelTypeID", "fuelTypeDesc"]], on="fuelTypeID", how="left")
    )

    mvs303souagedis_fueldist_agg_fil[
        "norm_weighted_stmyFraction"
    ] = mvs303souagedis_fueldist_agg_fil.groupby(
        ["sourceTypeID", "yearID"]
    ).weighted_stmyFraction.transform(
        sum
    )

    mvs303souagedis_fueldist_agg_fil["weighted_stmyFraction_1"] = (
        mvs303souagedis_fueldist_agg_fil.weighted_stmyFraction
        / mvs303souagedis_fueldist_agg_fil.norm_weighted_stmyFraction
    )
    mvs303souagedis_fueldist_agg_fil = mvs303souagedis_fueldist_agg_fil.drop(
        columns=["weighted_stmyFraction", "norm_weighted_stmyFraction"]
    )

    mvs303souagedis_fueldist_agg_fil_debug = mvs303souagedis_fueldist_agg_fil.groupby(
        ["sourceTypeID", "yearID"], as_index=False
    ).weighted_stmyFraction_1.sum()
    assert np.allclose(
        mvs303souagedis_fueldist_agg_fil_debug.weighted_stmyFraction_1, 1
    )
    return mvs303souagedis_fueldist_agg_fil


def main():
    mvs303fueldist = get_mvs303fueldist()
    path_mvs303fueldist = Path.joinpath(path_interm, "mvs303fueldist.csv")
    mvs303fueldist.to_csv(path_mvs303fueldist, index=False)
    mvs303defaultsutdist = get_mvs303defaultsutdist()
    path_mvs303defaultsutdist = Path.joinpath(path_interm, "mvs303defaultsutdist.csv")
    assert all(
        mvs303defaultsutdist.groupby(
            ["yearID", "roadTypeID", "modhpms_vtype_name"]
        ).activity_frac_modhpms.sum()
        == 1
    )
    mvs303defaultsutdist.to_csv(path_mvs303defaultsutdist, index=False)


if __name__ == "__main__":
    main()
    print(
        "----------------------------------------------------------------------------\n"
        "Finished Processing vi_fuel_mix.py\n"
        "----------------------------------------------------------------------------\n"
    )
