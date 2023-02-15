"""
Use FAF and MOVES national level run VMT Mix to get the dissagregated VMT-Mix
"""
import datetime
import numpy as np
import pandas as pd
from pathlib import Path
import geopandas as gpd
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname("__file__"), "..")))
from vmtmix_fy23.utils import (
    path_interm,
    path_output,
    path_txdot_districts_shp,
    ChainedAssignent,
)

switchoff_chainedass_warn = ChainedAssignent()


def prc_mvc(mvc_vmtmix_):
    mvc_vmtmix_long_ = mvc_vmtmix_.melt(
        id_vars=[
            "dgcode",
            "district",
            "based_on_dg",
            "mvs_rdtype_nm",
            "mvs_rdtype",
            "dowagg",
            "hour",
        ],
        value_vars=[
            "MC_dow",
            "PC_dow",
            "PT_LCT_dow",
            "Bus_dow",
            "SU_MH_RT_HDV_dow",
            "CT_HDV_dow",
        ],
        var_name="mvc_vtype_cat",
        value_name="mvc_vtype_cat_dow_adt",
    )

    mvc_vmtmix_long_["mvc_vtype_cat"] = mvc_vmtmix_long_.mvc_vtype_cat.str.split(
        "_dow", expand=True
    )[0]
    return mvc_vmtmix_long_


def add_yr_mod_cols_mvc(mvc_vmtmix_long_filt_, mvs303defaultsutdist_):
    """Process the mvc vehicle types that were not merged with national default data."""
    yearIDs = [
        1990,
        2000,
        2005,
        2010,
        2015,
        2020,
        2025,
        2030,
        2035,
        2040,
        2045,
        2050,
        2055,
        2060,
    ]
    assert set(mvs303defaultsutdist_.yearID.unique()) == set(yearIDs)
    with switchoff_chainedass_warn:
        mvc_vmtmix_long_filt_["yearID"] = [yearIDs] * len(mvc_vmtmix_long_filt_)
        mvc_vmtmix_long_filt_1_ = mvc_vmtmix_long_filt_.explode("yearID")
    mvc_vmtmix_long_filt_1_ = mvc_vmtmix_long_filt_1_.rename(
        columns={
            "mvc_vtype_cat": "modsutname",
            "mvc_vtype_cat_dow_adt": "modsut_vmt_est",
        }
    )
    mvc_vmtmix_long_filt_2_ = mvc_vmtmix_long_filt_1_.filter(
        items=[
            "dgcode",
            "district",
            "based_on_dg",
            "mvs_rdtype_nm",
            "mvs_rdtype",
            "dowagg",
            "hour",
            "yearID",
            "modsutname",
            "modsut_vmt_est",
        ]
    )
    return mvc_vmtmix_long_filt_2_


def prc_faf4_fac(faf4_su_ct_lh_sh_pct_):
    """Give factors back by SU and CT"""
    faf4_su_ct_lh_sh_pct_long_ = faf4_su_ct_lh_sh_pct_.melt(
        id_vars=["mvs_rdtype"],
        value_vars=[
            "pct_CLhT_vs_CT",
            "pct_CShT_vs_CT",
            "pct_SULhT_vs_SU",
            "pct_SUShT_vs_SU",
        ],
        var_name="su_ct_sh_lh",
        value_name="su_ct_sh_lh_pcts",
    )
    faf4_su_ct_lh_sh_pct_long_[
        "su_ct_sh_lh"
    ] = faf4_su_ct_lh_sh_pct_long_.su_ct_sh_lh.str.split("pct_", expand=True)[1]

    faf4_su_ct_lh_sh_pct_long_[
        ["sut_abb", "mvc_vtype_cat_abb"]
    ] = faf4_su_ct_lh_sh_pct_long_["su_ct_sh_lh"].str.split("_vs_", expand=True)

    map_suct = {
        old: new
        for old, new in zip(
            ["SUShT", "SULhT", "CShT", "CLhT"],
            [
                "Single Unit Short-haul Truck",
                "Single Unit Long-haul Truck",
                "Combination Short-haul Truck",
                "Combination Long-haul Truck",
            ],
        )
    }
    map_modhpms = {
        old: new for old, new in zip(["SU", "CT"], ["Single Unit Truck", "CT_HDV"])
    }
    faf4_su_ct_lh_sh_pct_long_[
        "sourceTypeName"
    ] = faf4_su_ct_lh_sh_pct_long_.sut_abb.map(map_suct)
    faf4_su_ct_lh_sh_pct_long_[
        "modsutname"
    ] = faf4_su_ct_lh_sh_pct_long_.mvc_vtype_cat_abb.map(map_modhpms)

    faf4_filt = faf4_su_ct_lh_sh_pct_long_.filter(
        items=["mvs_rdtype", "modsutname", "sourceTypeName", "su_ct_sh_lh_pcts"]
    )
    return faf4_filt


def fac_natdef(mvc_, mvc_vtype_cat, mvs303defaultsutdist_, modhpmsvehcat):
    filt_mvc = (
        lambda df: df[list(mvc_vtype_cat.keys())[0]] == list(mvc_vtype_cat.values())[0]
    )
    filt_mvsdef = (
        lambda df: df[list(modhpmsvehcat.keys())[0]] == list(modhpmsvehcat.values())[0]
    )

    map_mvc_mvsdef_cat = {
        list(modhpmsvehcat.values())[0]: list(mvc_vtype_cat.values())[0]
    }
    mvs303defaultsutdist_filt = mvs303defaultsutdist_[filt_mvsdef]
    with switchoff_chainedass_warn:
        mvs303defaultsutdist_filt.loc[
            filt_mvsdef, list(mvc_vtype_cat.keys())[0]
        ] = mvs303defaultsutdist_filt.loc[
            filt_mvsdef, list(modhpmsvehcat.keys())[0]
        ].map(
            map_mvc_mvsdef_cat
        )
    assert set(mvs303defaultsutdist_filt.roadTypeID.unique()) == set(
        mvc_.mvs_rdtype.unique()
    )
    mvs303defaultsutdist_filt = mvs303defaultsutdist_filt.rename(
        columns={"roadTypeID": "mvs_rdtype"}
    )

    mvc_mvssut_filt_ = mvc_[filt_mvc].merge(
        mvs303defaultsutdist_filt,
        on=[list(mvc_vtype_cat.keys())[0], "mvs_rdtype"],
        how="left",
    )
    assert ~all(mvc_mvssut_filt_["activity_frac_modhpms"].isna())
    mvc_mvssut_filt_["modsut_vmt_est"] = (
        mvc_mvssut_filt_.mvc_vtype_cat_dow_adt * mvc_mvssut_filt_.activity_frac_modhpms
    )
    mvc_mvssut_filt_1_ = mvc_mvssut_filt_.filter(
        items=[
            "dgcode",
            "district",
            "based_on_dg",
            "mvs_rdtype_nm",
            "mvs_rdtype",
            "dowagg",
            "hour",
            "yearID",
            "modsutname",
            "modsut_vmt_est",
        ]
    )
    return mvc_mvssut_filt_1_


def apply_faf4_fac(mvc_su_ct_, faf4_fac_):
    fix_dtype = {
        float_: int_
        for float_, int_ in zip(
            ["2.0", "3.0", "4.0", "5.0", "ALL"], ["2", "3", "4", "5", "ALL"]
        )
    }
    faf4_fac_["mvs_rdtype"] = faf4_fac_.mvs_rdtype.map(fix_dtype)
    assert set(mvc_su_ct_.mvs_rdtype.unique()) == set(faf4_fac_.mvs_rdtype.unique())
    mvc_su_ct_haul = mvc_su_ct_.merge(
        faf4_fac_, on=["mvs_rdtype", "modsutname"], how="left"
    )
    mvc_su_ct_haul["sut_vmt_est"] = (
        mvc_su_ct_haul.modsut_vmt_est * mvc_su_ct_haul.su_ct_sh_lh_pcts
    )
    mvc_su_ct_haul.filter(
        items=[
            "dgcode",
            "district",
            "based_on_dg",
            "mvs_rdtype_nm",
            "mvs_rdtype",
            "dowagg",
            "hour",
            "yearID",
            "modsutname",
            "modsut_vmt_est",
            "sourceTypeName",
            "su_ct_sh_lh_pcts",
            "sut_vmt_est",
        ]
    )
    return mvc_su_ct_haul


def concat_suts(
    mvc_mc_pc, mvc_sut_pt_lct, mvc_sut_ob_sb_tb, mvc_modsut_rt_mh, mvc_su_ct_sut
):
    mvc_mc_pc_pt_lct_ob_sb_tb_rt_mh = pd.concat(
        [mvc_mc_pc, mvc_sut_pt_lct, mvc_sut_ob_sb_tb, mvc_modsut_rt_mh]
    )

    sut_nm_map = dict(
        zip(
            [
                "MC",
                "PC",
                "Passenger Truck",
                "Light Commercial Truck",
                "Other Buses",
                "Transit Bus",
                "School Bus",
                "Refuse Truck",
                "Motor Home",
            ],
            [
                "Motorcycle",
                "Passenger Car",
                "Passenger Truck",
                "Light Commercial Truck",
                "Other Buses",
                "Transit Bus",
                "School Bus",
                "Refuse Truck",
                "Motor Home",
            ],
        )
    )
    mvc_mc_pc_pt_lct_ob_sb_tb_rt_mh = mvc_mc_pc_pt_lct_ob_sb_tb_rt_mh.assign(
        sourceTypeName=lambda df: df.modsutname.map(sut_nm_map),
        su_ct_sh_lh_pcts=np.nan,
        sut_vmt_est=lambda df: df.modsut_vmt_est,
    )

    mvc_mc_pc_pt_lct_ob_sb_tb_rt_mh_su_ct_shlh = pd.concat(
        [mvc_mc_pc_pt_lct_ob_sb_tb_rt_mh, mvc_su_ct_sut]
    )
    mvc_suts_ = mvc_mc_pc_pt_lct_ob_sb_tb_rt_mh_su_ct_shlh.copy(deep=True)
    return mvc_suts_


def apply_fuel_dist(mvc_suts_, mvs303fueldist_):
    assert set(mvc_suts_.sourceTypeName) == set(mvs303fueldist_.sourceTypeName)
    mvc_suts_ftype = mvc_suts_.merge(
        mvs303fueldist_, on=["yearID", "sourceTypeName"], how="left"
    )
    mvc_suts_ftype.columns
    mvc_suts_ftype["sut_ftype_vmt_est"] = (
        mvc_suts_ftype.sut_vmt_est * mvc_suts_ftype.weighted_stmyFraction_1
    )
    assert ~all(mvc_suts_ftype.sut_ftype_vmt_est.isna())
    mvc_suts_ftype_debug = mvc_suts_ftype.groupby(
        ["district", "mvs_rdtype_nm", "dowagg", "sourceTypeName", "fuelTypeDesc"],
        as_index=False,
    ).agg(hour_set=("hour", set), yearID_set=("yearID", set))
    mvc_suts_ftype_debug[["sourceTypeName", "fuelTypeDesc"]].drop_duplicates()
    assert all(mvc_suts_ftype_debug.hour_set == set(range(0, 24)))
    assert all(
        mvc_suts_ftype_debug.yearID_set
        == set((1990, 2000, 2005)) | set(range(2010, 2065, 5))
    )
    assert (
        len(mvc_suts_ftype_debug) == 25 * 5 * 4 * 24
    )  # 25 districts # 5 road types (ALL
    # included) # 4 dow # 24 SUT+Ftype
    return mvc_suts_ftype


def filt_to_tod(mvc_suts_ftype_, tod_map_, txdist_):
    tod_lng_map = {}
    for key, vals in tod_map_.items():
        for val in vals:
            tod_lng_map[val] = key
    mvc_suts_ftype_["tod"] = mvc_suts_ftype_.hour.map(tod_lng_map)

    mvc_suts_ftype_day = mvc_suts_ftype_.copy(deep=True)
    mvc_suts_ftype_day["tod"] = "day"
    mvc_suts_ftype_tod_ = pd.concat([mvc_suts_ftype_, mvc_suts_ftype_day])

    mvc_suts_ftype_tod_agg_ = mvc_suts_ftype_tod_.groupby(
        [
            "dgcode",
            "district",
            "mvs_rdtype_nm",
            "mvs_rdtype",
            "dowagg",
            "yearID",
            "sourceTypeName",
            "sourceTypeID",
            "fuelTypeID",
            "fuelTypeDesc",
            "tod",
        ],
        as_index=False,
    ).agg(sut_ftype_tod_vmt_est=("sut_ftype_vmt_est", "sum"))

    mvc_suts_ftype_tod_agg_["tod_vmt_est"] = mvc_suts_ftype_tod_agg_.groupby(
        ["dgcode", "district", "mvs_rdtype_nm", "mvs_rdtype", "dowagg", "yearID", "tod"]
    ).sut_ftype_tod_vmt_est.transform(sum)

    mvc_suts_ftype_tod_agg_["vmt_mix"] = (
        mvc_suts_ftype_tod_agg_.sut_ftype_tod_vmt_est
        / mvc_suts_ftype_tod_agg_.tod_vmt_est
    )
    assert np.allclose(
        mvc_suts_ftype_tod_agg_.groupby(
            [
                "dgcode",
                "district",
                "mvs_rdtype_nm",
                "mvs_rdtype",
                "dowagg",
                "yearID",
                "tod",
            ]
        ).vmt_mix.sum(),
        1,
    )

    mvc_suts_ftype_tod_agg_ = mvc_suts_ftype_tod_agg_.merge(
        txdist_, on="district", how="left"
    ).filter(
        items=[
            "dgcode",
            "txdot_dist",
            "district",
            "mvs_rdtype_nm",
            "mvs_rdtype",
            "dowagg",
            "yearID",
            "tod",
            "sourceTypeName",
            "sourceTypeID",
            "fuelTypeID",
            "fuelTypeDesc",
            "vmt_mix",
        ]
    )
    return mvc_suts_ftype_tod_agg_


def main():
    now_yr = str(datetime.datetime.now().year)
    now_mnt = str(datetime.datetime.now().month).zfill(2)
    now_mntyr = now_mnt + now_yr
    # Set path
    path_mvc_vmtmix = list(path_output.glob("mvc_vmtmix_*.csv"))[0]
    path_faf4_su_ct_lh_sh_pct = Path.joinpath(path_interm, "faf4_su_ct_lh_sh_pct.tab")
    path_mvs303defaultsutdist = Path.joinpath(path_interm, "mvs303defaultsutdist.csv")
    path_mvs303fueldist = Path.joinpath(path_interm, "mvs303fueldist.csv")
    path_fin_vmtmix = Path.joinpath(path_output, f"fin_vmtmix_{now_mntyr}.csv")
    # Read Data
    mvc_vmtmix = pd.read_csv(path_mvc_vmtmix)
    faf4_su_ct_lh_sh_pct = pd.read_csv(path_faf4_su_ct_lh_sh_pct, sep="\t")
    mvs303defaultsutdist = pd.read_csv(path_mvs303defaultsutdist)
    mvs303fueldist = pd.read_csv(path_mvs303fueldist)
    txdist_tmp = gpd.read_file(path_txdot_districts_shp)
    txdist = txdist_tmp[["DIST_NBR", "DIST_NM"]].rename(
        columns={"DIST_NBR": "txdot_dist", "DIST_NM": "district"}
    )
    # Process Data
    mvc_vmtest_long = prc_mvc(mvc_vmtmix_=mvc_vmtmix)
    mvc_sut_pt_lct = fac_natdef(
        mvc_=mvc_vmtest_long,
        mvc_vtype_cat={"mvc_vtype_cat": "PT_LCT"},
        mvs303defaultsutdist_=mvs303defaultsutdist,
        modhpmsvehcat={"modhpms_vtype_name": "PT_LCT"},
    )
    mvc_sut_ob_sb_tb = fac_natdef(
        mvc_=mvc_vmtest_long,
        mvc_vtype_cat={"mvc_vtype_cat": "Bus"},
        mvs303defaultsutdist_=mvs303defaultsutdist,
        modhpmsvehcat={"modhpms_vtype_name": "Buses"},
    )
    mvc_modsut_su_rt_mh = fac_natdef(
        mvc_=mvc_vmtest_long,
        mvc_vtype_cat={"mvc_vtype_cat": "SU_MH_RT_HDV"},
        mvs303defaultsutdist_=mvs303defaultsutdist,
        modhpmsvehcat={"modhpms_vtype_name": "SU_MH_RT_HDV"},
    )
    # Process remaining mvc vehicle categories.
    mvc_vmtmix_long_filt = mvc_vmtest_long.loc[
        lambda df: df.mvc_vtype_cat.isin(["MC", "PC", "CT_HDV"])
    ]
    mvc_mc_pc_ct = add_yr_mod_cols_mvc(
        mvc_vmtmix_long_filt_=mvc_vmtmix_long_filt,
        mvs303defaultsutdist_=mvs303defaultsutdist,
    )

    # Long haul vs. Short haul using FAF4
    mvc_su = mvc_modsut_su_rt_mh.loc[lambda df: df.modsutname == "Single Unit Truck"]
    mvc_ct = mvc_mc_pc_ct[lambda df: df.modsutname == "CT_HDV"]
    mvc_su_ct = pd.concat([mvc_su, mvc_ct])
    faf4_fac = prc_faf4_fac(faf4_su_ct_lh_sh_pct_=faf4_su_ct_lh_sh_pct)
    mvc_su_ct_sut = apply_faf4_fac(mvc_su_ct_=mvc_su_ct, faf4_fac_=faf4_fac)

    # Concat data
    mvc_modsut_rt_mh = mvc_modsut_su_rt_mh.loc[
        lambda df: df.modsutname.isin(["Motor Home", "Refuse Truck"])
    ]
    mvc_mc_pc = mvc_mc_pc_ct[lambda df: df.modsutname.isin(["MC", "PC"])]
    mvc_suts = concat_suts(
        mvc_mc_pc=mvc_mc_pc,
        mvc_sut_pt_lct=mvc_sut_pt_lct,
        mvc_sut_ob_sb_tb=mvc_sut_ob_sb_tb,
        mvc_modsut_rt_mh=mvc_modsut_rt_mh,
        mvc_su_ct_sut=mvc_su_ct_sut,
    )
    # Apply Fuel Fractions
    mvc_suts_ftype = apply_fuel_dist(mvc_suts_=mvc_suts, mvs303fueldist_=mvs303fueldist)
    mvc_suts_ftype.sut_ftype_vmt_est.describe()
    tod_map = {
        "AM": (6, 7, 8),
        "MD": (9, 10, 11, 12, 13, 14, 15),
        "PM": (16, 17, 18),
        "ON": (19, 20, 21, 22, 23, 0, 1, 2, 3, 4, 5),
    }
    from itertools import chain

    hours_ = list(chain(*tod_map.values()))
    hours_.sort()
    assert (set(hours_) == set(mvc_suts_ftype.hour)) & (
        len(hours_) == len(set(mvc_suts_ftype.hour))
    )
    assert len(set(mvc_suts_ftype.district)) == 25
    mvc_suts_ftype_tod = filt_to_tod(
        mvc_suts_ftype_=mvc_suts_ftype, tod_map_=tod_map, txdist_=txdist
    )
    assert len(set(mvc_suts_ftype_tod.district)) == 25
    mvc_suts_ftype_tod = mvc_suts_ftype_tod.sort_values(
        [
            "district",
            "yearID",
            "dowagg",
            "tod",
            "mvs_rdtype_nm",
            "sourceTypeID",
            "fuelTypeID",
        ]
    ).reset_index(drop=True)
    mvc_suts_ftype_tod.to_csv(path_fin_vmtmix, index=False)


if __name__ == "__main__":
    main()
    print(
        "----------------------------------------------------------------------------\n"
        "Finished Processing vii_vmt_mix_disagg.py\n"
        "----------------------------------------------------------------------------\n"
    )
