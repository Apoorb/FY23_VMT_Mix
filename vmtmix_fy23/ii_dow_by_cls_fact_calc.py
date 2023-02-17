"""
Use TxDOT Perm counter data to get DOW factors by different vehicle category.
Use the formula used by Tao to be f_d,m ATR factor to get this factor by different
vehicle categories.
Created by: Apoorb
Created/ Modified on: 02/14/2023
"""
from pathlib import Path
import pandas as pd
import numpy as np
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname("__file__"), "..")))
from vmtmix_fy23.utils import (
    ChainedAssignent,
    get_snake_case_dict,
    timing,
    path_inp,
    path_interm,
    path_txdot_fy22,
)

switchoff_chainedass_warn = ChainedAssignent()


def fun_region_episode(atr_data, episode_index, region_cat_name):
    """
    Function to calculate the adt of a given region (region_cat_name, e.g. district,
    MPO) and episode (episode_index, e.g., weekend, summer) the region_cat_name should
    a field in the df atr_data, episode_index is a column of logical variable whose
    length is the same as df atr_data
    """
    episode_atr_data = atr_data[episode_index]
    with switchoff_chainedass_warn:
        region_episode_atr_data = (
            episode_atr_data.groupby([region_cat_name]).mean().reset_index()
        )
    return region_episode_atr_data


def year_range_test(perm_countr_fil_):
    assert (
        (perm_countr_fil_.year.min() == 2013)
        and (perm_countr_fil_.year.max() == 2020)
        and (set(perm_countr_fil_.year.unique()) == set(range(2013, 2021)))
    ), (
        "The permanent counter years are not in the interval [2013, 2020] or missing "
        "some intermediate year."
    )


def unique_datetimes_test(perm_countr_fil_):
    assert all(
        perm_countr_fil_.groupby(
            ["sta_pre_id_suf_fr", "start_datetime"]
        ).start_datetime.nunique()
        == 1
    ), (
        "Duplicate timestamp found; there should not be duplicate timestamps for a "
        "station."
    )


def conv_aadt_adt_mnth_dow_by_vehcat(out_fi, min_yr=2013, max_yr=2019):
    """Convert AADT To monthly DOW ADT."""
    path_perm_countr_pq = Path.joinpath(
        path_txdot_fy22, "PERM_CLASS_BY_HR_2013_2021.parquet"
    )
    path_dgcode_map = Path.joinpath(path_inp, "district_dgcode_map.xlsx")
    perm_countr = pd.read_parquet(path_perm_countr_pq)
    perm_countr["year"] = perm_countr.start_datetime.dt.year
    perm_countr["mnth_nm"] = perm_countr.start_datetime.dt.month_name().str[:3]
    perm_countr["dow_nm"] = perm_countr.start_datetime.dt.day_name().str[:3]
    perm_countr["date_"] = perm_countr.start_datetime.dt.date
    map_ra = dict([(2, "ra"), (3, "un_ra"), (4, "ra"), (5, "un_ra")])
    perm_countr["ra"] = perm_countr.mvs_rdtype.map(map_ra)
    perm_countr_fil = perm_countr.loc[
        (perm_countr.year <= max_yr) & (perm_countr.year >= min_yr)
    ].reset_index(drop=True)

    # ----------------------------------------------------------------------------------
    # Address sample size issue.
    # ----------------------------------------------------------------------------------
    a = 1
    # ----------------------------------------------------------------------------------
    # Get DOW Factors by Vehicle Class.
    # ----------------------------------------------------------------------------------
    vehclscntcols = {
        "class1": "MC",
        "class2": "PC",
        "class3": "PT_LCT",
        "class4": "Bus",
        "class5": "HDV",
        "class6": "HDV",
        "class7": "HDV",
        "class8": "HDV",
        "class9": "HDV",
        "class10": "HDV",
        "class11": "HDV",
        "class12": "HDV",
        "class13": "HDV",
        "class14": "HDV",
        "class15": "HDV",
    }
    mc_cols = [key for key, val in vehclscntcols.items() if val == "MC"]
    pc_cols = [key for key, val in vehclscntcols.items() if val == "PC"]
    pt_lct_cols = [key for key, val in vehclscntcols.items() if val == "PT_LCT"]
    bus_cols = [key for key, val in vehclscntcols.items() if val == "Bus"]
    hdv_cols = [key for key, val in vehclscntcols.items() if val == "HDV"]
    with switchoff_chainedass_warn:
        perm_countr_fil["MC"] = perm_countr_fil[mc_cols].sum(axis=1)
    perm_countr_fil["PC"] = perm_countr_fil[pc_cols].sum(axis=1)
    perm_countr_fil["PT_LCT"] = perm_countr_fil[pt_lct_cols].sum(axis=1)
    perm_countr_fil["Bus"] = perm_countr_fil[bus_cols].sum(axis=1)
    perm_countr_fil["HDV"] = perm_countr_fil[hdv_cols].sum(axis=1)
    agg_vtype_cols = ["MC", "PC", "PT_LCT", "Bus", "HDV"]
    # generate indices
    # all records included
    # define the region for which the factors will be generated
    Selected_region = (
        "district"  # the region needs to be the same as the field name in the ATR data
    )
    perm_countr_fil.groupby("district").sta_pre_id_suf_fr.nunique()
    atr_db_all = perm_countr_fil.groupby(
        ["sta_pre_id_suf_fr", "district", "mvs_rdtype", "date_", "mnth_nm", "dow_nm"],
        as_index=False,
    )[agg_vtype_cols].sum()
    atr_db_all["Total"] = atr_db_all[agg_vtype_cols].sum(axis=1)
    atr_db_all["pct_hdv"] = atr_db_all.HDV / atr_db_all.Total
    # XXX: Zero MC and Buses in a day seems reasonable. Need more investigation if we
    # need more thorough anawer.
    zero_mask = (atr_db_all[["PC", "PT_LCT", "HDV"]] == 0).any(axis=1)
    atr_db_all_fil = atr_db_all[
        ["district", "mnth_nm", "dow_nm"] + agg_vtype_cols + ["Total"]
    ]
    assert all(atr_db_all_fil.isna().sum(axis=0).values == 0)
    nonzero_index = ~zero_mask
    # ----------------------------------------------------------------------------------
    # Get AADT
    # ----------------------------------------------------------------------------------
    aadt = fun_region_episode(atr_db_all_fil, nonzero_index, Selected_region)
    aadt_debug = aadt.copy()
    aadt_debug = aadt_debug.assign(
        pct_mc=lambda df: np.round(df.MC / df.Total),
        pct_pc=lambda df: np.round(df.PC / df.Total),
        pct_pt_lct=lambda df: np.round(df.PT_LCT / df.Total),
        pct_bus=lambda df: np.round(df.Bus / df.Total),
        pct_hdv=lambda df: np.round(df.HDV / df.Total),
    )
    # ----------------------------------------------------------------------------------
    # AERR20_Days
    # produce ratios to split traffic between weekday and weekend for each month. AADT
    # won't be used here
    # ----------------------------------------------------------------------------------
    map_dow = {
        "Mon": "Wkd",
        "Tue": "Wkd",
        "Wed": "Wkd",
        "Thu": "Wkd",
        "Fri": "Fri",
        "Sat": "Sat",
        "Sun": "Sun",
    }
    with switchoff_chainedass_warn:
        atr_db_all_fil["dowagg"] = atr_db_all_fil.dow_nm.map(map_dow)
        dow_adt = atr_db_all_fil.groupby([Selected_region, "dowagg"]).mean().reset_index()
    df_adt = aadt.merge(
        dow_adt,
        how="left",
        left_on=[Selected_region],
        right_on=[Selected_region],
        suffixes=("", "_dow"),
    )
    with switchoff_chainedass_warn:
        df_adt["f_m_d_MC"] = df_adt.MC_dow / df_adt.MC
        df_adt["f_m_d_PC"] = df_adt.PC_dow / df_adt.PC
        df_adt["f_m_d_PT_LCT"] = df_adt.PT_LCT_dow / df_adt.PT_LCT
        df_adt["f_m_d_Bus"] = df_adt.Bus_dow / df_adt.Bus
        df_adt["f_m_d_HDV"] = df_adt.HDV_dow / df_adt.HDV
        df_adt["f_m_d_Total"] = df_adt.Total_dow / df_adt.Total
    # with pd.option_context("display.max_columns", 16):
    #     print(df_adt.describe())
    df_adt.to_csv(
        Path.joinpath(path_interm, out_fi), sep="\t", index=False
    )


@timing
def dow_by_cls_fac(out_fi, min_yr, max_yr):
    """
    Create DOW by veh class factors that will be applied to the AADT from ATR data
    by vehicle class.
    """
    conv_aadt_adt_mnth_dow_by_vehcat(out_fi=out_fi, min_yr=min_yr, max_yr=max_yr)


if __name__ == "__main__":
    dow_by_cls_fac(out_fi="conv_aadt2dow_by_vehcat.tab", min_yr=2013, max_yr=2019)
    print(
        "----------------------------------------------------------------------------\n"
        "Finished Processing iv_dow_fac_vehcat.py\n"
        "----------------------------------------------------------------------------\n"
    )
    # path_conv_aadt2dow_by_vehcat = Path.joinpath(
    #     path_interm, "conv_aadt2dow_by_vehcat.tab"
    # )
    # conv_aadt2dow_by_vehcat = pd.read_csv(path_conv_aadt2dow_by_vehcat, sep="\t")
