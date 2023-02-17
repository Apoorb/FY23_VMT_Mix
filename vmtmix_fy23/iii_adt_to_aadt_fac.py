"""
Use ATR factors from Tao's work. Following is modified Tao's code.
Created by: Tao
Modified by: Apoorb

"""
from pathlib import Path
import pandas as pd
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname("__file__"), "..")))
from vmtmix_fy23.utils import ChainedAssignent, timing, path_txdot_fy22, path_interm

switchoff_chainedass_warn = ChainedAssignent()


def fun_region_episode(atr_data, episode_index, region_cat_name):
    """
    Function to calculate the adt of a given region (region_cat_name, e.g. district,
    MPO) and episode (episode_index, e.g., weekend, summer) the region_cat_name should
    a field in the df atr_data, episode_index is a column of logical variable whose
    length is the same as df atr_data
    Parameters
    ----------
    atr_data
    episode_index
    region_cat_name

    Returns
    -------

    """
    episode_atr_data = atr_data[episode_index]
    region_episode_atr_data = (
        episode_atr_data.groupby([region_cat_name]).mean().reset_index()
    )
    return region_episode_atr_data


@timing
def conv_aadt_adt_mnth_dow(out_fi, min_yr=2013, max_yr=2019):
    """
    Convert AADT To monthly DOW ADT.
    """
    atr_count_columns = [
        "H01",
        "H02",
        "H03",
        "H04",
        "H05",
        "H06",
        "H07",
        "H08",
        "H09",
        "H10",
        "H11",
        "H12",
        "H13",
        "H14",
        "H15",
        "H16",
        "H17",
        "H18",
        "H19",
        "H20",
        "H21",
        "H22",
        "H23",
        "H24",
        "TOTAL",
    ]
    atr_file = Path.joinpath(path_txdot_fy22, "TxDOT_PERM_HOURLY_DATA_2013_092021.csv")
    atr_db_all = pd.read_csv(atr_file, low_memory=False, dtype={"Date": str})
    atr_db_all["ST_DATE"] = pd.to_datetime(atr_db_all.ST_DATE)
    atr_db_all["Year"] = atr_db_all.ST_DATE.dt.year
    atr_db_all["Month"] = atr_db_all.ST_DATE.dt.month
    # Extract the day of the week
    atr_db_all["day"] = atr_db_all["ST_DATE"].dt.dayofweek
    # Map integers to short day names
    map_dow = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}
    delete_Index = atr_db_all["TOTAL"] == 0
    atr_db_all = atr_db_all[~delete_Index]
    year_index = (atr_db_all["Year"] >= min_yr) & (atr_db_all["Year"] <= max_yr)
    atr_db_all = atr_db_all[year_index]
    assert all(
        atr_db_all.groupby(["DISTRICT"]).LOCAL_ID.nunique().values >= 5
    ), "At least 5 stations should be present per district."
    # generate indices
    # all records included
    all_index = atr_db_all["Month"] > 0
    # define the region for which the factors will be generated
    Selected_region = (
        "DISTRICT"  # the region needs to be the same as the field name in the ATR data
    )
    map_mnth = {
        1: "Jan",
        2: "Feb",
        3: "Mar",
        4: "Apr",
        5: "May",
        6: "Jun",
        7: "Jul",
        8: "Aug",
        9: "Sep",
        10: "Oct",
        11: "Nov",
        12: "Dec",
    }
    # ----------------------------------------------------------------------------------
    # Get AADT
    # ----------------------------------------------------------------------------------
    aadt = fun_region_episode(atr_db_all, all_index, Selected_region)
    aadt = aadt[[Selected_region, atr_count_columns[-1]]]
    Index_template = aadt.copy(deep=True)
    Index_template.sort_values(by=Selected_region, ascending=True, inplace=True)
    Index_template[Selected_region + "_alphabet_order"] = list(
        range(1, Index_template[Selected_region].shape[0] + 1)
    )
    Index_template.drop(columns={atr_count_columns[-1]}, inplace=True)
    # ----------------------------------------------------------------------------------
    # AERR20_Days
    # produce ratios to split traffic between weekday and weekend for each month. AADT
    # won't be used here
    # ----------------------------------------------------------------------------------
    df_adt = aadt.copy(deep=True)
    df_adt = pd.merge(
        df_adt,
        Index_template,
        how="left",
        left_on=[Selected_region],
        right_on=[Selected_region],
        suffixes=("", "_r"),
    )
    month_adt = (
        atr_db_all.groupby([Selected_region, "Month", "day"]).mean().reset_index()
    )
    month_adt = month_adt[[Selected_region, "Month", "day", "TOTAL"]]
    month_adt["dow_nm"] = month_adt["day"].map(map_dow)
    month_adt["mnth_nm"] = month_adt["Month"].map(map_mnth)
    df_adt = df_adt.merge(
        month_adt,
        how="left",
        left_on=[Selected_region],
        right_on=[Selected_region],
        suffixes=("", "_mnth_dow"),
    )
    df_adt = df_adt.rename(columns={"TOTAL": "AADT", "TOTAL_mnth_dow": "ADT_mnth_dow"})

    df_adt["f_m_d"] = df_adt.ADT_mnth_dow / df_adt.AADT
    df_adt.f_m_d.describe()
    df_adt.to_csv(
        Path.joinpath(path_interm, out_fi), sep="\t", index=False
    )


@timing
def mth_dow_fac(out_fi, min_yr, max_yr):
    conv_aadt_adt_mnth_dow(out_fi=out_fi, min_yr=min_yr, max_yr=max_yr)


if __name__ == "__main__":
    mth_dow_fac(out_fi="conv_aadt2mnth_dow.tab", min_yr=2013, max_yr=2019)
    print(
        "----------------------------------------------------------------------------\n"
        "Finished Processing iii_adt_to_aadt_fac.py\n"
        "----------------------------------------------------------------------------\n"
    )
    # path_conv_aadt2mnth_dow = Path.joinpath(path_interm, "conv_aadt2mnth_dow.tab")
    # conv_aadt_adt_mnth = pd.read_csv(path_conv_aadt2mnth_dow, sep="\t")
