"""
Get FHWA VMT Mix from Card 4.
"""
import pandas as pd
import numpy as np
from pathlib import Path
import pyarrow.parquet as pq
import geopandas as gpd
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname("__file__"), "..")))
from vmtmix_fy23.utils import (
    path_inp,
    path_interm,
    path_output,
    path_txdot_fy22,
    path_fig_dir,
    path_txdot_districts_shp,
    ChainedAssignent,
    get_snake_case_dict,
)

switchoff_chainedass_warn = ChainedAssignent()


# ToDo: Covert the classification counts to annual values before adding.
# ToDo: Create a case for using district level analysis or county level...
# ToDo: Get the FHWA Mix
# ToDo: Get DOW factors for different vehicle categories. Apply on FHWA Mix.
class MVCVmtMix:
    map_ra = dict(
        [(2, "r_ra"), (3, "r_ura"), (4, "u_ra"), (5, "u_ura"), ("ALL", "ALL")]
    )
    vehclscntcols = {
        "class1": "MC",
        "class2": "PC",
        "class3": "PT_LCT",
        "class4": "Bus",
        "class5": "SU_MH_RT_HDV",
        "class6": "SU_MH_RT_HDV",
        "class7": "SU_MH_RT_HDV",
        "class8": "CT_HDV",
        "class9": "CT_HDV",
        "class10": "CT_HDV",
        "class11": "CT_HDV",
        "class12": "CT_HDV",
        "class13": "CT_HDV",
        "class14": "Unk",
        "class15": "Unk",
    }
    agg_vtype_cols = ["MC", "PC", "PT_LCT", "Bus", "SU_MH_RT_HDV", "CT_HDV"]

    def __init__(
        self, path_inp=path_inp, path_interm=path_interm, min_yr=2013, max_yr=2019
    ):
        # Set input paths
        self.path_mvc_pq = Path.joinpath(
            path_txdot_fy22,  "MVC_2013_21_received_on_030922.parquet"
        )
        self.path_conv_aadt2mnth_dow = Path.joinpath(
            path_interm, "conv_aadt2mnth_dow.tab"
        )
        self.path_conv_aadt2dow_by_vehcat = Path.joinpath(
            path_interm, "conv_aadt2dow_by_vehcat.tab"
        )
        self.path_dgcodes_marty = Path.joinpath(path_inp, "district_dgcode_map.xlsx")
        self.min_yr = min_yr
        self.max_yr = max_yr
        self._txdist = pd.DataFrame()
        self.mvc = pd.DataFrame()
        self.conv_aadt_adt_mnth = pd.DataFrame()
        self.conv_aadt2dow_by_vehcat = pd.DataFrame()

        # Read/ process relevant data
        self.set_mvc()
        self.set_txdist()
        self.set_conv_aadt_adt_mnth()
        self.set_conv_aadt2dow_by_vehcat()
        self.dgcodes = pd.read_excel(self.path_dgcodes_marty)

    def set_mvc(self):
        df_mvc = pq.read_table(self.path_mvc_pq).to_pandas()
        df_mvc["year"] = df_mvc.start_datetime.dt.year
        df_mvc["hour"] = df_mvc.start_datetime.dt.hour
        df_mvc["mnth_nm"] = df_mvc.start_datetime.dt.month_name().str[:3]
        df_mvc["dow_nm"] = df_mvc.start_datetime.dt.day_name().str[:3]
        df_mvc["date_"] = df_mvc.start_datetime.dt.date
        df_mvc = df_mvc[(df_mvc.year <= self.max_yr) & (df_mvc.year >= self.min_yr)]
        df_mvc_nona = df_mvc.loc[~df_mvc.mvs_rdtype.isna()]
        with switchoff_chainedass_warn:
            df_mvc_nona["mvs_rdtype"] = df_mvc_nona["mvs_rdtype"].astype(int)
        nan_data_size = (len(df_mvc) - len(df_mvc_nona)) / len(df_mvc)
        print(f"Removing {nan_data_size :%} of data with no road type.")
        df_mvc.loc[df_mvc.mvs_rdtype.isna(), "sta_pre_id_suf_fr"].unique()
        mvc_ALL = df_mvc_nona.copy(deep=True)
        mvc_ALL["mvs_rdtype"] = "ALL"
        mvc_ALL["rural_urban"] = "ALL"
        mvc_ALL["funcl_cls"] = "ALL"
        mvc_ALL["access_type"] = "ALL"
        mvc_ALL["fr"] = np.nan
        mvc_1 = pd.concat([df_mvc_nona, mvc_ALL])
        with switchoff_chainedass_warn:
            mvc_1["mvs_rdtype_nm"] = mvc_1.mvs_rdtype.map(self.map_ra)
        debug = mvc_1.loc[lambda df: df.mvs_rdtype.isna()]

        mc_cols = [key for key, val in self.vehclscntcols.items() if val == "MC"]
        pc_cols = [key for key, val in self.vehclscntcols.items() if val == "PC"]
        pt_lct_cols = [
            key for key, val in self.vehclscntcols.items() if val == "PT_LCT"
        ]
        bus_cols = [key for key, val in self.vehclscntcols.items() if val == "Bus"]
        suhdv_cols = [
            key for key, val in self.vehclscntcols.items() if val == "SU_MH_RT_HDV"
        ]
        cthdv_cols = [key for key, val in self.vehclscntcols.items() if val == "CT_HDV"]
        # FixMe: Check for 0 or abnormal volumes
        with switchoff_chainedass_warn:
            mvc_1["MC"] = mvc_1[mc_cols].sum(axis=1)
            mvc_1["PC"] = mvc_1[pc_cols].sum(axis=1)
            mvc_1["PT_LCT"] = mvc_1[pt_lct_cols].sum(axis=1)
            mvc_1["Bus"] = mvc_1[bus_cols].sum(axis=1)
            mvc_1["SU_MH_RT_HDV"] = mvc_1[suhdv_cols].sum(axis=1)
            mvc_1["CT_HDV"] = mvc_1[cthdv_cols].sum(axis=1)
        self.mvc = mvc_1

    def set_txdist(self):
        txdist_tmp = gpd.read_file(path_txdot_districts_shp)
        self.txdist = txdist_tmp[["DIST_NBR", "DIST_NM"]].rename(
            columns={"DIST_NBR": "txdot_dist", "DIST_NM": "district"}
        )

    def set_conv_aadt_adt_mnth(self):
        conv_aadt_adt_mnth = pd.read_csv(self.path_conv_aadt2mnth_dow, sep="\t")
        conv_aadt_adt_mnth = conv_aadt_adt_mnth.rename(
            columns=get_snake_case_dict(conv_aadt_adt_mnth)
        )
        conv_aadt_adt_mnth = conv_aadt_adt_mnth.merge(
            self.txdist, on=["district"], how="outer"
        )
        conv_aadt_adt_mnth["inv_f_m_d"] = (
            1 / conv_aadt_adt_mnth.f_m_d
        )  # Convert DOW, Month ADT to AADT.

        self.conv_aadt_adt_mnth = conv_aadt_adt_mnth.filter(
            items=["txdot_dist", "district", "mnth_nm", "dow_nm", "inv_f_m_d"]
        )

    def set_conv_aadt2dow_by_vehcat(self):
        conv_aadt2dow_by_vehcat = pd.read_csv(
            self.path_conv_aadt2dow_by_vehcat, sep="\t"
        )
        self.conv_aadt2dow_by_vehcat = conv_aadt2dow_by_vehcat.merge(
            self.txdist, on=["district"], how="outer"
        )

    def filt_mvc_counts(self):
        mvc_filt_ = self.mvc.groupby(
            [
                "sta_pre_id_suf_fr",
                "txdot_dist",
                "mvs_rdtype_nm",
                "mvs_rdtype",
                "mnth_nm",
                "date_",
                "year",
                "dow_nm",
                "hour",
            ],
            as_index=False,
        )[self.agg_vtype_cols].mean()
        mvc_filt_adt_ = mvc_filt_.merge(
            self.conv_aadt_adt_mnth, on=["txdot_dist", "mnth_nm", "dow_nm"], how="left"
        ).merge(self.dgcodes, on=["district"], how="left")
        assert set(mvc_filt_adt_.dgcode) == set(
            self.dgcodes.dgcode
        ), "Need all DGCODES for aggregation."
        return mvc_filt_adt_

    def agg_mvc_counts(self, spatial_level="district"):
        mvc_filt_adt = self.filt_mvc_counts()
        with switchoff_chainedass_warn:
            mvc_filt_adt["MC_adt"] = mvc_filt_adt["MC"] * mvc_filt_adt.inv_f_m_d
            mvc_filt_adt["PC_adt"] = mvc_filt_adt["PC"] * mvc_filt_adt.inv_f_m_d
            mvc_filt_adt["PT_LCT_adt"] = mvc_filt_adt["PT_LCT"] * mvc_filt_adt.inv_f_m_d
            mvc_filt_adt["Bus_adt"] = mvc_filt_adt["Bus"] * mvc_filt_adt.inv_f_m_d
            mvc_filt_adt["SU_MH_RT_HDV_adt"] = (
                mvc_filt_adt["SU_MH_RT_HDV"] * mvc_filt_adt.inv_f_m_d
            )
            mvc_filt_adt["CT_HDV_adt"] = mvc_filt_adt["CT_HDV"] * mvc_filt_adt.inv_f_m_d
        agg_vtype_cols_adt = [f"{col}_adt" for col in self.agg_vtype_cols]
        mvc_filt_adt_agg = mvc_filt_adt.groupby(
            [spatial_level, "mvs_rdtype_nm", "mvs_rdtype", "hour"], as_index=False
        )[agg_vtype_cols_adt].mean()
        return mvc_filt_adt_agg

    def get_mvc_sample_size(self, spatial_level):
        mvc_filt_adt = self.filt_mvc_counts()
        mvc_filt_adt_sample_size = mvc_filt_adt.groupby(
            [spatial_level, "mvs_rdtype_nm", "mvs_rdtype", "year", "hour"],
            as_index=False,
        ).sta_pre_id_suf_fr.nunique()
        mvc_filt_adt_sample_size_agg_ = mvc_filt_adt_sample_size.groupby(
            [spatial_level, "mvs_rdtype_nm", "mvs_rdtype", "hour"], as_index=False
        ).sta_pre_id_suf_fr.mean()
        return mvc_filt_adt_sample_size_agg_

    def plot_heatmap(self, mvs_rdtype_nm_sel, spatial_level="district"):
        mvc_filt_adt_sample_size_agg = self.get_mvc_sample_size(
            spatial_level=spatial_level
        )
        mvc_filt_adt_sample_size_agg.mvs_rdtype_nm.unique()
        mvc_filt_adt_sample_size_rdtyp = mvc_filt_adt_sample_size_agg.loc[
            lambda df: df.mvs_rdtype_nm == mvs_rdtype_nm_sel
        ].pivot(index=spatial_level, columns="hour", values="sta_pre_id_suf_fr")
        sns.set_context("poster", font_scale=1)
        fig, ax = plt.subplots(figsize=(12, 6))
        g = sns.heatmap(
            mvc_filt_adt_sample_size_rdtyp,
            cmap="viridis",
            linewidths=0.5,
            vmin=0,
            vmax=mvc_filt_adt_sample_size_agg.sta_pre_id_suf_fr.max(),
            annot=True,
            annot_kws={"fontdict": {"fontsize": "small"}},
            ax=ax,
            fmt="g",
        )
        ax.set_title("{}—{}".format(spatial_level, mvs_rdtype_nm_sel), y=1.05)
        return fig


def get_min_ss_per_loc(mvcvmtmix_, spatial_level_):
    txdist_rdtyp_ = mvcvmtmix_.txdist
    dgcodes_rdtyp_ = mvcvmtmix_.dgcodes.drop_duplicates("dgcode")[["dgcode"]]
    if spatial_level_ == "district":
        spatial_rdtyp_ = txdist_rdtyp_
    elif spatial_level_ == "dgcode":
        spatial_rdtyp_ = dgcodes_rdtyp_
    else:
        raise ValueError("spatial_level_ can either be 'district' or 'dgcode'")
    spatial_rdtyp_["mvs_rdtype_nm"] = [["ALL", "r_ra", "r_ura", "u_ra", "u_ura"]] * len(
        spatial_rdtyp_
    )
    spatial_rdtyp_lng = spatial_rdtyp_.explode("mvs_rdtype_nm")
    mvc_ss = mvcvmtmix_.get_mvc_sample_size(spatial_level=spatial_level_)
    mvc_ss_min_ss = mvc_ss.groupby(
        [spatial_level_, "mvs_rdtype_nm", "mvs_rdtype"], as_index=False
    ).agg(min_avg_sta_count=("sta_pre_id_suf_fr", "min"))
    mvc_ss_min_ss_5 = mvc_ss_min_ss.loc[mvc_ss_min_ss.min_avg_sta_count >= 5]
    mvc_all_and_mvc_min_ss_5 = spatial_rdtyp_lng.merge(
        mvc_ss_min_ss_5, on=[spatial_level_, "mvs_rdtype_nm"], how="left"
    )
    if spatial_level_ == "dgcode":
        assert all(
            mvc_all_and_mvc_min_ss_5.min_avg_sta_count >= 5
        ), "Some dgcode and road types do not have sufficient samples."
    return mvc_all_and_mvc_min_ss_5


def handle_low_district_ss(all_district_sta_counts_, mvcvmtmix_):
    all_district_sta_counts_1 = all_district_sta_counts_.merge(
        mvcvmtmix_.dgcodes, on="district"
    )
    good_sta_ss = all_district_sta_counts_1.loc[lambda df: df.min_avg_sta_count >= 5]
    low_sta_ss = all_district_sta_counts_1.loc[lambda df: df.min_avg_sta_count.isna()]
    mvc_agg_dist = mvcvmtmix_.agg_mvc_counts(spatial_level="district")

    good_ss_grps = set(good_sta_ss[["district", "mvs_rdtype_nm"]].apply(tuple, axis=1))
    low_ss_grps = set(low_sta_ss[["district", "mvs_rdtype_nm"]].apply(tuple, axis=1))
    mvc_agg_dist_good_sta_ss = mvc_agg_dist.groupby(
        ["district", "mvs_rdtype_nm"]
    ).filter(lambda grp: grp.name in good_ss_grps)
    mvc_agg_dist_good_sta_ss["based_on_dg"] = False
    mvc_agg_dist_good_sta_ss = mvc_agg_dist_good_sta_ss.merge(
        mvcvmtmix_.dgcodes, on="district", how="left"
    )

    mvc_agg_dg = mvcvmtmix_.agg_mvc_counts(spatial_level="dgcode")
    mvc_agg_dg_with_dup_dist = mvc_agg_dg.merge(mvcvmtmix_.dgcodes, on="dgcode")
    mvc_agg_dist_low_sta_ss = mvc_agg_dg_with_dup_dist.groupby(
        ["district", "mvs_rdtype_nm"]
    ).filter(lambda grp: grp.name in low_ss_grps)
    mvc_agg_dist_low_sta_ss["based_on_dg"] = True

    mvc_agg_dist_imputed_ = pd.concat(
        [mvc_agg_dist_good_sta_ss, mvc_agg_dist_low_sta_ss]
    )
    assert len(mvc_agg_dist_imputed_) == 25 * 5 * 24
    assert all(
        mvc_agg_dist_imputed_.groupby(
            ["district", "mvs_rdtype_nm", "hour"]
        ).PT_LCT_adt.count()
        == 1
    ), "Expect  1 unique count for the 2400 rows."
    return mvc_agg_dist_imputed_


def compute_vmtmix_dow(mvc_agg_dist_imputed_, mvcvmtmix_):
    fac_dow_by_vehcat = mvcvmtmix_.conv_aadt2dow_by_vehcat
    fac_dow_by_vehcat_filt = fac_dow_by_vehcat.filter(
        items=[
            "district",
            "dowagg",
            "f_m_d_MC",
            "f_m_d_PC",
            "f_m_d_PT_LCT",
            "f_m_d_Bus",
            "f_m_d_HDV",  # Couldn't separate SU vs. CT. A lot of bad readings.
        ]
    )

    mvc_agg_dist_imputed_dow_ = mvc_agg_dist_imputed_.merge(
        fac_dow_by_vehcat_filt, on="district", how="left"
    )

    mvc_agg_dist_imputed_dow_ = mvc_agg_dist_imputed_dow_.assign(
        MC_dow=lambda df: df.MC_adt * df.f_m_d_MC,
        PC_dow=lambda df: df.PC_adt * df.f_m_d_PC,
        PT_LCT_dow=lambda df: df.PT_LCT_adt * df.f_m_d_PT_LCT,
        Bus_dow=lambda df: df.Bus_adt * df.f_m_d_Bus,
        SU_MH_RT_HDV_dow=lambda df: df.SU_MH_RT_HDV_adt * df.f_m_d_HDV,
        CT_HDV_dow=lambda df: df.CT_HDV_adt * df.f_m_d_HDV,
        Total_dow=lambda df: (
            df.MC_dow
            + df.PC_dow
            + df.PT_LCT_dow
            + df.Bus_dow
            + df.SU_MH_RT_HDV_dow
            + df.CT_HDV_dow
        ),
    )

    mvc_agg_dist_imputed_dow_filt = mvc_agg_dist_imputed_dow_.filter(
        items=[
            "dgcode",
            "district",
            "based_on_dg",
            "mvs_rdtype_nm",
            "mvs_rdtype",
            "dowagg",
            "hour",
            "MC_dow",
            "PC_dow",
            "PT_LCT_dow",
            "Bus_dow",
            "SU_MH_RT_HDV_dow",
            "CT_HDV_dow",
            "Total_dow",
        ]
    ).assign(
        MC_frac=lambda df: df.MC_dow / df.Total_dow,
        PC_frac=lambda df: df.PC_dow / df.Total_dow,
        PT_LCT_frac=lambda df: df.PT_LCT_dow / df.Total_dow,
        Bus_frac=lambda df: df.Bus_dow / df.Total_dow,
        SU_MH_RT_HDV_frac=lambda df: df.SU_MH_RT_HDV_dow / df.Total_dow,
        CT_HDV_frac=lambda df: df.CT_HDV_dow / df.Total_dow,
    )

    return mvc_agg_dist_imputed_dow_filt


def main():
    path_out_sta_counts = Path.joinpath(path_interm, "sta_counts_mvc_script_v.csv")
    path_out_mvc_vmtmix = Path.joinpath(path_output, "mvc_vmtmix.csv")

    mvcvmtmix = MVCVmtMix()
    fig_dist = {}
    ra_nms = list(mvcvmtmix.map_ra.values())
    for ra_nm in ra_nms:
        spatial_level = "district"
        fig_dist[ra_nm] = mvcvmtmix.plot_heatmap(
            mvs_rdtype_nm_sel=ra_nm, spatial_level=spatial_level
        )
        path_fig = Path.joinpath(
            path_fig_dir, f"mvc_sta_counts_{spatial_level}_{ra_nm}.jpg"
        )
        fig_dist[ra_nm].savefig(path_fig)
    mvc_agg_dg = mvcvmtmix.agg_mvc_counts(spatial_level="dgcode")
    mvc_ss_dg = mvcvmtmix.get_mvc_sample_size(spatial_level="dgcode")
    fig_dg = {}
    ra_nms = list(mvcvmtmix.map_ra.values())
    for ra_nm in ra_nms:
        spatial_level = "dgcode"
        fig_dg[ra_nm] = mvcvmtmix.plot_heatmap(
            mvs_rdtype_nm_sel=ra_nm, spatial_level="dgcode"
        )
        path_fig = Path.joinpath(
            path_fig_dir, f"mvc_sta_counts_{spatial_level}_{ra_nm}.jpg"
        )
        fig_dg[ra_nm].savefig(path_fig)
    all_district_sta_counts = get_min_ss_per_loc(
        mvcvmtmix_=mvcvmtmix, spatial_level_="district"
    )
    filling_dgcode_sta_counts = get_min_ss_per_loc(
        mvcvmtmix_=mvcvmtmix, spatial_level_="dgcode"
    )

    mvc_agg_dist_imputed = handle_low_district_ss(
        all_district_sta_counts_=all_district_sta_counts, mvcvmtmix_=mvcvmtmix
    )
    vmtmix_dow = compute_vmtmix_dow(mvc_agg_dist_imputed, mvcvmtmix)
    # TODO: Investigate the minimum sample size needed based on standard deviation.
    all_district_sta_counts.to_csv(path_out_sta_counts, index=False)

    vmtmix_dow.to_csv(path_out_mvc_vmtmix, index=False)


if __name__ == "__main__":
    main()
    print(
        "----------------------------------------------------------------------------\n"
        "Finished Processing iv_mvc_hpms_vmt_mix.py\n"
        "----------------------------------------------------------------------------\n"
    )