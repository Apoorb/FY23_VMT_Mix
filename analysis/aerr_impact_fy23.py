# -*- coding: utf-8 -*-
"""
Test the emission quantities b/w old and new VMT Mix for HGB counties.
Created by: Apoorba Bibeka
Created on: 03/22/2022
"""
import pathlib
from pathlib import Path
import pandas as pd
import geopandas as gpd
import numpy as np
from vmtmix_fy23.utils import path_county_shp

path_aerr20_csvs = Path(
    r"E:\Texas A&M Transportation Institute\TxDOT_TPP_Projects - Task 5.3 Activity Forecasting Factors\Data\fy23_vmt_mix\Input\aerr20_output"
)
path_sutft_act = Path.joinpath(path_aerr20_csvs, "mvs303_act_sutft.csv")
path_sutft_emis = Path.joinpath(path_aerr20_csvs, "mvs303_emis_sutft.csv")
path_vmtmix_fy23_13_21 = Path(
    r"E:\Texas A&M Transportation Institute\TxDOT_TPP_Projects - Task 5.3 Activity Forecasting Factors\Data\fy23_vmt_mix\output\fy23_fin_vmtmix_13_21_032023.csv"
)
sutft_act = pd.read_csv(path_sutft_act)
suft_emis = pd.read_csv(path_sutft_emis)
vmtmix_fy23_13_21 = pd.read_csv(path_vmtmix_fy23_13_21)
gdf_county = gpd.read_file(path_county_shp)
district_map = gdf_county[["TXDOT_DIST", "FIPS_ST_CN"]].astype(int)
sutft_vmt = sutft_act.loc[sutft_act.mvs_act_type == "vmt"]
sutft_vmt = sutft_vmt.rename(columns={"activity": "vmt"})
sutft_vmt["vmt_agg"] = sutft_vmt.groupby(
    [
        "county",
        "fips",
        "year_id",
        "road_type_id",
        "roadtype_lab",
        "activity_type_id",
        "mvs_act_type",
        "act_em_lab",
    ]
).vmt.transform(sum)
sutft_vmt = sutft_vmt.filter(
    items=[
        "county",
        "fips",
        "year_id",
        "road_type_id",
        "roadtype_lab",
        "activity_type_id",
        "mvs_act_type",
        "source_type_id",
        "fuel_type_id",
        "act_em_lab",
        "vmt",
        "vmt_agg",
    ]
)
suft_emis_vmt = suft_emis.merge(
    sutft_vmt,
    on=[
        "county",
        "fips",
        "year_id",
        "road_type_id",
        "roadtype_lab",
        "source_type_id",
        "fuel_type_id",
    ],
    how="left",
)
suft_emis_vmt_onroad = suft_emis_vmt.loc[
    lambda df: (df.road_type_id != 1) & (df.vmt != 0)
]
suft_emis_vmt_onroad["emission_rate_calc"] = (
    suft_emis_vmt_onroad.emission_quant / suft_emis_vmt_onroad.vmt
)
hgb_fips = [48201, 48039, 48157, 48473, 48339, 48291, 48071, 48167]
san_fips = [48029, 48091, 48187, 48259, 48493]
hgb_san_fips = list(hgb_fips) + list(san_fips)
fips_254 = list(district_map.FIPS_ST_CN.unique())
analysis_fips = fips_254
suft_emis_vmt_onroad_anly = suft_emis_vmt_onroad.loc[
    lambda df: df.fips.isin(analysis_fips)
]
suft_emis_vmt_onroad_anly["vmt_mix_dp"] = (
    suft_emis_vmt_onroad_anly.vmt / suft_emis_vmt_onroad_anly.vmt_agg
)
district_map_anly = district_map.loc[
    district_map.FIPS_ST_CN.isin(analysis_fips)
].rename(columns={"FIPS_ST_CN": "fips", "TXDOT_DIST": "txdot_dist"})
vmtmix_anly = vmtmix_fy23_13_21.merge(district_map_anly, on="txdot_dist")
assert set(vmtmix_anly.fips.unique()).symmetric_difference(set(analysis_fips)) == set()
vmtmix_anly_filt = vmtmix_anly.loc[
    lambda df: (df.dowagg == "Wkd")
    & (df.tod == "day")
    & (df.mvs_rdtype_nm != "ALL")
    & (df.yearID == 2020)
]
assert len(analysis_fips) * 24 * 4 == len(vmtmix_anly_filt)
vmtmix_anly_filt["road_type_id"] = vmtmix_anly_filt.mvs_rdtype.astype(int)
vmtmix_anly_filt_1 = vmtmix_anly_filt.filter(
    items=["fips", "district", "road_type_id", "sourceTypeID", "fuelTypeID", "vmt_mix"]
).rename(
    columns={
        "sourceTypeID": "source_type_id",
        "fuelTypeID": "fuel_type_id",
        "vmt_mix": "vmt_mix_axb",
    }
)
suft_emis_vmt_onroad_anly_1 = suft_emis_vmt_onroad_anly.merge(
    vmtmix_anly_filt_1, on=["fips", "road_type_id", "source_type_id", "fuel_type_id"]
)
suft_emis_vmt_onroad_anly_1["vmt_axb"] = (
    suft_emis_vmt_onroad_anly_1.vmt_agg * suft_emis_vmt_onroad_anly_1.vmt_mix_axb
)
suft_emis_vmt_onroad_anly_1["emission_quant_axbvmt"] = (
    suft_emis_vmt_onroad_anly_1.emission_rate_calc * suft_emis_vmt_onroad_anly_1.vmt_axb
)

emis_comp_pol = (
    suft_emis_vmt_onroad_anly_1.loc[
        lambda df: df.eis_poll_nei17.isin(
            ["CO", "NH3", "SO2", "NOX", "VOC", "CO2", "PM10-PRI", "PM25-PRI", 71432]
        )
    ]
    .groupby(["short_name_mvs3", "county", "fips", "district"], as_index=False)
    .agg(
        vmt_mix_dp=("vmt_mix_dp", "sum"),
        vmt_mix_axb=("vmt_mix_axb", "sum"),
        vmt_dp=("vmt", "sum"),
        vmt_axb=("vmt_axb", "sum"),
        emission_quant_dp=("emission_quant", "sum"),
        emission_quant_axb=("emission_quant_axbvmt", "sum"),
    )
)
emis_comp_pol = emis_comp_pol.assign(
    vmt_mix_diff=lambda df: df.vmt_mix_axb - df.vmt_mix_dp,
    vmt_diff=lambda df: df.vmt_axb - df.vmt_dp,
    vmt_perdiff=lambda df: 100 * df.vmt_diff / df.vmt_dp,
    emission_quant_diff=lambda df: df.emission_quant_axb - df.emission_quant_dp,
    emission_quant_perdiff=lambda df: 100
    * df.emission_quant_diff
    / df.emission_quant_dp,
).sort_values(["short_name_mvs3", "emission_quant_perdiff"])

emis_by_rdtype = (
    suft_emis_vmt_onroad_anly_1.loc[
        lambda df: df.eis_poll_nei17.isin(
            ["CO", "NH3", "SO2", "NOX", "VOC", "CO2", "PM10-PRI", "PM25-PRI", 71432]
        )
    ]
    .groupby(
        ["short_name_mvs3", "county", "fips", "district", "roadtype_lab"],
        as_index=False,
    )
    .agg(emis=("emission_quant_axbvmt", "sum"))
)
emis_by_rdtype["emis_agg"] = emis_by_rdtype.groupby(
    ["short_name_mvs3", "county", "fips", "district"]
).emis.transform(sum)
emis_by_rdtype["emis_by_rdtype"] = emis_by_rdtype.emis / emis_by_rdtype.emis_agg
emis_by_rdtype_piv = (
    pd.pivot_table(
        emis_by_rdtype,
        index=["short_name_mvs3", "county", "fips", "district"],
        columns="roadtype_lab",
        values="emis_by_rdtype",
    )
    .fillna(0)
    .reset_index()
)
emis_by_rdtype_piv = emis_by_rdtype_piv.rename(
    columns={
        "Rural Restricted Access": "rra",
        "Rural Unrestricted Access": "rua",
        "Urban Restricted Access": "ura",
        "Urban Unrestricted Access": "uua",
    }
)

emis_by_sutft = (
    suft_emis_vmt_onroad_anly_1.loc[
        lambda df: df.eis_poll_nei17.isin(
            ["CO", "NH3", "SO2", "NOX", "VOC", "CO2", "PM10-PRI", "PM25-PRI", 71432]
        )
    ]
    .groupby(
        ["short_name_mvs3", "county", "fips", "district", "sut_fueltype_lab"],
        as_index=False,
    )
    .agg(emis=("emission_quant_axbvmt", "sum"))
)
emis_by_sutft["emis_agg"] = emis_by_sutft.groupby(
    ["short_name_mvs3", "county", "fips", "district"]
).emis.transform(sum)
emis_by_sutft["emis_by_sutft"] = emis_by_sutft.emis / emis_by_sutft.emis_agg
emis_by_sutft_piv = (
    pd.pivot_table(
        emis_by_sutft,
        index=["short_name_mvs3", "county", "fips", "district"],
        columns="sut_fueltype_lab",
        values="emis_by_sutft",
    )
    .fillna(0)
    .reset_index()
)


emis_comp_pol = emis_comp_pol.merge(
    emis_by_rdtype_piv, on=["short_name_mvs3", "county", "fips", "district"]
).merge(emis_by_sutft_piv, on=["short_name_mvs3", "county", "fips", "district"])
path_out = Path.joinpath(path_vmtmix.parent, "QC", "hgb_san_vmt_mix_axb_impact.xlsx")
emis_comp_pol.to_excel(path_out, index=False)
# Add % of miles on different types of roads in a county in old vs. new VMT-Mix.

#
# vmt_by_rdtype = suft_emis_vmt_onroad_anly_1.loc[
#     lambda df: df.eis_poll_nei17.isin(
#         ["CO", "NH3", "SO2", "NOX", "VOC", "CO2", "PM10-PRI", "PM25-PRI",
#          71432])].groupby(
#     ['short_name_mvs3', 'county', 'fips', 'district', "roadtype_lab"],
#     as_index=False).agg(vmt=("vmt", "sum"))
# vmt_by_rdtype["vmt_agg"] = vmt_by_rdtype.groupby(
#     ['short_name_mvs3', 'county', 'fips', 'district']).vmt.transform(sum)
# vmt_by_rdtype["vmt_by_rdtype"] = vmt_by_rdtype.vmt / vmt_by_rdtype.vmt_agg
# vmt_by_rdtype_piv = pd.pivot_table(
#     vmt_by_rdtype,
#     index=['short_name_mvs3', 'county', 'fips', 'district'],
#     columns="roadtype_lab",
#     values="vmt_by_rdtype").fillna(0).reset_index()
# vmt_by_rdtype_piv = vmt_by_rdtype_piv.rename(
#     columns={'Rural Restricted Access': "rra", 'Rural Unrestricted Access': "rua",
#              'Urban Restricted Access': "ura", 'Urban Unrestricted Access': "uua"})
#
# vmt_by_sutft = suft_emis_vmt_onroad_anly_1.loc[
#     lambda df: df.eis_poll_nei17.isin(
#         ["CO", "NH3", "SO2", "NOX", "VOC", "CO2", "PM10-PRI", "PM25-PRI",
#          71432])].groupby(
#     ['short_name_mvs3', 'county', 'fips', 'district', "sut_fueltype_lab"],
#     as_index=False).agg(vmt=("vmt", "sum"))
# vmt_by_sutft["vmt_agg"] = vmt_by_sutft.groupby(
#     ['short_name_mvs3', 'county', 'fips', 'district']).vmt.transform(sum)
# vmt_by_sutft["vmt_by_sutft"] = vmt_by_sutft.vmt / vmt_by_sutft.vmt_agg
# vmt_by_sutft_piv = pd.pivot_table(
#     vmt_by_sutft,
#     index=['short_name_mvs3', 'county', 'fips', 'district'],
#     columns="sut_fueltype_lab",
#     values="vmt_by_sutft").fillna(0).reset_index()
