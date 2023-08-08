"""
Compare the FY23 VMT-Mix with HPMS VMT-Mix.
    # 1. Percent of emissions from Running
    # 2. Refactor running VMT to different vehicles
    # 3. Recompute emissions
    # 4. See the impact
Created by: Apoorb
Created on: July 20, 2023
"""
from pathlib import Path
import pandas as pd
import geopandas as gpd
import numpy as np

# Load Data
#######################################################################################
pa_data = Path(r"C:\Users\a-bibeka\PycharmProjects\FY23_VMT_Mix\data")
pa_emis = pa_data.joinpath("mvs303_emis_sutft.csv")
pa_act = pa_data.joinpath("mvs303_act_sutft.csv")
pa_vmix_fy23 = pa_data.joinpath("fy23_fin_vmtmix_13_21_032023.csv")
pa_vmix_hpms = pa_data.joinpath("PivotChartForVMTmixComparison.xlsx")
pa_rate_map = pa_data.joinpath("Rate_Categories.csv")
pa_county_shp = pa_data.joinpath(
    "Texas_County_Boundaries", "County.shp"
)
sutft_act = pd.read_csv(pa_act)
suft_emis = pd.read_csv(pa_emis)
vmtmix = pd.read_csv(pa_vmix_fy23)
rate_map = pd.read_csv(pa_rate_map)
gdf_county = gpd.read_file(pa_county_shp)
district_map = gdf_county[["TXDOT_DIST", "FIPS_ST_CN"]].astype(int)
# Transform
#######################################################################################
rate_map.pollutantName.unique()
POLS = [
    'Carbon Monoxide (CO)',
    'Oxides of Nitrogen (NOx)',
    'Volatile Organic Compounds',
    'CO2 Equivalent',
    'Primary Exhaust PM10  - Total',
    'Primary PM10 - Brakewear Particulate',
    'Primary PM10 - Tirewear Particulate',
    'Primary Exhaust PM2.5 - Total',
    'Primary PM2.5 - Brakewear Particulate',
    'Primary PM2.5 - Tirewear Particulate'
    ]
rate_map_filt = rate_map.loc[lambda df: df.pollutantName.isin(POLS)]
pol_filt = rate_map_filt[["pollutantID", "pollutantName"]].drop_duplicates().reset_index(drop=True)
pol_emis = suft_emis[["pollutant_id", "short_name_mvs3", "short_name_nei17"]].drop_duplicates().reset_index(drop=True)
pol_filt_1 = pol_filt.merge(pol_emis, left_on="pollutantID", right_on="pollutant_id")
fips_254 = list(district_map.FIPS_ST_CN.unique())
analysis_fips = fips_254

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

suft_emis_vmt_onroad_anly = suft_emis_vmt_onroad.loc[
    lambda df: df.fips.isin(analysis_fips)
]
#
#######################################################################################