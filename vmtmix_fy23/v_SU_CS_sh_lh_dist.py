"""
Get short-haul vs. long-haul distribution from the FAF4 Network Assignment data for
Texas.
"""
import pandas as pd
import numpy as np
from pathlib import Path
import geopandas as gpd
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname("__file__"), "..")))
from vmtmix_fy23.utils import (
    ChainedAssignent,
    timing,
    get_snake_case_dict,
    path_inp,
    path_interm,
    path_tx_hpms_2018,
    path_faf,
    path_county_shp,
)


switchoff_chainedass_warn = ChainedAssignent()


class TrucksDist:
    """
    Get VMT distribution between long-haul and short-haul for combination trucks (CT)
    and single unit trucks (SU)
    """

    def __init__(self, path_faf_, path_inp_):
        """
        Set the paths and create placeholder for the datasets that will be read.
        Parameters
        ----------
        path_faf_: Path to the FAF 4 data folder. This folder has the FAF 4 data-based
        assignment and the metadata which contains information on the route miles. It
        also has single unit (SU) and combination truck (CT) ADT, which along with the
        miles column can be used to compute the VMT for the SU and CT.
        path_inp_: Path to the input folder. Contains the shapefiles that identify
        urbanized areas, among other datasets.
        """
        self.dbf_ass_faf4 = Path.joinpath(
            path_faf_, "faf4", "assignment_results", "FAF4DATA_V43.DBF"
        )
        self.shp_meta_faf4 = Path.joinpath(
            path_faf_, "faf4", "faf4_esri_arcgis", "FAF4.shp"
        )
        self.path_urbanized_shp = Path.joinpath(
            path_inp_, "Shape_files", "TxDOT_Urbanized_Areas", "Urbanized_Area.shp"
        )
        self.ass_faf4_tx = pd.DataFrame()
        self.meta_faf4_tx = gpd.GeoDataFrame()
        self.gdf_county_1 = pd.DataFrame()
        self.txdot_urbanized = gpd.GeoDataFrame()
        self.meta_faf4_tx_filt = pd.DataFrame()  # Filtered to keep values for district
        # where road miles are greater than self.filt_faf4_txdist_mi
        self.meta_faf4_tx = pd.DataFrame()  # All data. To be used to get Texas default
        # for filling missing values.
        self.vmt_dist_txdist = pd.DataFrame()
        self.vmt_dist_tx = pd.DataFrame()
        self.filt_faf4_txdist_mi = 50
        self.ls_fhwa_fclass = [1, 2, 3, 4, 5, 6, 7]
        self.faf4_rurcode = 99999
        self.access_type = {
            1: "ra",
            2: "ra",
            3: "ura",
            4: "ura",
            5: "ura",
            6: "ura",
            7: "ura",
        }

    @timing
    def read_data(self):
        """
        Read FAF4 assignment and metadata, county geodata, and urbanized area
        geodata. Filter the FAF metadata to only keep Texas data.
        """
        ## Read FAF4 Assignment Data
        ass_faf4 = gpd.read_file(self.dbf_ass_faf4)
        ass_faf4 = ass_faf4.rename(columns=get_snake_case_dict(ass_faf4))
        self.ass_faf4_tx = ass_faf4.loc[ass_faf4.state == "TX"]
        self.ass_faf4_tx.info()
        ## Read FAF4 Metadata
        meta_faf4 = gpd.read_file(self.shp_meta_faf4)
        meta_faf4 = meta_faf4.rename(columns=get_snake_case_dict(meta_faf4))
        self.meta_faf4_tx = meta_faf4.loc[meta_faf4.state == "TX"]
        self.meta_faf4_tx.info()
        ## Read County Shapefile
        gdf_county = gpd.read_file(path_county_shp)
        gdf_county = gdf_county.rename(columns=get_snake_case_dict(gdf_county))
        self.gdf_county_1 = gdf_county.filter(items=["txdot_dist", "fips_st_cn"])
        self.txdot_urbanized = gpd.read_file(self.path_urbanized_shp)

    @timing
    def prc_meta_faf4(self):
        """
        Process the FAF4 metadata that has route information. We already filtered the
        FAF4 metadata to keep Texas only data in the `read_data` function. Following are
        keys processing steps of this function:
        - Assign county FIPS to the FAF4 metadata and the assignment data.
        - FAF metadata: Filter the data to just keep faf4 ID, road type, area type, county FIPS. FAF4 ID links metadata with assignment data.
        - FAF metadata: Check for percent of rows with missing functional class or area code.
        - FAF metadata: Filter out the rows with missing functional class and area codes.
        - FAF metadata: Map MOVES road types to the FAF4 area and road type codes.
        - FAF metadata: Add district column by merging with county shapefile.
        - Filter out the data for districts where we only have 50 miles of total route miles.
        Parameters
        ----------
        self: object
            Instance of `TrucksDist` class.
        """
        ## Assign State+County FIPS
        with switchoff_chainedass_warn:
            self.meta_faf4_tx["fips_st_cn"] = "48" + self.meta_faf4_tx.ctfips.astype(
                int
            ).astype(str).str.pad(width=3, side="left", fillchar="0")
            self.ass_faf4_tx["fips_st_cn"] = "48" + self.ass_faf4_tx.ctfips.astype(
                int
            ).astype(str).str.pad(width=3, side="left", fillchar="0")
        ## See that the rows with missing functional class or area code are an
        # insignificant
        ## percentage of the total rows.
        self.meta_faf4_tx.fafzone.unique()
        self.meta_faf4_tx.filter(
            items=["faf4_id", "fclass", "urban_code", "ctfips", "geometry"]
        )
        df_miss_urb_cd_fclass = self.meta_faf4_tx.loc[
            (self.meta_faf4_tx.urban_code.isna())
            | (~self.meta_faf4_tx.fclass.isin(self.ls_fhwa_fclass))
        ]
        pct_miss_urb_cd = len(df_miss_urb_cd_fclass) / len(self.meta_faf4_tx)
        print(
            f"Percent of rows with missing area type or road type codes in FAF data: {pct_miss_urb_cd:.2%}"
        )

        ## Get non nan functional class and area code rows. Map to MOVES functional
        # class.
        meta_faf4_tx_1 = self.meta_faf4_tx.loc[
            ~(
                (self.meta_faf4_tx.urban_code.isna())
                | (~self.meta_faf4_tx.fclass.isin(self.ls_fhwa_fclass))
            )
        ]
        map_urb = {
            urb_cd: (lambda x: "r" if x == self.faf4_rurcode else "u")(urb_cd)
            for urb_cd in meta_faf4_tx_1.urban_code.unique()
        }
        with switchoff_chainedass_warn:
            meta_faf4_tx_1["rural_urban"] = meta_faf4_tx_1.urban_code.map(map_urb)
        set(meta_faf4_tx_1.fclass.unique())
        with switchoff_chainedass_warn:
            meta_faf4_tx_1["access_type"] = meta_faf4_tx_1.fclass.map(self.access_type)
        meta_faf4_tx_1 = meta_faf4_tx_1.assign(
            mvs_rdtype=lambda df: np.select(
                [
                    (df.rural_urban == "r") & (df.access_type == "ra"),
                    (df.rural_urban == "r") & (df.access_type == "ura"),
                    (df.rural_urban == "u") & (df.access_type == "ra"),
                    (df.rural_urban == "u") & (df.access_type == "ura"),
                ],
                [2, 3, 4, 5],
                np.nan,
            )
        )
        meta_faf4_tx_1["mvs_rdtype_str"] = (
            meta_faf4_tx_1.rural_urban + meta_faf4_tx_1.access_type
        )
        assert all(~meta_faf4_tx_1.mvs_rdtype.isna())
        # Filter to relevant columns.
        meta_faf4_tx_2 = meta_faf4_tx_1.filter(
            items=[
                "faf4_id",
                "mvs_rdtype",
                "mvs_rdtype_str",
                "fclass",
                "access",
                "urban_code",
                "fips_st_cn",
                "miles",
            ]
        )
        # Add district column
        meta_faf4_tx_3 = meta_faf4_tx_2.merge(
            self.gdf_county_1, how="left", on="fips_st_cn"
        )
        debug_sample_size = meta_faf4_tx_3.groupby(
            "txdot_dist"
        ).mvs_rdtype.value_counts()
        assert set(meta_faf4_tx_3.txdot_dist) == set(self.gdf_county_1.txdot_dist)

        # Filter to keep entires with a decent sample size (subjective limit---need
        # more time to dig deeper!)
        mask = meta_faf4_tx_3.groupby(["txdot_dist", "mvs_rdtype"]).miles.transform(
            lambda x: x.sum() >= self.filt_faf4_txdist_mi
        )
        meta_faf4_tx_row_counts = (
            meta_faf4_tx_3.loc[mask]
            .assign(cnt_rows=1)
            .groupby(["txdot_dist", "mvs_rdtype"])
            .agg(cnt_rows=("cnt_rows", "sum"), miles=("miles", "sum"))
            .unstack()
        )
        # Filtered observations with lots of `na`---will use statewide values to fill na.
        self.meta_faf4_tx_filt = meta_faf4_tx_3.loc[mask]
        # Statewide data. Use for getting default values.
        self.meta_faf4_tx_default = meta_faf4_tx_3.copy()

    @timing
    def get_vmt_dist(self) -> dict[pd.DataFrame, pd.DataFrame]:
        """
        Get the distribution of the CLhT, CShT, SULhT, and SUShT by district and at
        statewide level. There are missing values at district level, so we
        are using the statewide values in the analysis. Following are
        keys processing steps of this function:

        - Create `ass_faf4_tx_distr` and `ass_faf4_tx_state` by merging FAF4 metadata and assignment data.
        - `ass_faf4_tx_state` is used for statewide distribution, thus we overwrite all the `txdot_dist` values with 0.
        - Call `compute_vmt_dist` to compute the distribution of the CLhT, CShT, SULhT, and SUShT.
        - `vmt_dist_tx` can be directly used. `vmt_dist_tx_distr` have missing values, so we merge with other datasets to help identify discrepencies.
        - `vmt_dist_tx_distr` is not used.

        Parameters
        ----------
        self: object
            Instance of `TrucksDist` class.

        Returns
        -------
        dict[pd.DataFrame, pd.DataFrame]:
            vmt_dist_tx has the distribution of the CLhT, CShT, SULhT, and SUShT at statewide level.
            vmt_dist_txdist has the distribution of the CLhT, CShT, SULhT, and SUShT at by district.
        """
        # Filter assignment table.
        ass_faf4_tx_distr = self.ass_faf4_tx.merge(
            self.meta_faf4_tx_filt.drop(columns="fips_st_cn"), on="faf4_id", how="right"
        )

        ass_faf4_tx_state = self.ass_faf4_tx.merge(
            self.meta_faf4_tx_default.drop(columns="fips_st_cn"),
            on="faf4_id",
            how="right",
        )
        ass_faf4_tx_state["txdot_dist"] = 0
        vmt_dist_tx_distr = self.compute_vmt_dist(ass_faf4_=ass_faf4_tx_distr)
        self.vmt_dist_tx = self.compute_vmt_dist(ass_faf4_=ass_faf4_tx_state)

        txdot_dist = self.gdf_county_1.txdot_dist.unique()
        mvs_rdtype_dist = [[2, 3, 4, 5]] * len(txdot_dist)
        distr_rdtype = (
            pd.DataFrame(dict(txdot_dist=txdot_dist, mvs_rdtype=mvs_rdtype_dist))
            .explode("mvs_rdtype")
            .sort_values(["txdot_dist", "mvs_rdtype"])
        )
        vmt_dist_tx_distr = distr_rdtype.merge(
            vmt_dist_tx_distr, on=["txdot_dist", "mvs_rdtype"], how="left"
        )
        self.vmt_dist_txdist = vmt_dist_tx_distr.merge(
            self.vmt_dist_tx.drop(columns="txdot_dist"),
            on=["mvs_rdtype"],
            suffixes=["", "_tx"],
            how="left",
        ).sort_values(["mvs_rdtype", "txdot_dist"])
        return dict(vmt_dist_tx=self.vmt_dist_tx, vmt_dist_txdist=self.vmt_dist_txdist)

    @staticmethod
    @timing
    def compute_vmt_dist(ass_faf4_, erg_crc_a88_vius2002_SULhT_pct=0.103) -> pd.DataFrame:
        """
        Static method, so does not have an instance of the class (self) in it.

        faf12 - Year 2012 FAF long distance truck volume estimated based on the FAF 4 Origin-Destination truck tonnage and includes empty trucks. Volume/day/section.
        nonfaf12 - Year 2012 Local truck traffic that is not part of FAF 4 O-D database. Volume/day/section
        su_aadt12 - Single Unit Truck Traffic year 2012
        comb_aadt1 - Combination Unit Truck Traffic year 2012

        Parameters
        ----------
        ass_faf4_: Combined FAF4 assignment and metadata.
        erg_crc_a88_vius2002_SULhT_pct: Single Unit Long Haul (SULhT) vs. Long Haul (Lh)
        trucks fraction based on the VIUS 2002 survey.

        Returns
        -------
        pd.DataFrame: distribution of the CLhT, CShT, SULhT, and SUShT.
        """
        ass_faf4_["lh_vmt12"] = ass_faf4_.faf12 * ass_faf4_.miles
        ass_faf4_["fafvmt12_chk_d"] = ass_faf4_.lh_vmt12 - ass_faf4_.fafvmt12
        assert (
            ass_faf4_.fafvmt12_chk_d.abs().max() <= 1.002
        ), "We are using the right AADTT and length"
        ass_faf4_["sh_vmt12"] = ass_faf4_.nonfaf12 * ass_faf4_.miles
        ass_faf4_["tot_vmt12"] = ass_faf4_["sh_vmt12"] + ass_faf4_["lh_vmt12"]
        ass_faf4_tx_dist_ = (
            ass_faf4_.groupby(["txdot_dist", "mvs_rdtype", "mvs_rdtype_str"])
            .agg(tot_vmt12=("tot_vmt12", sum), lh_vmt12=("lh_vmt12", sum))
            .reset_index()
        )
        all_rdtypes = (
            ass_faf4_tx_dist_.groupby(["txdot_dist"])
            .agg(tot_vmt12=("tot_vmt12", sum), lh_vmt12=("lh_vmt12", sum))
            .reset_index()
            .assign(mvs_rdtype="ALL", mvs_rdtype_str="ALL")
        )

        ass_faf4_tx_dist_1_ = pd.concat([ass_faf4_tx_dist_, all_rdtypes])

        ass_faf4_tx_dist_1_["pct_lh"] = (
            ass_faf4_tx_dist_1_["lh_vmt12"] / ass_faf4_tx_dist_1_["tot_vmt12"]
        )
        ass_faf4_tx_dist_1_["pct_SULhT_vs_totTrucks"] = (
            ass_faf4_tx_dist_1_.pct_lh * erg_crc_a88_vius2002_SULhT_pct
        )
        ass_faf4_tx_dist_1_["pct_CLhT_vs_totTrucks"] = ass_faf4_tx_dist_1_.pct_lh * (
            1 - erg_crc_a88_vius2002_SULhT_pct
        )

        ## Get CT vs. SU statistics from the HPMS side data.
        debug1 = ass_faf4_tx_dist_1_.groupby("mvs_rdtype").pct_lh.describe()
        # Use the HPMS fields to get SU vs. CT Stats
        ass_faf4_tx_hpms = ass_faf4_.copy()
        ass_faf4_tx_hpms["su_hpms_vmt12"] = (
            ass_faf4_tx_hpms.su_aadt12 * ass_faf4_tx_hpms.miles
        )
        ass_faf4_tx_hpms["ct_hpms_vmt12"] = (
            ass_faf4_tx_hpms.comb_aadt1 * ass_faf4_tx_hpms.miles
        )
        ass_faf4_tx_hpms["tot_hpms_vmt12"] = (
            ass_faf4_tx_hpms.su_hpms_vmt12 + ass_faf4_tx_hpms.ct_hpms_vmt12
        )
        ass_faf4_tx_hpms_dist = (
            ass_faf4_tx_hpms.groupby(["txdot_dist", "mvs_rdtype", "mvs_rdtype_str"])
            .agg(
                tot_hpms_vmt12=("tot_hpms_vmt12", sum),
                ct_hpms_vmt12=("ct_hpms_vmt12", sum),
            )
            .reset_index()
        )
        all_rdtypes_hpms = (
            ass_faf4_tx_hpms_dist.groupby(["txdot_dist"])
            .agg(
                tot_hpms_vmt12=("tot_hpms_vmt12", sum),
                ct_hpms_vmt12=("ct_hpms_vmt12", sum),
            )
            .reset_index()
            .assign(mvs_rdtype="ALL", mvs_rdtype_str="ALL")
        )

        ass_faf4_tx_hpms_dist_1_ = pd.concat([ass_faf4_tx_hpms_dist, all_rdtypes_hpms])

        ass_faf4_tx_hpms_dist_1_["pct_ct"] = (
            ass_faf4_tx_hpms_dist_1_["ct_hpms_vmt12"]
            / ass_faf4_tx_hpms_dist_1_["tot_hpms_vmt12"]
        )
        ass_faf4_tx_hpms_dist_1_["pct_su"] = 1 - ass_faf4_tx_hpms_dist_1_["pct_ct"]

        ass_faf4_ = ass_faf4_tx_dist_1_.merge(
            ass_faf4_tx_hpms_dist_1_,
            on=["txdot_dist", "mvs_rdtype", "mvs_rdtype_str"],
            how="left",
        )
        ass_faf4_["valid_entries"] = ass_faf4_.pct_CLhT_vs_totTrucks <= ass_faf4_.pct_ct
        ass_faf4_tx_2 = ass_faf4_.loc[ass_faf4_.valid_entries]
        with switchoff_chainedass_warn:
            ass_faf4_tx_2["pct_CLhT_vs_CT"] = (
                ass_faf4_tx_2["pct_CLhT_vs_totTrucks"] / ass_faf4_tx_2["pct_ct"]
            )
            ass_faf4_tx_2["pct_CShT_vs_CT"] = 1 - ass_faf4_tx_2["pct_CLhT_vs_CT"]

            ass_faf4_tx_2["pct_SULhT_vs_SU"] = (
                ass_faf4_tx_2["pct_SULhT_vs_totTrucks"] / ass_faf4_tx_2["pct_su"]
            )
            ass_faf4_tx_2["pct_SUShT_vs_SU"] = 1 - ass_faf4_tx_2["pct_SULhT_vs_SU"]
        filt_cols = [
            "txdot_dist",
            "mvs_rdtype",
            "pct_CLhT_vs_CT",
            "pct_CShT_vs_CT",
            "pct_SULhT_vs_SU",
            "pct_SUShT_vs_SU",
        ]
        ass_faf4_tx_3 = ass_faf4_tx_2.filter(items=filt_cols)
        return ass_faf4_tx_3


def main():
    truckdist = TrucksDist(
        path_faf_=path_faf, path_inp_=path_inp
    )
    truckdist.read_data()
    truckdist.prc_meta_faf4()
    vmt_dist_dict = truckdist.get_vmt_dist()
    path_out = Path.joinpath(path_interm, "faf4_su_ct_lh_sh_pct.tab")
    vmt_dist_dict["vmt_dist_tx"].to_csv(path_out, index=False, sep="\t")
    print("Finished processing...")


if __name__ == "__main__":
    main()
    print(
        "----------------------------------------------------------------------------\n"
        "Finished Processing v_SU_CS_sh_lh_dist.py\n"
        "----------------------------------------------------------------------------\n"
    )
