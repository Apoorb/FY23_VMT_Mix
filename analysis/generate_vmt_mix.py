"""
Run all the steps for VMT-Mix generation.
"""
import warnings
warnings.simplefilter(action='ignore', category=Warning)

from vmtmix_fy23.utils import timing
from vmtmix_fy23 import (
    i_raw_dt_prc, ii_dow_by_cls_fact_calc, iii_adt_to_aadt_fac, iv_mvc_hpms_counts,
    v_SU_CT_sh_lh_dist, vi_sut_nd_fuel_mix, vii_vmt_mix_disagg
)


@timing
def main(min_yr, max_yr):
    # Process the raw MVC and permanent counter data to fix date time format, station id,
    # map road types to MOVES, and save data to parquet for faster loading.
    i_raw_dt_prc.raw_dt_prc(
        MVC_file="MVC_2013_21_received_on_030922",
        PERM_file="PERM_CLASS_BY_HR_2013_2021"
    )
    # Create DOW by veh class factors that will be applied to the AADT from ATR data
    # by vehicle class.
    ii_dow_by_cls_fact_calc.dow_by_cls_fac(
        out_fi="conv_aadt2dow_by_vehcat.tab", min_yr=min_yr, max_yr=max_yr
    )
    # Create DOW + Month Factors to convert the ADT data in the MVC to AADT data. These
    # are not by vehicle class and computed from the expanded ATR data without vehicle
    # class information.
    iii_adt_to_aadt_fac.mth_dow_fac(
        out_fi="conv_aadt2mnth_dow.tab", min_yr=min_yr, max_yr=max_yr
    )
    # Compute the HPMS category counts from the MVC data and apply the above conversion
    # factors.
    iv_mvc_hpms_counts.mvc_hpms_cnt(out_fi="mvc_vmtmix", min_yr=min_yr, max_yr=max_yr)
    # Get the SU and CT, Sh and Lh splits from FAF4 assignment and metadata using
    # ERG methodology and VIUS 2002 factor.
    v_SU_CT_sh_lh_dist.faf4_su_ct_lh_sh_pct(out_fi="faf4_su_ct_lh_sh_pct.tab")
    # Get the SUT dist within HPMS and the fuel dist from MOVES default database.
    vi_sut_nd_fuel_mix.mvs_sut_nd_fuel_mx(
        fueldist_outfi="mvs303fueldist.csv",
        sut_hpms_dist_outfi="mvs303defaultsutdist.csv"
    )
    # Appy the FAF4, and MOVES dist to the HPMS counts, filter data to different TODs,
    # and normalize the final counts to get the SUT-FT dist.
    suf1 = min_yr - 2000
    suf2 = max_yr - 2000
    vii_vmt_mix_disagg.fin_vmt_mix(out_file_nm=f"fy23_fin_vmtmix_{suf1}_{suf2}")


if __name__ == "__main__":
    main(min_yr=2017, max_yr=2021)
    main(min_yr=2017, max_yr=2019)
    main(min_yr=2013, max_yr=2021)


