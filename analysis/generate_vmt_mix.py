"""
Run all the steps for VMT-Mix generation.
"""
import warnings
import datetime

warnings.simplefilter(action="ignore", category=Warning)

from vmtmix_fy23.utils import timing
from vmtmix_fy23 import (
    i_raw_dt_prc,
    ii_dow_by_cls_fact_calc,
    iv_mvc_hpms_counts,
    v_SU_CT_sh_lh_dist,
    vi_sut_nd_fuel_mix,
    vii_vmt_mix_disagg,
)

#Fixme: Test script iv and vii for logical consistency

@timing
def main(min_yr, max_yr, now_mntyr):
    suf1 = min_yr - 2000
    suf2 = max_yr - 2000
    # Process the raw MVC and permanent counter data to fix date time format, station id,
    # map road types to MOVES, and save data to parquet for faster loading.
    i_raw_dt_prc.raw_dt_prc(
        MVC_file="MVC_2013_21_received_on_030922",
        PERM_file="PERM_CLASS_BY_HR_2013_2021",
    )
    # Create DOW by veh class factors that will be applied to the AADT from ATR data
    # by vehicle class.
    ii_dow_by_cls_fact_calc.dow_by_cls_fac(
        out_fi="conv_aadt2dow_by_vehcat.tab", min_yr=min_yr, max_yr=max_yr
    )
    # Compute the HPMS category counts from the MVC data and apply the above conversion
    # factors.
    iv_mvc_hpms_counts.mvc_hpms_cnt(
        out_fi=f"mvc_vmtmix_{suf1}_{suf2}", min_yr=min_yr, max_yr=max_yr
    )
    # Get the SU and CT, Sh and Lh splits from FAF4 assignment and metadata using
    # ERG methodology and VIUS 2002 factor.
    v_SU_CT_sh_lh_dist.faf4_su_ct_lh_sh_pct(out_fi="faf4_su_ct_lh_sh_pct.tab")
    # Get the SUT dist within HPMS and the fuel dist from MOVES default database.
    vi_sut_nd_fuel_mix.mvs_sut_nd_fuel_mx(
        fueldist_outfi="mvs303fueldist.csv",
        sut_hpms_dist_outfi="mvs303defaultsutdist.csv",
    )
    # Appy the FAF4, and MOVES dist to the HPMS counts, filter data to different TODs,
    # and normalize the final counts to get the SUT-FT dist.
    vii_vmt_mix_disagg.fin_vmt_mix(
        in_file_nm=f"mvc_vmtmix_{suf1}_{suf2}_{now_mntyr}.csv",
        out_file_nm=f"fy23_fin_vmtmix_{suf1}_{suf2}_{now_mntyr}.csv",
    )


if __name__ == "__main__":
    now_yr = str(datetime.datetime.now().year)
    now_mnt = str(datetime.datetime.now().month).zfill(2)
    now_mntyr = now_mnt + now_yr
    # main(min_yr=2013, max_yr=2019, now_mntyr=now_mntyr)
    main(min_yr=2013, max_yr=2021, now_mntyr=now_mntyr)
    main(min_yr=2017, max_yr=2021, now_mntyr=now_mntyr)
    main(min_yr=2017, max_yr=2019, now_mntyr=now_mntyr)
