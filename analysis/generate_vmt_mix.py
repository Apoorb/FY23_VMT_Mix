"""
Run all the steps for VMT-Mix generation.
"""
import warnings
warnings.simplefilter(action='ignore', category=Warning)

from vmtmix_fy23.utils import timing
from vmtmix_fy23 import (
    i_raw_dt_prc, ii_dow_by_cls_fact_calc, iii_adt_to_aadt_fac, iv_mvc_hpms_counts,
    v_SU_CT_sh_lh_dist, vi_fuel_mix, vii_vmt_mix_disagg
)


@timing
def main():
    #
    i_raw_dt_prc.raw_dt_prc(
        MVC_file="MVC_2013_21_received_on_030922",
        PERM_file="PERM_CLASS_BY_HR_2013_2021"
    )
    #
    ii_dow_by_cls_fact_calc.dow_by_cls_fac(
        out_fi="conv_aadt2dow_by_vehcat.tab", min_yr=2013, max_yr=2019
    )
    #
    iii_adt_to_aadt_fac.mth_dow_fac(
        out_fi="conv_aadt2mnth_dow.tab", min_yr=2013, max_yr=2019
    )
    #
    iv_mvc_hpms_counts.mvc_hpms_cnt(out_fi="mvc_vmtmix", min_yr=2013, max_yr=2019)
    #
    v_SU_CT_sh_lh_dist.faf4_su_ct_lh_sh_pct(out_fi="faf4_su_ct_lh_sh_pct.tab")
    #
    vi_fuel_mix.mvs_sut_nd_fuel_mx(
        fueldist_outfi="mvs303fueldist.csv",
        sut_hpms_dist_outfi="mvs303defaultsutdist.csv"
    )
    #
    vii_vmt_mix_disagg.fin_vmt_mix(out_file_nm="fy23_fin_vmtmix_13_19")


if __name__ == "__main__":
    main()
