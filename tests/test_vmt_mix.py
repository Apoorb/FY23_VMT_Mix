"""


"""
import pandas as pd


def test_code_produce_same_vmt_mix_as_042023():
    p_apr22_vmt_mix = (
        r"E:\Texas A&M Transportation Institute"
        r"\TxDOT_TPP_Projects - Task 5.3 Activity Forecasting Factors (1)"
        r"\VMT-Mix\data\output\fin_vmtmix.csv"
    )
    p_feb23_vmt_mix = (
        r"E:\Texas A&M Transportation Institute"
        r"\TxDOT_TPP_Projects - Task 5.3 Activity Forecasting Factors\Data"
        r"\output\fin_vmtmix_022023_042022_comp.csv"
    )
    apr22_vmt_mix = pd.read_csv(p_apr22_vmt_mix)
    feb23_vmt_mix = pd.read_csv(p_feb23_vmt_mix)
    feb23_vmt_mix = feb23_vmt_mix.loc[lambda df: df.yearID >= 2010].reset_index(
        drop=True
    )
    pd.testing.assert_frame_equal(apr22_vmt_mix, feb23_vmt_mix)
