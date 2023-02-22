# VMT-Mix generation module
This module contains a script for generating VMT-Mix using data from various sources, including MVC, permanent counter data, FAF4 assignment metadata, and MOVES default database. The script runs several steps to process and convert the input data and compute the final VMT-Mix.
## Installation
To use this module, first clone the repository and install the required dependencies using the following command:
`pip install -r requirements.txt`
## Usage
To generate VMT-Mix, call the main function in the script and pass in the minimum and maximum years to generate VMT-Mix for. The function will run several steps, including processing raw data, computing conversion factors, and normalizing the final counts. The generated VMT-Mix will be saved to a file with a name that includes the year range used to generate it.
 ```
if __name__ == "__main__":
    main(min_yr=2017, max_yr=2021)
    main(min_yr=2017, max_yr=2019)
    main(min_yr=2013, max_yr=2021)
```
## Modules used
The following modules from vmtmix_fy23 are used in this script:

- `i_raw_dt_prc`: processes the raw MVC and permanent counter data to fix date time format, station id, map road types to MOVES, and save data to parquet for faster loading.
- `ii_dow_by_cls_fact_calc`: creates DOW by veh class factors that will be applied to the AADT from ATR data by vehicle class.
- `iii_adt_to_aadt_fac`: creates DOW + Month Factors to convert the ADT data in the MVC to AADT data.
- `iv_mvc_hpms_counts`: computes the HPMS category counts from the MVC data and applies the above conversion factors.
- `v_SU_CT_sh_lh_dist`: gets the SU and CT, Sh and Lh splits from FAF4 assignment and metadata using ERG methodology and VIUS 2002 factor.
- `vi_sut_nd_fuel_mix`: gets the SUT dist within HPMS and the fuel dist from MOVES default database.
- `vii_vmt_mix_disagg`: applies the FAF4 and MOVES dist to the HPMS counts, filters data to different TODs, and normalizes the final counts to get the SUT-FT dist.
## Acknowledgements
This module is part of a larger project and utilizes various open-source libraries and data sources. We acknowledge the contributions of the following:

- `vmtmix_fy23` module, which contains various utility functions for VMT-Mix generation.
- OpenAI for providing the training data for ChatGPT, the language model used to generate this README.