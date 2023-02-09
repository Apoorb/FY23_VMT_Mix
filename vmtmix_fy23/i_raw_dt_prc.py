"""
Process ATR and VCC TxDOT data. Check layers in STAR II (^1) to get more info.
^1 https://txdot.public.ms2soft.com/tcds/tsearch.asp?loc=Txdot&mod=TCDS
"""
import pandas as pd
import numpy as np
from pathlib import Path
import geopandas as gpd
import pyarrow.parquet as pq
import pyarrow as pa
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname("__file__"), "..")))
from vmtmix_fy23.utils import (
    ChainedAssignent,
    path_txdot_fy22,
    path_txdot_districts_shp,
    path_interm,
    path_county_shp,
    get_snake_case_dict,
)

switchoff_chainedass_warn = ChainedAssignent()
