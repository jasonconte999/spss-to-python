from functools import reduce
from typing import Any, Dict, List, Optional, Union

import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

def multi_union(*dataframes):
    """Union multiple dataframes into one."""
    return reduce(DataFrame.unionByName, dataframes)