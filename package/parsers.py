import sys

try:
    import ujson as json
except ImportError:
    import json  # noqa: F401
try:
    import parquet
except ImportError:
    parquet = False

try:
    import pandas as pd
except ImportError:
    pandas = False

try:
    from itertools import izip_longest as zip_longest
except ImportError:
    from itertools import zip_longest

if sys.version_info.major == 2:
    import unicodecsv as csv
else:
    import csv
    csv.field_size_limit(1000000000)