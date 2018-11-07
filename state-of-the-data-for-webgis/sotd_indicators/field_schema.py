#Note: fields with a '_' in it are reserved for ArcGIS Online. If the features aren't Hosted, we can remove the '_'
import numpy as np

dts = {
  "curr":np.dtype(
  [
    ('_ID', np.int),
    ('DOM_DATE', '|S48'),
    ('DOM_DATE_CNT', np.int32),
    ('DOM_DATE_PER', np.float64),
    ('DOM_YEAR', np.int32),
    ('DOM_YEAR_CNT', np.int32),
    ('DOM_YEAR_PER', np.float64),
    ('OLDEST_DATE', '|S1024'),
    ('NEWEST_DATE', '|S1024'),
    ('NO_DATE_CNT', np.int32),
    ('NO_DATE_PER', np.float64),
    ('PCT_2_YEAR', np.float64),
    ('PCT_5_YEAR', np.float64),
    ('PCT_10_YEAR', np.float64),
    ('PCT_15_YEAR', np.float64),
    ('PCT_15_PLUS_YEAR', np.float64),
    ('FEATURE_CNT', np.int32),
    ('CURRENCY_SCORE', np.int32)
  ]),
  "poac":np.dtype(
  [
    ('_ID', np.int),
    ('MEAN', np.float64),
    ('MEDIAN', np.float64),
    ('MODE', np.float64),
    ('MIN', np.float64),
    ('MAX', np.float64),
    ('NO_DATE_CNT', np.int32),
    ('NO_DATE_PCT', np.float64),
    ('FEATURE_CNT', np.int32),
    ('PA_SCORE', np.int32),
    ('TIER', '|S1024')
  ]),
  "srln":np.dtype(
  [
    ('_ID', np.int),
    ('SOURCE_LIST', '|S1024'),
    ('PRI_SOURCE', '|S256'),
    ('PRI_SOURCE_CNT', np.int32),
    ('PRI_SOURCE_PER', np.float64),
    ('SEC_SOURCE', '|S256'),
    ('SEC_SOURCE_CNT', np.int32),
    ('SEC_SOURCE_PER', np.float64)
  ]),
  "logc":np.dtype(
  [
    ('_ID', np.int),
    ('MEAN_DEF_CNT', np.float64),
    ('MEDIAN_DEF_CNT', np.int32),
    ('MIN_DEF_CNT', np.int32),
    ('MAX_DEF_CNT', np.int32),
    #STandard deviation
    ('PRI_NUM_DEF', np.int32),
    ('SEC_NUM_DEF', np.int32),
    ('PER_PRI', np.float64),
    ('PER_SEC', np.float64),
    ("PRI_ATTR_DEF", '|S20'), # pri_attr
    ("SEC_ATTR_DEF", '|S20'),
    ('PRI_ATTR_DEF_PER', np.float64),
    ('SEC_ATTR_DEF_PER', np.float64),
    ('FEATURE_CNT', np.int32),
    ('PRI_ATTR_DEF_CNT', np.float64),
    ('SEC_ATTR_DEF_CNT', np.float64),
    ('LC_SCORE', np.int32)
  ]),
  "cmpl":np.dtype(
  [
    ('_ID', np.int),
    ('TDS_DENSITY', np.float64),
    ('COMP_DENSITY', np.float64),
    ('COMPLETENESS_VALUE', np.float64),
    ('DIFFERENCE', np.float64)
  ]),
  "them":np.dtype(
  [
    ('_ID', np.int),
    ('DOM_SCALE', np.float64),
    ('DOM_COUNT', np.int32),
    ('DOM_PER', np.float64),
    ('MIN_SCALE', np.float64),
    ('MIN_PER', np.float64),
    ('MAX_SCALE', np.float64),
    ('MAX_PER', np.float64),
    ('CNT_2500', np.int32),
    ('CNT_5000', np.int32),
    ('CNT_12500', np.int32),
    ('CNT_25000', np.int32),
    ('CNT_50000', np.int32),
    ('CNT_100000', np.int32),
    ('CNT_250000', np.int32),
    ('CNT_500000', np.int32),
    ('CNT_1000000', np.int32),
    ('PER_2500', np.float64),
    ('PER_5000', np.float64),
    ('PER_12500', np.float64),
    ('PER_25000', np.float64),
    ('PER_50000', np.float64),
    ('PER_100000', np.float64),
    ('PER_250000', np.float64),
    ('PER_500000', np.float64),
    ('PER_1000000', np.float64),
    ('COUNT', np.int32),
    ('MISSION_PLANNING', '|S1024'),
    ('POPULATION_SCALE', '|S1024'),
    ('THEM_ACC_SCORE', np.float64),
    ('MEAN', np.float64)
  ])
}

field_schema = {
  "poac": [
    "MEAN",
    "MEDIAN",
    "MODE",
    "MIN",
    "MAX",
    "NO_DATE_CNT",
    "NO_DATE_PCT",
    "FEATURE_CNT",
    "PA_SCORE",
    "TIER"
  ],
  "cmpl": [
    'TDS_DENSITY',
    'COMP_DENSITY',
    'COMPLETENESS_VALUE',
    'DIFFERENCE'
  ],
  "them": [
    'DOM_SCALE',
    'DOM_COUNT',
    'DOM_PER',
    'MIN_SCALE',
    'MIN_PER',
    'MAX_SCALE',
    'MAX_PER',
    'CNT_2500',
    'CNT_5000',
    'CNT_12500',
    'CNT_25000',
    'CNT_50000',
    'CNT_100000',
    'CNT_250000',
    'CNT_500000',
    'CNT_1000000',
    'PER_2500',
    'PER_5000',
    'PER_12500',
    'PER_25000',
    'PER_50000',
    'PER_100000',
    'PER_250000',
    'PER_500000',
    'PER_1000000',
    'COUNT',  # Add Underscore if AGOL
    'MISSION_PLANNING',
    'POPULATION_SCALE',
    'THEM_ACC_SCORE',
    'MEAN'
  ],
  "srln": [
    'SOURCE_LIST',
    'PRI_SOURCE',
    'PRI_SOURCE_CNT',
    'PRI_SOURCE_PER',
    'SEC_SOURCE',
    'SEC_SOURCE_CNT',
    'SEC_SOURCE_PER'
  ],
  "curr": [
    'DOM_DATE',
    'DOM_DATE_CNT',
    'DOM_DATE_PER',
    'DOM_YEAR',
    'DOM_YEAR_CNT',
    'DOM_YEAR_PER',
    'OLDEST_DATE',
    'NEWEST_DATE',
    'NO_DATE_CNT',
    'NO_DATE_PER',
    'PCT_2_YEAR',
    'PCT_5_YEAR',
    'PCT_10_YEAR',
    'PCT_15_YEAR',
    'PCT_15_PLUS_YEAR',
    'FEATURE_CNT',
    'CURRENCY_SCORE'
  ],
  "logc": [
    'MEAN_DEF_CNT',
    'MEDIAN_DEF_CNT',
    'MIN_DEF_CNT',
    'MAX_DEF_CNT',
    'PRI_NUM_DEF',
    'SEC_NUM_DEF',
    'PER_PRI',
    'PER_SEC',
    'PRI_ATTR_DEF',
    'SEC_ATTR_DEF',
    'PRI_ATTR_DEF_PER',
    'SEC_ATTR_DEF_PER',
    'FEATURE_CNT',
    'PRI_ATTR_DEF_CNT',
    'SEC_ATTR_DEF_CNT',
    'LC_SCORE'
  ]
}
