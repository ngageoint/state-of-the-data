
DROP TABLE IF EXISTS sotd.sotd_aeronauticcrv;
CREATE TABLE sotd.sotd_aeronauticcrv
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_aeronauticpnt;
CREATE TABLE sotd.sotd_aeronauticpnt
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_aeronauticsrf;
CREATE TABLE sotd.sotd_aeronauticsrf
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_agriculturepnt;
CREATE TABLE sotd.sotd_agriculturepnt
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_agriculturesrf;
CREATE TABLE sotd.sotd_agriculturesrf
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_boundarypnt;
CREATE TABLE sotd.sotd_boundarypnt
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_culturecrv;
CREATE TABLE sotd.sotd_culturecrv
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_culturepnt;
CREATE TABLE sotd.sotd_culturepnt
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_culturesrf;
CREATE TABLE sotd.sotd_culturesrf
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_facilitypnt;
CREATE TABLE sotd.sotd_facilitypnt
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_facilitysrf;
CREATE TABLE sotd.sotd_facilitysrf
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_hydroaidnavigationpnt;
CREATE TABLE sotd.sotd_hydroaidnavigationpnt
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_hydroaidnavigationsrf;
CREATE TABLE sotd.sotd_hydroaidnavigationsrf
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_hydrographycrv;
CREATE TABLE sotd.sotd_hydrographycrv
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_hydrographypnt;
CREATE TABLE sotd.sotd_hydrographypnt
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_hydrographysrf;
CREATE TABLE sotd.sotd_hydrographysrf
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_industrycrv;
CREATE TABLE sotd.sotd_industrycrv
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_industrypnt;
CREATE TABLE sotd.sotd_industrypnt
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_industrysrf;
CREATE TABLE sotd.sotd_industrysrf
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_informationcrv;
CREATE TABLE sotd.sotd_informationcrv
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_informationpnt;
CREATE TABLE sotd.sotd_informationpnt
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_informationsrf;
CREATE TABLE sotd.sotd_informationsrf
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_militarycrv;
CREATE TABLE sotd.sotd_militarycrv
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_militarypnt;
CREATE TABLE sotd.sotd_militarypnt
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_militarysrf;
CREATE TABLE sotd.sotd_militarysrf
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_physiographycrv;
CREATE TABLE sotd.sotd_physiographycrv
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_physiographypnt;
CREATE TABLE sotd.sotd_physiographypnt
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_physiographysrf;
CREATE TABLE sotd.sotd_physiographysrf
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_portharbourcrv;
CREATE TABLE sotd.sotd_portharbourcrv
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_portharbourpnt;
CREATE TABLE sotd.sotd_portharbourpnt
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_portharboursrf;
CREATE TABLE sotd.sotd_portharboursrf
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_recreationcrv;
CREATE TABLE sotd.sotd_recreationcrv
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_recreationpnt;
CREATE TABLE sotd.sotd_recreationpnt
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_recreationsrf;
CREATE TABLE sotd.sotd_recreationsrf
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_settlementpnt;
CREATE TABLE sotd.sotd_settlementpnt
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_settlementsrf;
CREATE TABLE sotd.sotd_settlementsrf
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_storagepnt;
CREATE TABLE sotd.sotd_storagepnt
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_storagesrf;
CREATE TABLE sotd.sotd_storagesrf
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_structurecrv;
CREATE TABLE sotd.sotd_structurecrv
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_structurepnt;
CREATE TABLE sotd.sotd_structurepnt
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_structuresrf;
CREATE TABLE sotd.sotd_structuresrf
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_transportationgroundcrv;
CREATE TABLE sotd.sotd_transportationgroundcrv
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_transportationgroundpnt;
CREATE TABLE sotd.sotd_transportationgroundpnt
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_transportationgroundsrf;
CREATE TABLE sotd.sotd_transportationgroundsrf
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_transportationwatercrv;
CREATE TABLE sotd.sotd_transportationwatercrv
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_transportationwaterpnt;
CREATE TABLE sotd.sotd_transportationwaterpnt
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_transportationwatersrf;
CREATE TABLE sotd.sotd_transportationwatersrf
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_utilityinfrastructurecrv;
CREATE TABLE sotd.sotd_utilityinfrastructurecrv
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_utilityinfrastructurepnt;
CREATE TABLE sotd.sotd_utilityinfrastructurepnt
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_utilityinfrastructuresrf;
CREATE TABLE sotd.sotd_utilityinfrastructuresrf
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_vegetationcrv;
CREATE TABLE sotd.sotd_vegetationcrv
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_vegetationpnt;
CREATE TABLE sotd.sotd_vegetationpnt
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);

DROP TABLE IF EXISTS sotd.sotd_vegetationsrf;
CREATE TABLE sotd.sotd_vegetationsrf
(
	objectid integer
	, shape geometry
	, feature_cnt integer
	, pa_min double precision
	, pa_max double precision
	, pa_mean double precision
	, pa_median double precision
	, pa_mode double precision
	, pa_null_cnt double precision
	, pa_null_pct double precision
	, pa_score integer
	, pa_tier text
	, pri_source text
	, pri_source_count integer
	, pri_source_per double precision
	, sec_source text
	, sec_source_count integer
	, sec_source_per double precision
	, source_list text
	, dom_date date
	, dom_date_cnt integer
	, dom_date_per double precision
	, dom_year integer
	, currency_score integer
	, dom_year_count integer
	, dom_year_per double precision
	, newest_date date
	, oldest_date date
	, no_date_cnt integer
	, no_date_per double precision
	, pct_2_year double precision
	, pct_5_year double precision
	, pct_10_year double precision
	, pct_15_year double precision
	, pct_15_year_plus double precision
	, cnt_2500 double precision
	, per_2500 double precision
	, cnt_5000 double precision
	, per_5000 double precision
	, cnt_12500 double precision
	, per_12500 double precision
	, cnt_25000 double precision
	, per_25000 double precision
	, cnt_50000 double precision
	, per_50000 double precision
	, cnt_100000 double precision
	, per_100000 double precision
	, cnt_250000 double precision
	, per_250000 double precision
	, cnt_500000 double precision
	, per_500000 double precision
	, cnt_1000000 double precision
	, per_1000000 double precision
	, dom_scale integer
	, dom_count integer
	, min_scale integer
	, min_scale_count integer
	, max_scale integer
	, max_scale_count integer
	, mission_planning text
	, grls_score text
	, them_acc_score integer
	, hadr_feature_cnt integer
	, hadr_max_def_cnt integer
	, hadr_min_def_cnt integer
	, hadr_mean_def_cnt double precision
	, hadr_median_def_cnt integer
	, hadr_pri_attr_def text
	, hadr_pri_attr_def_cnt integer
	, hadr_per_pri double precision
	, hadr_pri_num_def integer
	, hadr_pri_num_def_cnt integer
	, hadr_sec_attr_def text
	, hadr_sec_attr_def_cnt integer
	, hadr_per_sec double precision
	, hadr_sec_num_def integer
	, hadr_sec_num_def_cnt integer
	, hadr_lc_score integer
	, psg_feature_cnt integer
	, psg_max_def_cnt integer
	, psg_min_def_cnt integer
	, psg_mean_def_cnt double precision
	, psg_median_def_cnt integer
	, psg_pri_attr_def text
	, psg_pri_attr_def_cnt integer
	, psg_per_pri double precision
	, psg_pri_num_def integer
	, psg_pri_num_def_cnt integer
	, psg_sec_attr_def text
	, psg_sec_attr_def_cnt integer
	, psg_per_sec double precision
	, psg_sec_num_def integer
	, psg_sec_num_def_cnt integer
	, psg_lc_score integer
	, classification text
	, caveat text
	, cd_since_dom_year double precision
	, temp_acc_score integer
);
