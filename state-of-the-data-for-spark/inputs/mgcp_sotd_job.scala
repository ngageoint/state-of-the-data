import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import scala.collection.JavaConversions._
import spark.sessionState.conf

println(conf.autoBroadcastJoinThreshold)
println(conf.broadcastTimeout)

var jdbcOptionsInput = Map(
	"driver" -> "org.postgresql.Driver"
	, "url" -> "jdbc:postgresql://@@@emr_processing.sourceServerGridsMGCP@@@:@@@emr_processing.sourcePortGrids@@@/@@@emr_processing.sourceDatabaseMGCP@@@"
	, "user" -> "@@@emr_processing.sourceUsernameMGCP@@@"
	, "password" -> "@@@emr_processing.sourcePasswordMGCP@@@"
)

var jdbcOptionsOutput = Map(
	"driver" -> "org.postgresql.Driver"
	, "url" -> "jdbc:postgresql://@@@emr_processing.destinationServer@@@:@@@emr_processing.destinationPort@@@/@@@emr_processing.destinationDatabaseMGCP@@@"
	, "user" -> "@@@emr_processing.destinationUsernameMGCP@@@"
	, "password" -> "@@@emr_processing.destinationPasswordMGCP@@@"
)

// create the dataframe for the pop grids
var dfMinMaxPG = spark.read.format("jdbc").options(jdbcOptionsOutput).option("query", "SELECT MIN(objectid) objectid_min, MAX(objectid) objectid_max FROM sotd.pop_grid").load()
spark.read.format("jdbc").options(jdbcOptionsOutput).option("lowerBound", dfMinMaxPG.head().getInt(0)).option("upperBound", dfMinMaxPG.head().getInt(1)).option("numPartitions", 128).option("partitionColumn", "objectid").option("dbtable", """
(
	SELECT
		objectid
		, CONCAT(FLOOR((st_x(st_centroid(shape)) - -180.0) / 0.25), ':', FLOOR((st_y(st_centroid(shape)) - -90.0) / 0.25)) grid_id
		, grls_score
	FROM sotd.pop_grid
	WHERE st_x(st_centroid(shape)) IS NOT NULL
) tbl_pg
""").load().createOrReplaceTempView("df_pg")

// create the dataframe for the RPM feature class
var dfMinMaxRPM = spark.read.format("jdbc").options(jdbcOptionsOutput).option("query", "SELECT MIN(objectid) objectid_min, MAX(objectid) objectid_max FROM sotd.rpm").load()
spark.read.format("jdbc").options(jdbcOptionsOutput).option("lowerBound", dfMinMaxRPM.head().getInt(0)).option("upperBound", dfMinMaxRPM.head().getInt(1)).option("numPartitions", 128).option("partitionColumn", "objectid").option("dbtable", s"""
(
	SELECT
		objectid
		, CONCAT(FLOOR((st_x(st_centroid(shape)) - -180.0) / 0.25), ':', FLOOR((st_y(st_centroid(shape)) - -90.0) / 0.25)) grid_id
		, ${1998.to(2019).map(f => s"y${f}").mkString(", ")}
	FROM sotd.rpm
	WHERE st_x(st_centroid(shape)) IS NOT NULL
) tbl_rpm
""").load().createOrReplaceTempView("df_rpm")

List("aerofaca", "aerofacp", "agristra", "agristrp", "aquedcta", "aquedctl", "aquedctp", "barrierl", "bluffl", "bridgea", "bridgel", "builda", "buildp", "builtupa", "builtupp", "cisternp", "coasta", "coastl", "coastp", "comma", "commp", "cropa", "dama", "daml", "damp", "dangera", "dangerl", "dangerp", "disposea", "embanka", "embankl", "extracta", "extractp", "ferryl", "ferryp", "firebrka", "fordl", "fordp", "forta", "fortp", "grassa", "grounda", "harbora", "harborp", "indl", "inunda", "lakeresa", "landfrm1a", "landfrm2a", "landfrma", "landfrml", "landfrmp", "landicea", "landmrka", "landmrkl", "landmrkp", "locka", "lockl", "lockp", "markersp", "mila", "mill", "milp", "miscaerop", "miscl", "miscp", "miscpopa", "miscpopp", "mtnp", "nucleara", "oasisa", "obstrp", "physa", "piera", "pierl", "pipel", "plazaa", "powera", "powerl", "powerp", "processa", "processp", "pumpinga", "pumpingp", "railrdl", "rampa", "rapidsa", "rapidsl", "rapidsp", "rigwellp", "roadl", "rrturnp", "rryarda", "ruinsa", "runwaya", "runwayl", "runwayp", "seastrta", "seastrtl", "shedl", "shedp", "sporta", "storagea", "storagep", "substata", "substatp", "swampa", "telel", "testa", "textp", "thermala", "thermalp", "towerp", "trackl", "traill", "transa", "transl", "transp", "treata", "treatp", "treesa", "treesl", "treesp", "tundraa", "tunnela", "tunnell", "utilp", "voida", "watrcrsa", "watrcrsl", "wellsprp").foreach(fcName => {
	try {
		// fetch the partitioning values
		var dfMinMax = spark.read.format("jdbc").options(jdbcOptionsInput).option("query", s"SELECT MIN(objectid) objectid_min, MAX(objectid) objectid_max FROM mgcp.${fcName}").load()
		var oidMin = dfMinMax.head().getInt(0)
		var oidMax = dfMinMax.head().getInt(1)

		// create the initial dataframe that can be partitioned by objectid
		var dfMain = spark.read.format("jdbc").options(jdbcOptionsInput).option("lowerBound", oidMin).option("upperBound", oidMax).option("numPartitions", 128).option("partitionColumn", "objectid").option("dbtable", s"""
		(
			SELECT
				objectid
				, CONCAT(FLOOR((st_x(st_centroid(shape)) - -180.0) / 0.25), ':', FLOOR((st_y(st_centroid(shape)) - -90.0) / 0.25)) grid_id
				, srt
				, sdv
				, acc
				, srt_value
			FROM mgcp.${fcName} x
			LEFT JOIN(SELECT unnest(CAST(xpath('//CodedValue/Name/text()',definition) AS text)::text[]) AS srt_value,
			unnest(CAST(xpath('//CodedValue/Code/text()',definition) AS text)::int[]) AS code
			from sde.gdb_items
			WHERE sde.gdb_items.Name = 'MD_SOURCE') y
			ON x.srt = y.code
			WHERE st_x(st_centroid(shape)) IS NOT NULL
		) tbl
		""").load()
		dfMain.cache()
		dfMain.createOrReplaceTempView("df_mn")

		spark.sql("""
			SELECT
				grid_id
				, COUNT(1) feature_cnt
				, MIN(acc) pa_min
				, MAX(acc) pa_max
				, AVG(acc) pa_mean
				, percentile_approx(acc, 0.5) pa_median
			FROM df_mn
			GROUP BY grid_id
		""").createOrReplaceTempView("df_gr")

		spark.sql("""
			SELECT
				grid_id
				, pri_source
				, pri_source_count
			FROM (
				SELECT
					grid_id
					, srt_value pri_source
					, COUNT(1) pri_source_count
					, row_number() OVER (PARTITION BY grid_id ORDER BY COUNT(1) DESC) my_rank
				FROM df_mn
				GROUP BY grid_id, srt_value
			) t
			WHERE my_rank = 1
		""").createOrReplaceTempView("df_ps")

		spark.sql("""
			SELECT
				grid_id
				, sec_source
				, sec_source_count
			FROM (
				SELECT
					grid_id
					, srt_value sec_source
					, COUNT(1) sec_source_count
					, row_number() OVER (PARTITION BY grid_id ORDER BY COUNT(1) DESC) my_rank
				FROM df_mn
				GROUP BY grid_id, srt_value
			) t
			WHERE my_rank = 2
		""").createOrReplaceTempView("df_ss")

		spark.sql("""
			SELECT
				grid_id
				, concat_ws(',', collect_set(srt_value)) source_list
			FROM (
				SELECT
					grid_id
					, srt_value
				FROM df_mn
				GROUP BY grid_id, srt_value
			) t
			GROUP BY grid_id
		""").createOrReplaceTempView("df_sl")

		spark.sql("""
			SELECT
				grid_id
				, TO_DATE(dom_date, 'yyyy-MM-dd') dom_date
				, dom_date_cnt
			FROM (
				SELECT
					grid_id
					, sdv dom_date
					, COUNT(1) dom_date_cnt
					, row_number() OVER (PARTITION BY grid_id ORDER BY COUNT(1) DESC) my_rank
				FROM df_mn
				WHERE sdv <> 'noInformation'
				GROUP BY grid_id, sdv
			) t
			WHERE my_rank = 1
		""").createOrReplaceTempView("df_dd")

		spark.sql("""
			SELECT
				grid_id
				, int(dom_year) dom_year
				, dom_year_count
				, year(current_date()) - int(dom_year) dom_year_diff
			FROM (
				SELECT
					grid_id
					, SUBSTR(TRIM(sdv), 0, 4) dom_year
					, COUNT(1) dom_year_count
					, row_number() OVER (PARTITION BY grid_id ORDER BY COUNT(1) DESC) my_rank
				FROM df_mn
				WHERE sdv <> 'noInformation'
				GROUP BY grid_id, dom_year
			) t
			WHERE my_rank = 1
		""").createOrReplaceTempView("df_dy")

		// make this into an array
		var domYearCases = 1998.to(2019).map(a => s"WHEN l.dom_year=${a} THEN ${a.to(2019).map(b => s"ifnull(r.y${b},0)").mkString("+")}").mkString(" ")
		spark.sql(s"""
			SELECT
				l.grid_id
				, CASE ${domYearCases}
				  ELSE 0.0
				END AS cd_since_dom_year
			FROM df_dy l
			LEFT JOIN df_rpm r ON l.grid_id=r.grid_id
		""").createOrReplaceTempView("df_cd")

		spark.sql(s"""
			SELECT
				percentile(cd_since_dom_year, 0.022) minus2sigma
				, percentile(cd_since_dom_year, 0.158) minus1sigma
				, percentile(cd_since_dom_year, 0.842) plus1sigma
				, percentile(cd_since_dom_year, 0.978) plus2sigma
			FROM df_cd
		""").createOrReplaceTempView("df_sigma")

		spark.sql("""
			SELECT
				l.grid_id
				, r.minus2sigma
				, r.minus1sigma
				, r.plus1sigma
				, r.plus2sigma
			FROM df_cd l
			CROSS JOIN df_sigma r
		""").createOrReplaceTempView("df_ta")

		spark.sql("""
			SELECT
				grid_id
				, MAX(TO_DATE(sdv, 'yyyy-MM-dd')) newest_date
				, MIN(TO_DATE(sdv, 'yyyy-MM-dd')) oldest_date
			FROM df_mn
			WHERE sdv <> 'noInformation'
			GROUP BY grid_id
		""").createOrReplaceTempView("df_dl")

		spark.sql("""
			SELECT
				grid_id
				, COUNT(1) no_date_cnt
			FROM df_mn
			WHERE sdv = 'noInformation'
			GROUP BY grid_id
		""").createOrReplaceTempView("df_dn")

		spark.sql("""
			SELECT
				grid_id
				, COUNT(1) cnt_2_year
			FROM df_mn
			WHERE sdv <> 'noInformation' AND TO_DATE(sdv, 'yyyy-MM-dd') BETWEEN date_sub(current_date, 365 * 2) AND current_date
			GROUP BY grid_id
		""").createOrReplaceTempView("df_d2")

		List(List(5, 2), List(10, 5), List(15, 10)).foreach(yearDiff => {
			spark.sql(s"""
				SELECT
					grid_id
					, COUNT(1) cnt_${yearDiff(0)}_year
				FROM df_mn
				WHERE sdv <> 'noInformation' AND TO_DATE(sdv, 'yyyy-MM-dd') BETWEEN date_sub(current_date, 365 * ${yearDiff(0)}) AND date_sub(current_date, 365 * ${yearDiff(1)})
				GROUP BY grid_id
			""").createOrReplaceTempView(s"df_d${yearDiff(0)}")
		})

		spark.sql("""
			SELECT
				grid_id
				, COUNT(1) cnt_15_year_plus
			FROM df_mn
			WHERE sdv <> 'noInformation' AND TO_DATE(sdv, 'yyyy-MM-dd') < date_sub(current_date, 365 * 15)
			GROUP BY grid_id
		""").createOrReplaceTempView("df_d15_plus")

		spark.sql("""
			SELECT
				grid_id
				, acc pa_mode
			FROM (
				SELECT
					grid_id
					, acc
					, COUNT(acc) my_count
					, row_number() OVER (PARTITION BY grid_id ORDER BY COUNT(1) DESC) my_rank
				FROM df_mn
				GROUP BY grid_id, acc
			) t
			WHERE my_rank = 1
		""").createOrReplaceTempView("df_am")

		spark.sql("""
			SELECT
				grid_id
				, COUNT(1) pa_null_cnt
			FROM df_mn
			WHERE acc IS NULL
			GROUP BY grid_id
		""").createOrReplaceTempView("df_an")

		spark.sql("""
			SELECT
				split(l.grid_id, '[:]')[1] * 1440 + 1 + split(l.grid_id, '[:]')[0] objectid
				, CONCAT('SRID=4326;POLYGON ((', (split(l.grid_id, '[:]')[0] * 0.25) + -180.0, ' ', (split(l.grid_id, '[:]')[1] * 0.25) + -90.0, ', ', (split(l.grid_id, '[:]')[0] * 0.25) + -180.0, ' ', ((split(l.grid_id, '[:]')[1] * 0.25) + -90.0) + 0.25, ', ', ((split(l.grid_id, '[:]')[0] * 0.25) + -180.0) + 0.25, ' ', ((split(l.grid_id, '[:]')[1] * 0.25) + -90.0) + 0.25, ', ', ((split(l.grid_id, '[:]')[0] * 0.25) + -180.0) + 0.25, ' ', (split(l.grid_id, '[:]')[1] * 0.25) + -90.0, ', ', (split(l.grid_id, '[:]')[0] * 0.25) + -180.0, ' ', (split(l.grid_id, '[:]')[1] * 0.25) + -90.0, '))') shape
				, l.feature_cnt
				, l.pa_min
				, l.pa_max
				, l.pa_mean
				, l.pa_median
				, am.pa_mode
				, ifnull(an.pa_null_cnt, 0) pa_null_cnt
				, ifnull(an.pa_null_cnt, 0) / l.feature_cnt * 100.0 pa_null_pct
				, CASE
					WHEN (l.pa_median ==1) THEN 5
                    WHEN (l.pa_median ==2) THEN 3 
                    ELSE 0
                END AS pa_score
                , CASE
                    WHEN (l.pa_median ==1) THEN 'Accurate'
                    WHEN (l.pa_median ==2) THEN 'Approximate'
					ELSE 'No Ranking'
				END AS pa_tier
				, ps.pri_source
				, ps.pri_source_count
				, ps.pri_source_count / l.feature_cnt * 100.0 pri_source_per
				, ss.sec_source sec_source
				, ifnull(ss.sec_source_count, 0) sec_source_count
				, ifnull(ss.sec_source_count, 0) / l.feature_cnt * 100.0 sec_source_per
				, sl.source_list
				, dd.dom_date
				, ifnull(dd.dom_date_cnt, 0) dom_date_cnt
				, ifnull(dd.dom_date_cnt, 0) / l.feature_cnt * 100.0 dom_date_per
				, dy.dom_year
				, if(cd.cd_since_dom_year > 2019, 0, cd.cd_since_dom_year) cd_since_dom_year
				, CASE
					WHEN cd.cd_since_dom_year <= ta.minus2sigma THEN 5
					WHEN cd.cd_since_dom_year > ta.minus2sigma AND cd.cd_since_dom_year <= ta.minus1sigma THEN 4
					WHEN cd.cd_since_dom_year > ta.minus1sigma AND cd.cd_since_dom_year <= ta.plus1sigma THEN 3
					WHEN cd.cd_since_dom_year > ta.plus1sigma  AND cd.cd_since_dom_year <= ta.plus2sigma THEN 2
					WHEN cd.cd_since_dom_year > ta.plus2sigma THEN 1
					WHEN dy.dom_year > 2019 THEN 0
					ELSE 0
				END AS temp_acc_score
				, CASE
					WHEN dy.dom_year = 1111 THEN 6
					WHEN dy.dom_year_diff <= 2 THEN 5
					WHEN dy.dom_year_diff > 2 AND dy.dom_year_diff <= 5 THEN 4
					WHEN dy.dom_year_diff > 5 AND dy.dom_year_diff <= 10 THEN 3
					WHEN dy.dom_year_diff > 10 AND dy.dom_year_diff <= 15 THEN 2
					ELSE 1
				END AS currency_score
				, ifnull(dy.dom_year_count, 0) dom_year_count
				, ifnull(dy.dom_year_count, 0) / l.feature_cnt * 100.0 dom_year_per
				, dl.newest_date
				, dl.oldest_date
				, ifnull(dn.no_date_cnt, 0) no_date_cnt
				, ifnull(dn.no_date_cnt, 0) / l.feature_cnt * 100.0 no_date_per
				, ifnull(d2.cnt_2_year, 0) / l.feature_cnt * 100.0 pct_2_year
				, ifnull(d5.cnt_5_year, 0) / l.feature_cnt * 100.0 pct_5_year
				, ifnull(d10.cnt_10_year, 0) / l.feature_cnt * 100.0 pct_10_year
				, ifnull(d15.cnt_15_year, 0) / l.feature_cnt * 100.0 pct_15_year
				, ifnull(d15_plus.cnt_15_year_plus, 0) / l.feature_cnt * 100.0 pct_15_year_plus
				, 'U' AS classification
				, 'FO' AS caveat
			FROM df_gr l
			LEFT JOIN df_am            am       ON l.grid_id=am.grid_id
			LEFT JOIN df_an            an       ON l.grid_id=an.grid_id
			LEFT JOIN df_ps            ps       ON l.grid_id=ps.grid_id
			LEFT JOIN df_ss            ss       ON l.grid_id=ss.grid_id
			LEFT JOIN df_sl            sl       ON l.grid_id=sl.grid_id
			LEFT JOIN df_dd            dd       ON l.grid_id=dd.grid_id
			LEFT JOIN df_dy            dy       ON l.grid_id=dy.grid_id
			LEFT JOIN df_dl            dl       ON l.grid_id=dl.grid_id
			LEFT JOIN df_dn            dn       ON l.grid_id=dn.grid_id
			LEFT JOIN df_d2            d2       ON l.grid_id=d2.grid_id
			LEFT JOIN df_d5            d5       ON l.grid_id=d5.grid_id
			LEFT JOIN df_d10           d10      ON l.grid_id=d10.grid_id
			LEFT JOIN df_d15           d15      ON l.grid_id=d15.grid_id
			LEFT JOIN df_d15_plus      d15_plus ON l.grid_id=d15_plus.grid_id
			LEFT JOIN df_pg            pg       ON l.grid_id=pg.grid_id
			LEFT JOIN df_cd            cd       ON l.grid_id=cd.grid_id
			LEFT JOIN df_ta            ta       ON l.grid_id=ta.grid_id
		""").write.format("jdbc").option("driver", "org.postgresql.Driver").option("url", "jdbc:postgresql://@@@emr_processing.destinationServer@@@:@@@emr_processing.destinationPort@@@/@@@emr_processing.destinationDatabaseMGCP@@@").option("user", "@@@emr_processing.destinationUsernameMGCP@@@").option("password", "@@@emr_processing.destinationPasswordMGCP@@@").option("truncate", true).option("stringtype", "unspecified").option("dbtable", s"sotd.mgcp_${fcName}").mode(SaveMode.Overwrite).save()
		println(s"---------------------------------------------------------\ncompleted ${fcName}")
	} catch {
		case e: Exception => println(s"---------------------------------------------------------\nexception on ${fcName}, ${e}")

		// Attempt to seed the table that doesn't exist in topo with a geometry value (this will cause issues publishing our services/aprxs otherwise)
		try {
			// Read in table that was just created via pre-sql - SHOULD BE EMPTY
			var outputDf = spark.read.format("jdbc").options(jdbcOptionsOutput).option("query", s"SELECT * FROM sotd.mgcp_${fcName}").load()
		
			// If it is empty, create a df, insert the placeholder, and then join it to a new DF to write to the db
			if (outputDf.count() == 0) {
		
				var schemaStats = StructType(Array(StructField("objectid", IntegerType, true), StructField("shape", StringType, true)))
				var seedDf = spark.createDataFrame(Seq(Row(1, s"SRID=4326;POLYGON ((-0.01 -0.01, -0.01 0.01, 0.01 0.01, 0.01 -0.01, -0.01 -0.01))")), schemaStats)//.registerTempTable(s"df_${fcName}_postsql")
			
				// Join the seeded geom record to the empty schema table created from outputDf
				var resultDf = outputDf.join(seedDf, Seq("objectid","shape"), "fullouter")
				resultDf.write.format("jdbc").option("driver", "org.postgresql.Driver").option("url", "jdbc:postgresql://@@@emr_processing.destinationServer@@@:@@@emr_processing.destinationPort@@@/@@@emr_processing.destinationDatabaseMGCP@@@").option("user", "@@@emr_processing.destinationUsernameMGCP@@@").option("password", "@@@emr_processing.destinationPasswordMGCP@@@").option("truncate", true).option("stringtype", "unspecified").option("dbtable", s"sotd.mgcp_${fcName}").mode(SaveMode.Overwrite).save()
			}
		} catch {
			case e: Exception => println(s"Could not add seeded geom record to table ${fcName}, ${e}")
		}
	}
})
System.exit(0)