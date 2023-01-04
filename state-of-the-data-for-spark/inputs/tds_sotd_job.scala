import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import scala.collection.JavaConversions._
import spark.sessionState.conf

println(conf.autoBroadcastJoinThreshold)
println(conf.broadcastTimeout)
var numPartitions = 64

var jdbcOptionsInput = Map(
	"driver" -> "org.postgresql.Driver"
	, "url" -> "jdbc:postgresql://@@@emr_processing.sourceServerGrids@@@:@@@emr_processing.sourcePortGrids@@@/@@@emr_processing.sourceDatabaseTDS@@@"
	, "user" -> "@@@emr_processing.sourceUsernameTDS@@@"
	, "password" -> "@@@emr_processing.sourcePasswordTDS@@@"
)

var jdbcOptionsOutput = Map(
	"driver" -> "org.postgresql.Driver"
	, "url" -> "jdbc:postgresql://@@@emr_processing.destinationServer@@@:@@@emr_processing.destinationPort@@@/@@@emr_processing.destinationDatabaseTDS@@@"
	, "user" -> "@@@emr_processing.destinationUsernameTDS@@@"
	, "password" -> "@@@emr_processing.destinationPasswordTDS@@@"
)

var clausesLookupSets = Map(
	"hadr" -> Map(
		"aeronauticpnt" -> List("asy,zva,aoo,lzn,zi005_fna,fpt,zi019_asx,caa,haf,wid,pcf", "(f_code='GB230' AND (aoo in (NULL, -999999, -999999.0))) OR (f_code='GB230' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='GB030' AND (haf in (NULL, -999999, -999999.0))) OR (f_code='GB030' AND (lzn in (NULL, -999999, -999999.0))) OR (f_code='GB030' AND (wid in (NULL, -999999, -999999.0))) OR (f_code='GB030' AND (zi019_asx in (NULL, -999999, -999999.0))) OR (f_code='GB035' AND (asy in (NULL, -999999, -999999.0))) OR (f_code='GB035' AND (caa in (NULL, -999999, -999999.0))) OR (f_code='GB035' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='GB035' AND (zva in (NULL, -999999, -999999.0))) OR (f_code='GB005' AND (asy in (NULL, -999999, -999999.0))) OR (f_code='GB005' AND (caa in (NULL, -999999, -999999.0))) OR (f_code='GB005' AND (fpt in (NULL, -999999, -999999.0))) OR (f_code='GB005' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='GB005' AND (zva in (NULL, -999999, -999999.0))) OR (f_code='GB040' AND (lzn in (NULL, -999999, -999999.0))) OR (f_code='GB040' AND (wid in (NULL, -999999, -999999.0))) OR (f_code='GB065' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='GB065' AND (asy in (NULL, -999999, -999999.0))) OR (f_code='GB065' AND (caa in (NULL, -999999, -999999.0))) OR (f_code='GB065' AND (zva in (NULL, -999999, -999999.0))) OR (f_code='GB065' AND (zi005_fna in (NULL, 'noInformation', 'Null', 'NULL', 'None', '')))", "if(f_code='GB230' AND aoo in (NULL, -999999, -999999.0), 'aoo', ''),if(f_code='GB230' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='GB030' AND haf in (NULL, -999999, -999999.0), 'haf', ''),if(f_code='GB030' AND lzn in (NULL, -999999, -999999.0), 'lzn', ''),if(f_code='GB030' AND wid in (NULL, -999999, -999999.0), 'wid', ''),if(f_code='GB030' AND zi019_asx in (NULL, -999999, -999999.0), 'zi019_asx', ''),if(f_code='GB035' AND asy in (NULL, -999999, -999999.0), 'asy', ''),if(f_code='GB035' AND caa in (NULL, -999999, -999999.0), 'caa', ''),if(f_code='GB035' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='GB035' AND zva in (NULL, -999999, -999999.0), 'zva', ''),if(f_code='GB005' AND asy in (NULL, -999999, -999999.0), 'asy', ''),if(f_code='GB005' AND caa in (NULL, -999999, -999999.0), 'caa', ''),if(f_code='GB005' AND fpt in (NULL, -999999, -999999.0), 'fpt', ''),if(f_code='GB005' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='GB005' AND zva in (NULL, -999999, -999999.0), 'zva', ''),if(f_code='GB040' AND lzn in (NULL, -999999, -999999.0), 'lzn', ''),if(f_code='GB040' AND wid in (NULL, -999999, -999999.0), 'wid', ''),if(f_code='GB065' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='GB065' AND asy in (NULL, -999999, -999999.0), 'asy', ''),if(f_code='GB065' AND caa in (NULL, -999999, -999999.0), 'caa', ''),if(f_code='GB065' AND zva in (NULL, -999999, -999999.0), 'zva', ''),if(f_code='GB065' AND zi005_fna in (NULL, 'noInformation', 'Null', 'NULL', 'None', ''), 'zi005_fna', '')"),
		"aeronauticsrf" -> List("zi019_asu,gb052_ridl,zvh,lzn,zi019_asx,zi019_sfs,gb052_ridh,caa,wid", "(f_code='GB055' AND (zi019_asx in (NULL, -999999, -999999.0))) OR (f_code='GB055' AND (zi019_asu in (NULL, -999999, -999999.0))) OR (f_code='GB055' AND (zi019_sfs in (NULL, -999999, -999999.0))) OR (f_code='GB055' AND (caa in (NULL, -999999, -999999.0))) OR (f_code='GB055' AND (zvh in (NULL, -999999, -999999.0))) OR (f_code='GB055' AND (lzn in (NULL, -999999, -999999.0))) OR (f_code='GB055' AND (gb052_ridh in (NULL, 'noInformation', 'Null', 'NULL', 'None', ''))) OR (f_code='GB055' AND (gb052_ridl in (NULL, 'noInformation', 'Null', 'NULL', 'None', ''))) OR (f_code='GB055' AND (wid in (NULL, -999999, -999999.0)))", "if(f_code='GB055' AND zi019_asx in (NULL, -999999, -999999.0), 'zi019_asx', ''),if(f_code='GB055' AND zi019_asu in (NULL, -999999, -999999.0), 'zi019_asu', ''),if(f_code='GB055' AND zi019_sfs in (NULL, -999999, -999999.0), 'zi019_sfs', ''),if(f_code='GB055' AND caa in (NULL, -999999, -999999.0), 'caa', ''),if(f_code='GB055' AND zvh in (NULL, -999999, -999999.0), 'zvh', ''),if(f_code='GB055' AND lzn in (NULL, -999999, -999999.0), 'lzn', ''),if(f_code='GB055' AND gb052_ridh in (NULL, 'noInformation', 'Null', 'NULL', 'None', ''), 'gb052_ridh', ''),if(f_code='GB055' AND gb052_ridl in (NULL, 'noInformation', 'Null', 'NULL', 'None', ''), 'gb052_ridl', ''),if(f_code='GB055' AND wid in (NULL, -999999, -999999.0), 'wid', '')"),
		"agriculturepnt" -> List("aoo", "(f_code='AJ110' AND (aoo in (NULL, -999999, -999999.0)))", "if(f_code='AJ110' AND aoo in (NULL, -999999, -999999.0), 'aoo', '')"),
		"agriculturesrf" -> List("zi013_ffp", "(f_code='EA010' AND (zi013_ffp in (NULL, -999999, -999999.0))) OR (f_code='BH135' AND (zi013_ffp in (NULL, -999999, -999999.0)))", "if(f_code='EA010' AND zi013_ffp in (NULL, -999999, -999999.0), 'zi013_ffp', ''),if(f_code='BH135' AND zi013_ffp in (NULL, -999999, -999999.0), 'zi013_ffp', '')"),
		"culturepnt" -> List("zi037_rel", "(f_code='AL030' AND (zi037_rel in (NULL, -999999, -999999.0)))", "if(f_code='AL030' AND zi037_rel in (NULL, -999999, -999999.0), 'zi037_rel', '')"),
		"facilitypnt" -> List("ffn,zi014_ppo,pcf", "(f_code='AL010' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL010' AND (zi014_ppo in (NULL, -999999, -999999.0))) OR (f_code='AL010' AND (ffn in (NULL, -999999, -999999.0)))", "if(f_code='AL010' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL010' AND zi014_ppo in (NULL, -999999, -999999.0), 'zi014_ppo', ''),if(f_code='AL010' AND ffn in (NULL, -999999, -999999.0), 'ffn', '')"),
		"hydrographycrv" -> List("mcc,sbb,cwt,rle,trs,zi024_hyp,atc,pcf", "(f_code='BH010' AND (atc in (NULL, -999999, -999999.0))) OR (f_code='BH010' AND (cwt in (NULL, -999999, -999999.0))) OR (f_code='BH010' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BH010' AND (rle in (NULL, -999999, -999999.0))) OR (f_code='BH010' AND (sbb in (NULL, -999999, -999999.0))) OR (f_code='BI020' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BI020' AND (mcc in (NULL, -999999, -999999.0))) OR (f_code='BI020' AND (trs in (NULL, -999999, -999999.0))) OR (f_code='BH140' AND (zi024_hyp in (NULL, -999999, -999999.0)))", "if(f_code='BH010' AND atc in (NULL, -999999, -999999.0), 'atc', ''),if(f_code='BH010' AND cwt in (NULL, -999999, -999999.0), 'cwt', ''),if(f_code='BH010' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BH010' AND rle in (NULL, -999999, -999999.0), 'rle', ''),if(f_code='BH010' AND sbb in (NULL, -999999, -999999.0), 'sbb', ''),if(f_code='BI020' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BI020' AND mcc in (NULL, -999999, -999999.0), 'mcc', ''),if(f_code='BI020' AND trs in (NULL, -999999, -999999.0), 'trs', ''),if(f_code='BH140' AND zi024_hyp in (NULL, -999999, -999999.0), 'zi024_hyp', '')"),
		"hydrographypnt" -> List("wst,mns,iwt,ocs,zi024_hyp,pcf", "(f_code='BH082' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BH082' AND (iwt in (NULL, -999999, -999999.0))) OR (f_code='BH082' AND (mns in (NULL, -999999, -999999.0))) OR (f_code='BH082' AND (zi024_hyp in (NULL, -999999, -999999.0))) OR (f_code='BH170' AND (zi024_hyp in (NULL, -999999, -999999.0))) OR (f_code='BD115' AND (ocs in (NULL, -999999, -999999.0))) OR (f_code='BH145' AND (wst in (NULL, -999999, -999999.0))) OR (f_code='BH230' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='BH082' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BH082' AND iwt in (NULL, -999999, -999999.0), 'iwt', ''),if(f_code='BH082' AND mns in (NULL, -999999, -999999.0), 'mns', ''),if(f_code='BH082' AND zi024_hyp in (NULL, -999999, -999999.0), 'zi024_hyp', ''),if(f_code='BH170' AND zi024_hyp in (NULL, -999999, -999999.0), 'zi024_hyp', ''),if(f_code='BD115' AND ocs in (NULL, -999999, -999999.0), 'ocs', ''),if(f_code='BH145' AND wst in (NULL, -999999, -999999.0), 'wst', ''),if(f_code='BH230' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"hydrographysrf" -> List("inu", "(f_code='BH090' AND (inu in (NULL, -999999, -999999.0)))", "if(f_code='BH090' AND inu in (NULL, -999999, -999999.0), 'inu', '')"),
		"industrypnt" -> List("zi014_ppo,srl,ppo,ffn,mzn,uma,pcf", "(f_code='AF040' AND (srl in (NULL, -999999, -999999.0))) OR (f_code='AA010' AND (mzn in (NULL, -999999, -999999.0))) OR (f_code='AA010' AND (ffn in (NULL, -999999, -999999.0))) OR (f_code='AA010' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AA010' AND (zi014_ppo in (NULL, -999999, -999999.0))) OR (f_code='AA010' AND (uma in (NULL, -999999, -999999.0))) OR (f_code='AA054' AND (ppo in (NULL, -999999, -999999.0))) OR (f_code='AA040' AND (srl in (NULL, -999999, -999999.0))) OR (f_code='AA040' AND (zi014_ppo in (NULL, -999999, -999999.0)))", "if(f_code='AF040' AND srl in (NULL, -999999, -999999.0), 'srl', ''),if(f_code='AA010' AND mzn in (NULL, -999999, -999999.0), 'mzn', ''),if(f_code='AA010' AND ffn in (NULL, -999999, -999999.0), 'ffn', ''),if(f_code='AA010' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AA010' AND zi014_ppo in (NULL, -999999, -999999.0), 'zi014_ppo', ''),if(f_code='AA010' AND uma in (NULL, -999999, -999999.0), 'uma', ''),if(f_code='AA054' AND ppo in (NULL, -999999, -999999.0), 'ppo', ''),if(f_code='AA040' AND srl in (NULL, -999999, -999999.0), 'srl', ''),if(f_code='AA040' AND zi014_ppo in (NULL, -999999, -999999.0), 'zi014_ppo', '')"),
		"informationpnt" -> List("zi005_fna,nlt", "(f_code='ZD040' AND (nlt in (NULL, -999999, -999999.0))) OR (f_code='ZD040' AND (zi005_fna in (NULL, 'noInformation', 'Null', 'NULL', 'None', '')))", "if(f_code='ZD040' AND nlt in (NULL, -999999, -999999.0), 'nlt', ''),if(f_code='ZD040' AND zi005_fna in (NULL, 'noInformation', 'Null', 'NULL', 'None', ''), 'zi005_fna', '')"),
		"militarypnt" -> List("pcf", "(f_code='SU001' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='SU001' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"physiographycrv" -> List("slt", "(f_code='BA010' AND (slt in (NULL, -999999, -999999.0)))", "if(f_code='BA010' AND slt in (NULL, -999999, -999999.0), 'slt', '')"),
		"physiographypnt" -> List("vgt", "(f_code='DB180' AND (vgt in (NULL, -999999, -999999.0)))", "if(f_code='DB180' AND vgt in (NULL, -999999, -999999.0), 'vgt', '')"),
		"physiographysrf" -> List("tsm", "(f_code='DA010' AND (tsm in (NULL, -999999, -999999.0)))", "if(f_code='DA010' AND tsm in (NULL, -999999, -999999.0), 'tsm', '')"),
		"portharbourcrv" -> List("wle,pwc", "(f_code='BB081' AND (pwc in (NULL, -999999, -999999.0))) OR (f_code='BB081' AND (wle in (NULL, -999999, -999999.0))) OR (f_code='BB082' AND (wle in (NULL, -999999, -999999.0)))", "if(f_code='BB081' AND pwc in (NULL, -999999, -999999.0), 'pwc', ''),if(f_code='BB081' AND wle in (NULL, -999999, -999999.0), 'wle', ''),if(f_code='BB082' AND wle in (NULL, -999999, -999999.0), 'wle', '')"),
		"settlementpnt" -> List("bac,pcf", "(f_code='AL020' AND (bac in (NULL, -999999, -999999.0))) OR (f_code='AL020' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='AL020' AND bac in (NULL, -999999, -999999.0), 'bac', ''),if(f_code='AL020' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"storagepnt" -> List("ppo,ssc,lun,pcf", "(f_code='AM075' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AM065' AND (lun in (NULL, -999999, -999999.0))) OR (f_code='AM065' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AM010' AND (lun in (NULL, -999999, -999999.0))) OR (f_code='AM070' AND (ssc in (NULL, -999999, -999999.0))) OR (f_code='AM070' AND (ppo in (NULL, -999999, -999999.0))) OR (f_code='AM071' AND (ppo in (NULL, -999999, -999999.0)))", "if(f_code='AM075' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AM065' AND lun in (NULL, -999999, -999999.0), 'lun', ''),if(f_code='AM065' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AM010' AND lun in (NULL, -999999, -999999.0), 'lun', ''),if(f_code='AM070' AND ssc in (NULL, -999999, -999999.0), 'ssc', ''),if(f_code='AM070' AND ppo in (NULL, -999999, -999999.0), 'ppo', ''),if(f_code='AM071' AND ppo in (NULL, -999999, -999999.0), 'ppo', '')"),
		"structurepnt" -> List("aoo,ttc,pcf", "(f_code='AL013' AND (aoo in (NULL, -999999, -999999.0))) OR (f_code='AL013' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL241' AND (ttc in (NULL, -999999, -999999.0)))", "if(f_code='AL013' AND aoo in (NULL, -999999, -999999.0), 'aoo', ''),if(f_code='AL013' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL241' AND ttc in (NULL, -999999, -999999.0), 'ttc', '')"),
		"transportationgroundcrv" -> List("ltn,lzn,sep,zi016_wd1,zi017_rra,sbb,rrc,bot,zi016_roc,pcf,zi016_wtc,rle,gtc,wid,mes,cwt,zi017_rgc,wle,rin_roi,loc,zi017_rir,trs", "(f_code='AQ040' AND (bot in (NULL, -999999, -999999.0))) OR (f_code='AQ040' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AQ040' AND (trs in (NULL, -999999, -999999.0))) OR (f_code='AQ040' AND (wid in (NULL, -999999, -999999.0))) OR (f_code='AP010' AND (loc in (NULL, -999999, -999999.0))) OR (f_code='AP010' AND (cwt in (NULL, -999999, -999999.0))) OR (f_code='AP010' AND (sbb in (NULL, -999999, -999999.0))) OR (f_code='AP010' AND (rle in (NULL, -999999, -999999.0))) OR (f_code='AP010' AND (zi016_wtc in (NULL, -999999, -999999.0))) OR (f_code='AQ063' AND (wle in (NULL, -999999, -999999.0))) OR (f_code='AQ065' AND (lzn in (NULL, -999999, -999999.0))) OR (f_code='AQ065' AND (wid in (NULL, -999999, -999999.0))) OR (f_code='AP040' AND (gtc in (NULL, -999999, -999999.0))) OR (f_code='AN050' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AN050' AND (ltn in (NULL, -999999, -999999.0))) OR (f_code='AN050' AND (rle in (NULL, -999999, -999999.0))) OR (f_code='AN050' AND (cwt in (NULL, -999999, -999999.0))) OR (f_code='AN050' AND (zi017_rgc in (NULL, -999999, -999999.0))) OR (f_code='AN050' AND (zi017_rra in (NULL, -999999, -999999.0))) OR (f_code='AN050' AND (sbb in (NULL, -999999, -999999.0))) OR (f_code='AN050' AND (loc in (NULL, -999999, -999999.0))) OR (f_code='AN010' AND (cwt in (NULL, -999999, -999999.0))) OR (f_code='AN010' AND (loc in (NULL, -999999, -999999.0))) OR (f_code='AN010' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AN010' AND (rle in (NULL, -999999, -999999.0))) OR (f_code='AN010' AND (sbb in (NULL, -999999, -999999.0))) OR (f_code='AN010' AND (ltn in (NULL, -999999, -999999.0))) OR (f_code='AN010' AND (zi017_rgc in (NULL, -999999, -999999.0))) OR (f_code='AN010' AND (zi017_rir in (NULL, -999999, -999999.0))) OR (f_code='AN010' AND (zi017_rra in (NULL, -999999, -999999.0))) OR (f_code='AN010' AND (rrc in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (mes in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (sep in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (zi016_roc in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (zi016_wd1 in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (loc in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (ltn in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (cwt in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (sbb in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (rle in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (zi016_wtc in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (rin_roi in (NULL, -999999, -999999.0))) OR (f_code='AP050' AND (loc in (NULL, -999999, -999999.0))) OR (f_code='AP050' AND (cwt in (NULL, -999999, -999999.0))) OR (f_code='AP050' AND (sbb in (NULL, -999999, -999999.0))) OR (f_code='AP050' AND (rle in (NULL, -999999, -999999.0))) OR (f_code='AP050' AND (zi016_wtc in (NULL, -999999, -999999.0))) OR (f_code='AQ130' AND (trs in (NULL, -999999, -999999.0))) OR (f_code='AQ130' AND (wid in (NULL, -999999, -999999.0)))", "if(f_code='AQ040' AND bot in (NULL, -999999, -999999.0), 'bot', ''),if(f_code='AQ040' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AQ040' AND trs in (NULL, -999999, -999999.0), 'trs', ''),if(f_code='AQ040' AND wid in (NULL, -999999, -999999.0), 'wid', ''),if(f_code='AP010' AND loc in (NULL, -999999, -999999.0), 'loc', ''),if(f_code='AP010' AND cwt in (NULL, -999999, -999999.0), 'cwt', ''),if(f_code='AP010' AND sbb in (NULL, -999999, -999999.0), 'sbb', ''),if(f_code='AP010' AND rle in (NULL, -999999, -999999.0), 'rle', ''),if(f_code='AP010' AND zi016_wtc in (NULL, -999999, -999999.0), 'zi016_wtc', ''),if(f_code='AQ063' AND wle in (NULL, -999999, -999999.0), 'wle', ''),if(f_code='AQ065' AND lzn in (NULL, -999999, -999999.0), 'lzn', ''),if(f_code='AQ065' AND wid in (NULL, -999999, -999999.0), 'wid', ''),if(f_code='AP040' AND gtc in (NULL, -999999, -999999.0), 'gtc', ''),if(f_code='AN050' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AN050' AND ltn in (NULL, -999999, -999999.0), 'ltn', ''),if(f_code='AN050' AND rle in (NULL, -999999, -999999.0), 'rle', ''),if(f_code='AN050' AND cwt in (NULL, -999999, -999999.0), 'cwt', ''),if(f_code='AN050' AND zi017_rgc in (NULL, -999999, -999999.0), 'zi017_rgc', ''),if(f_code='AN050' AND zi017_rra in (NULL, -999999, -999999.0), 'zi017_rra', ''),if(f_code='AN050' AND sbb in (NULL, -999999, -999999.0), 'sbb', ''),if(f_code='AN050' AND loc in (NULL, -999999, -999999.0), 'loc', ''),if(f_code='AN010' AND cwt in (NULL, -999999, -999999.0), 'cwt', ''),if(f_code='AN010' AND loc in (NULL, -999999, -999999.0), 'loc', ''),if(f_code='AN010' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AN010' AND rle in (NULL, -999999, -999999.0), 'rle', ''),if(f_code='AN010' AND sbb in (NULL, -999999, -999999.0), 'sbb', ''),if(f_code='AN010' AND ltn in (NULL, -999999, -999999.0), 'ltn', ''),if(f_code='AN010' AND zi017_rgc in (NULL, -999999, -999999.0), 'zi017_rgc', ''),if(f_code='AN010' AND zi017_rir in (NULL, -999999, -999999.0), 'zi017_rir', ''),if(f_code='AN010' AND zi017_rra in (NULL, -999999, -999999.0), 'zi017_rra', ''),if(f_code='AN010' AND rrc in (NULL, -999999, -999999.0), 'rrc', ''),if(f_code='AP030' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AP030' AND mes in (NULL, -999999, -999999.0), 'mes', ''),if(f_code='AP030' AND sep in (NULL, -999999, -999999.0), 'sep', ''),if(f_code='AP030' AND zi016_roc in (NULL, -999999, -999999.0), 'zi016_roc', ''),if(f_code='AP030' AND zi016_wd1 in (NULL, -999999, -999999.0), 'zi016_wd1', ''),if(f_code='AP030' AND loc in (NULL, -999999, -999999.0), 'loc', ''),if(f_code='AP030' AND ltn in (NULL, -999999, -999999.0), 'ltn', ''),if(f_code='AP030' AND cwt in (NULL, -999999, -999999.0), 'cwt', ''),if(f_code='AP030' AND sbb in (NULL, -999999, -999999.0), 'sbb', ''),if(f_code='AP030' AND rle in (NULL, -999999, -999999.0), 'rle', ''),if(f_code='AP030' AND zi016_wtc in (NULL, -999999, -999999.0), 'zi016_wtc', ''),if(f_code='AP030' AND rin_roi in (NULL, -999999, -999999.0), 'rin_roi', ''),if(f_code='AP050' AND loc in (NULL, -999999, -999999.0), 'loc', ''),if(f_code='AP050' AND cwt in (NULL, -999999, -999999.0), 'cwt', ''),if(f_code='AP050' AND sbb in (NULL, -999999, -999999.0), 'sbb', ''),if(f_code='AP050' AND rle in (NULL, -999999, -999999.0), 'rle', ''),if(f_code='AP050' AND zi016_wtc in (NULL, -999999, -999999.0), 'zi016_wtc', ''),if(f_code='AQ130' AND trs in (NULL, -999999, -999999.0), 'trs', ''),if(f_code='AQ130' AND wid in (NULL, -999999, -999999.0), 'wid', '')"),
		"transportationgroundpnt" -> List("wid,ffn,lun,trs,pcf", "(f_code='AH070' AND (ffn in (NULL, -999999, -999999.0))) OR (f_code='AQ141' AND (lun in (NULL, -999999, -999999.0))) OR (f_code='AN075' AND (wid in (NULL, -999999, -999999.0))) OR (f_code='AQ125' AND (ffn in (NULL, -999999, -999999.0))) OR (f_code='AQ125' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AQ125' AND (trs in (NULL, -999999, -999999.0)))", "if(f_code='AH070' AND ffn in (NULL, -999999, -999999.0), 'ffn', ''),if(f_code='AQ141' AND lun in (NULL, -999999, -999999.0), 'lun', ''),if(f_code='AN075' AND wid in (NULL, -999999, -999999.0), 'wid', ''),if(f_code='AQ125' AND ffn in (NULL, -999999, -999999.0), 'ffn', ''),if(f_code='AQ125' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AQ125' AND trs in (NULL, -999999, -999999.0), 'trs', '')"),
		"transportationwatercrv" -> List("trs,zi024_hyp,bgt", "(f_code='BI045' AND (bgt in (NULL, -999999, -999999.0))) OR (f_code='BH020' AND (zi024_hyp in (NULL, -999999, -999999.0))) OR (f_code='AQ070' AND (trs in (NULL, -999999, -999999.0)))", "if(f_code='BI045' AND bgt in (NULL, -999999, -999999.0), 'bgt', ''),if(f_code='BH020' AND zi024_hyp in (NULL, -999999, -999999.0), 'zi024_hyp', ''),if(f_code='AQ070' AND trs in (NULL, -999999, -999999.0), 'trs', '')"),
		"utilityinfrastructurecrv" -> List("cab,sbb,cwt,ppo,rle,rta,loc", "(f_code='AT005' AND (cab in (NULL, -999999, -999999.0))) OR (f_code='AQ113' AND (loc in (NULL, -999999, -999999.0))) OR (f_code='AQ113' AND (ppo in (NULL, -999999, -999999.0))) OR (f_code='AQ113' AND (rta in (NULL, -999999, -999999.0))) OR (f_code='AQ113' AND (rle in (NULL, -999999, -999999.0))) OR (f_code='AQ113' AND (cwt in (NULL, -999999, -999999.0))) OR (f_code='AQ113' AND (sbb in (NULL, -999999, -999999.0)))", "if(f_code='AT005' AND cab in (NULL, -999999, -999999.0), 'cab', ''),if(f_code='AQ113' AND loc in (NULL, -999999, -999999.0), 'loc', ''),if(f_code='AQ113' AND ppo in (NULL, -999999, -999999.0), 'ppo', ''),if(f_code='AQ113' AND rta in (NULL, -999999, -999999.0), 'rta', ''),if(f_code='AQ113' AND rle in (NULL, -999999, -999999.0), 'rle', ''),if(f_code='AQ113' AND cwt in (NULL, -999999, -999999.0), 'cwt', ''),if(f_code='AQ113' AND sbb in (NULL, -999999, -999999.0), 'sbb', '')"),
		"utilityinfrastructurepnt" -> List("tos,pos,at005_cab,pcf", "(f_code='AD010' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AD010' AND (pos in (NULL, -999999, -999999.0))) OR (f_code='AT042' AND (at005_cab in (NULL, -999999, -999999.0))) OR (f_code='AT042' AND (tos in (NULL, -999999, -999999.0)))", "if(f_code='AD010' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AD010' AND pos in (NULL, -999999, -999999.0), 'pos', ''),if(f_code='AT042' AND at005_cab in (NULL, -999999, -999999.0), 'at005_cab', ''),if(f_code='AT042' AND tos in (NULL, -999999, -999999.0), 'tos', '')"),
		"vegetationcrv" -> List("dmt,tre", "(f_code='EC015' AND (dmt in (NULL, -999999, -999999.0))) OR (f_code='EC015' AND (tre in (NULL, -999999, -999999.0)))", "if(f_code='EC015' AND dmt in (NULL, -999999, -999999.0), 'dmt', ''),if(f_code='EC015' AND tre in (NULL, -999999, -999999.0), 'tre', '')"),
		"vegetationsrf" -> List("vsp", "(f_code='ED020' AND (vsp in (NULL, -999999, -999999.0)))", "if(f_code='ED020' AND vsp in (NULL, -999999, -999999.0), 'vsp', '')")
	),
	"psg" -> Map(
		"aeronauticcrv" -> List("zi019_asu,sbb,zi019_asx,zi019_sfs,axs,pcf,zi019_asp,txp", "(f_code='GB075' AND (axs in (NULL, -999999, -999999.0))) OR (f_code='GB075' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='GB075' AND (sbb in (NULL, -999999, -999999.0))) OR (f_code='GB075' AND (txp in (NULL, -999999, -999999.0))) OR (f_code='GB075' AND (zi019_asp in (NULL, -999999, -999999.0))) OR (f_code='GB075' AND (zi019_asu in (NULL, -999999, -999999.0))) OR (f_code='GB075' AND (zi019_asx in (NULL, -999999, -999999.0))) OR (f_code='GB075' AND (zi019_sfs in (NULL, -999999, -999999.0)))", "if(f_code='GB075' AND axs in (NULL, -999999, -999999.0), 'axs', ''),if(f_code='GB075' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='GB075' AND sbb in (NULL, -999999, -999999.0), 'sbb', ''),if(f_code='GB075' AND txp in (NULL, -999999, -999999.0), 'txp', ''),if(f_code='GB075' AND zi019_asp in (NULL, -999999, -999999.0), 'zi019_asp', ''),if(f_code='GB075' AND zi019_asu in (NULL, -999999, -999999.0), 'zi019_asu', ''),if(f_code='GB075' AND zi019_asx in (NULL, -999999, -999999.0), 'zi019_asx', ''),if(f_code='GB075' AND zi019_sfs in (NULL, -999999, -999999.0), 'zi019_sfs', '')"),
		"aeronauticpnt" -> List("zi019_asu,asy,mcc,wid,pec,apt,lzn,ffn,fpt,zi019_asx,zi019_sfs,axs,haf,trs,pcf", "(f_code='AL351' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AQ060' AND (ffn in (NULL, -999999, -999999.0))) OR (f_code='AQ060' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AQ060' AND (trs in (NULL, -999999, -999999.0))) OR (f_code='AQ110' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='GB005' AND (fpt in (NULL, -999999, -999999.0))) OR (f_code='GB005' AND (apt in (NULL, -999999, -999999.0))) OR (f_code='GB005' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='GB030' AND (zi019_asx in (NULL, -999999, -999999.0))) OR (f_code='GB030' AND (zi019_asu in (NULL, -999999, -999999.0))) OR (f_code='GB030' AND (lzn in (NULL, -999999, -999999.0))) OR (f_code='GB030' AND (zi019_sfs in (NULL, -999999, -999999.0))) OR (f_code='GB030' AND (axs in (NULL, -999999, -999999.0))) OR (f_code='GB030' AND (haf in (NULL, -999999, -999999.0))) OR (f_code='GB030' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='GB030' AND (wid in (NULL, -999999, -999999.0))) OR (f_code='GB035' AND (asy in (NULL, -999999, -999999.0))) OR (f_code='GB035' AND (fpt in (NULL, -999999, -999999.0))) OR (f_code='GB035' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='GB040' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='GB040' AND (mcc in (NULL, -999999, -999999.0))) OR (f_code='GB065' AND (asy in (NULL, -999999, -999999.0))) OR (f_code='GB065' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='GB065' AND (pec in (NULL, -999999, -999999.0))) OR (f_code='GB230' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='GB250' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='AL351' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AQ060' AND ffn in (NULL, -999999, -999999.0), 'ffn', ''),if(f_code='AQ060' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AQ060' AND trs in (NULL, -999999, -999999.0), 'trs', ''),if(f_code='AQ110' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='GB005' AND fpt in (NULL, -999999, -999999.0), 'fpt', ''),if(f_code='GB005' AND apt in (NULL, -999999, -999999.0), 'apt', ''),if(f_code='GB005' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='GB030' AND zi019_asx in (NULL, -999999, -999999.0), 'zi019_asx', ''),if(f_code='GB030' AND zi019_asu in (NULL, -999999, -999999.0), 'zi019_asu', ''),if(f_code='GB030' AND lzn in (NULL, -999999, -999999.0), 'lzn', ''),if(f_code='GB030' AND zi019_sfs in (NULL, -999999, -999999.0), 'zi019_sfs', ''),if(f_code='GB030' AND axs in (NULL, -999999, -999999.0), 'axs', ''),if(f_code='GB030' AND haf in (NULL, -999999, -999999.0), 'haf', ''),if(f_code='GB030' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='GB030' AND wid in (NULL, -999999, -999999.0), 'wid', ''),if(f_code='GB035' AND asy in (NULL, -999999, -999999.0), 'asy', ''),if(f_code='GB035' AND fpt in (NULL, -999999, -999999.0), 'fpt', ''),if(f_code='GB035' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='GB040' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='GB040' AND mcc in (NULL, -999999, -999999.0), 'mcc', ''),if(f_code='GB065' AND asy in (NULL, -999999, -999999.0), 'asy', ''),if(f_code='GB065' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='GB065' AND pec in (NULL, -999999, -999999.0), 'pec', ''),if(f_code='GB230' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='GB250' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"aeronauticsrf" -> List("zi019_asu,sbb,lzn,zi019_asx,zi019_sfs,axs,zi019_asp,asu,wid,pcf", "(f_code='GB015' AND (zi019_asu in (NULL, -999999, -999999.0))) OR (f_code='GB015' AND (zi019_asx in (NULL, -999999, -999999.0))) OR (f_code='GB015' AND (zi019_sfs in (NULL, -999999, -999999.0))) OR (f_code='GB015' AND (axs in (NULL, -999999, -999999.0))) OR (f_code='GB015' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='GB045' AND (asu in (NULL, -999999, -999999.0))) OR (f_code='GB045' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='GB055' AND (axs in (NULL, -999999, -999999.0))) OR (f_code='GB055' AND (lzn in (NULL, -999999, -999999.0))) OR (f_code='GB055' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='GB055' AND (sbb in (NULL, -999999, -999999.0))) OR (f_code='GB055' AND (wid in (NULL, -999999, -999999.0))) OR (f_code='GB055' AND (zi019_asp in (NULL, -999999, -999999.0))) OR (f_code='GB055' AND (zi019_asu in (NULL, -999999, -999999.0))) OR (f_code='GB055' AND (zi019_asx in (NULL, -999999, -999999.0))) OR (f_code='GB055' AND (zi019_sfs in (NULL, -999999, -999999.0))) OR (f_code='GB070' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='GB015' AND zi019_asu in (NULL, -999999, -999999.0), 'zi019_asu', ''),if(f_code='GB015' AND zi019_asx in (NULL, -999999, -999999.0), 'zi019_asx', ''),if(f_code='GB015' AND zi019_sfs in (NULL, -999999, -999999.0), 'zi019_sfs', ''),if(f_code='GB015' AND axs in (NULL, -999999, -999999.0), 'axs', ''),if(f_code='GB015' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='GB045' AND asu in (NULL, -999999, -999999.0), 'asu', ''),if(f_code='GB045' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='GB055' AND axs in (NULL, -999999, -999999.0), 'axs', ''),if(f_code='GB055' AND lzn in (NULL, -999999, -999999.0), 'lzn', ''),if(f_code='GB055' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='GB055' AND sbb in (NULL, -999999, -999999.0), 'sbb', ''),if(f_code='GB055' AND wid in (NULL, -999999, -999999.0), 'wid', ''),if(f_code='GB055' AND zi019_asp in (NULL, -999999, -999999.0), 'zi019_asp', ''),if(f_code='GB055' AND zi019_asu in (NULL, -999999, -999999.0), 'zi019_asu', ''),if(f_code='GB055' AND zi019_asx in (NULL, -999999, -999999.0), 'zi019_asx', ''),if(f_code='GB055' AND zi019_sfs in (NULL, -999999, -999999.0), 'zi019_sfs', ''),if(f_code='GB070' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"agriculturepnt" -> List("zi013_csp,ffn,zi014_ppo,pcf", "(f_code='AJ030' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AJ050' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AJ085' AND (ffn in (NULL, -999999, -999999.0))) OR (f_code='AJ085' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AJ110' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL270' AND (zi013_csp in (NULL, -999999, -999999.0))) OR (f_code='AL270' AND (zi014_ppo in (NULL, -999999, -999999.0))) OR (f_code='AL270' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AM020' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BH051' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='AJ030' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AJ050' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AJ085' AND ffn in (NULL, -999999, -999999.0), 'ffn', ''),if(f_code='AJ085' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AJ110' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL270' AND zi013_csp in (NULL, -999999, -999999.0), 'zi013_csp', ''),if(f_code='AL270' AND zi014_ppo in (NULL, -999999, -999999.0), 'zi014_ppo', ''),if(f_code='AL270' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AM020' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BH051' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"agriculturesrf" -> List("zi013_pig", "(f_code='EA010' AND (zi013_pig in (NULL, -999999, -999999.0)))", "if(f_code='EA010' AND zi013_pig in (NULL, -999999, -999999.0), 'zi013_pig', '')"),
		"boundarypnt" -> List("pcf", "(f_code='ZB030' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='ZB030' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"culturecrv" -> List("ssc,pcf", "(f_code='AL130' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL130' AND (ssc in (NULL, -999999, -999999.0)))", "if(f_code='AL130' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL130' AND ssc in (NULL, -999999, -999999.0), 'ssc', '')"),
		"culturepnt" -> List("zi037_rel,ssc,tty,pcf", "(f_code='AL012' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL025' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL030' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL030' AND (zi037_rel in (NULL, -999999, -999999.0))) OR (f_code='AL036' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL036' AND (ssc in (NULL, -999999, -999999.0))) OR (f_code='AL036' AND (tty in (NULL, -999999, -999999.0))) OR (f_code='AL036' AND (zi037_rel in (NULL, -999999, -999999.0))) OR (f_code='BH075' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='AL012' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL025' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL030' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL030' AND zi037_rel in (NULL, -999999, -999999.0), 'zi037_rel', ''),if(f_code='AL036' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL036' AND ssc in (NULL, -999999, -999999.0), 'ssc', ''),if(f_code='AL036' AND tty in (NULL, -999999, -999999.0), 'tty', ''),if(f_code='AL036' AND zi037_rel in (NULL, -999999, -999999.0), 'zi037_rel', ''),if(f_code='BH075' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"culturesrf" -> List("pcf", "(f_code='AK120' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='AK120' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"facilitypnt" -> List("ffn,pcf", "(f_code='AL010' AND (ffn in (NULL, -999999, -999999.0))) OR (f_code='AL010' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AT045' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='AL010' AND ffn in (NULL, -999999, -999999.0), 'ffn', ''),if(f_code='AL010' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AT045' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"facilitysrf" -> List("pcf", "(f_code='AG030' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='AG030' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"hydroaidnavigationpnt" -> List("pcf", "(f_code='BC050' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='BC050' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"hydrographycrv" -> List("cda,mcc,sbb,cwt,woc,nvs,tid,rle,aoo,dft,dfu,fcs,wcc,loc,trs,zi024_hyp,atc,pcf", "(f_code='BH010' AND (atc in (NULL, -999999, -999999.0))) OR (f_code='BH010' AND (cda in (NULL, -999999, -999999.0))) OR (f_code='BH010' AND (cwt in (NULL, -999999, -999999.0))) OR (f_code='BH010' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BH010' AND (rle in (NULL, -999999, -999999.0))) OR (f_code='BH010' AND (sbb in (NULL, -999999, -999999.0))) OR (f_code='BH010' AND (zi024_hyp in (NULL, -999999, -999999.0))) OR (f_code='BH030' AND (cda in (NULL, -999999, -999999.0))) OR (f_code='BH030' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BH030' AND (zi024_hyp in (NULL, -999999, -999999.0))) OR (f_code='BH065' AND (loc in (NULL, -999999, -999999.0))) OR (f_code='BH065' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BH065' AND (rle in (NULL, -999999, -999999.0))) OR (f_code='BH100' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BH100' AND (zi024_hyp in (NULL, -999999, -999999.0))) OR (f_code='BH110' AND (loc in (NULL, -999999, -999999.0))) OR (f_code='BH110' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BH140' AND (cda in (NULL, -999999, -999999.0))) OR (f_code='BH140' AND (loc in (NULL, -999999, -999999.0))) OR (f_code='BH140' AND (nvs in (NULL, -999999, -999999.0))) OR (f_code='BH140' AND (tid in (NULL, -999999, -999999.0))) OR (f_code='BH140' AND (wcc in (NULL, -999999, -999999.0))) OR (f_code='BH140' AND (zi024_hyp in (NULL, -999999, -999999.0))) OR (f_code='BH165' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BI020' AND (woc in (NULL, -999999, -999999.0))) OR (f_code='BI020' AND (dft in (NULL, -999999, -999999.0))) OR (f_code='BI020' AND (dfu in (NULL, -999999, -999999.0))) OR (f_code='BI020' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BI020' AND (mcc in (NULL, -999999, -999999.0))) OR (f_code='BI020' AND (trs in (NULL, -999999, -999999.0))) OR (f_code='BI040' AND (aoo in (NULL, -999999, -999999.0))) OR (f_code='BI040' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BI044' AND (fcs in (NULL, -999999, -999999.0))) OR (f_code='BI044' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BI044' AND (mcc in (NULL, -999999, -999999.0)))", "if(f_code='BH010' AND atc in (NULL, -999999, -999999.0), 'atc', ''),if(f_code='BH010' AND cda in (NULL, -999999, -999999.0), 'cda', ''),if(f_code='BH010' AND cwt in (NULL, -999999, -999999.0), 'cwt', ''),if(f_code='BH010' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BH010' AND rle in (NULL, -999999, -999999.0), 'rle', ''),if(f_code='BH010' AND sbb in (NULL, -999999, -999999.0), 'sbb', ''),if(f_code='BH010' AND zi024_hyp in (NULL, -999999, -999999.0), 'zi024_hyp', ''),if(f_code='BH030' AND cda in (NULL, -999999, -999999.0), 'cda', ''),if(f_code='BH030' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BH030' AND zi024_hyp in (NULL, -999999, -999999.0), 'zi024_hyp', ''),if(f_code='BH065' AND loc in (NULL, -999999, -999999.0), 'loc', ''),if(f_code='BH065' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BH065' AND rle in (NULL, -999999, -999999.0), 'rle', ''),if(f_code='BH100' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BH100' AND zi024_hyp in (NULL, -999999, -999999.0), 'zi024_hyp', ''),if(f_code='BH110' AND loc in (NULL, -999999, -999999.0), 'loc', ''),if(f_code='BH110' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BH140' AND cda in (NULL, -999999, -999999.0), 'cda', ''),if(f_code='BH140' AND loc in (NULL, -999999, -999999.0), 'loc', ''),if(f_code='BH140' AND nvs in (NULL, -999999, -999999.0), 'nvs', ''),if(f_code='BH140' AND tid in (NULL, -999999, -999999.0), 'tid', ''),if(f_code='BH140' AND wcc in (NULL, -999999, -999999.0), 'wcc', ''),if(f_code='BH140' AND zi024_hyp in (NULL, -999999, -999999.0), 'zi024_hyp', ''),if(f_code='BH165' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BI020' AND woc in (NULL, -999999, -999999.0), 'woc', ''),if(f_code='BI020' AND dft in (NULL, -999999, -999999.0), 'dft', ''),if(f_code='BI020' AND dfu in (NULL, -999999, -999999.0), 'dfu', ''),if(f_code='BI020' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BI020' AND mcc in (NULL, -999999, -999999.0), 'mcc', ''),if(f_code='BI020' AND trs in (NULL, -999999, -999999.0), 'trs', ''),if(f_code='BI040' AND aoo in (NULL, -999999, -999999.0), 'aoo', ''),if(f_code='BI040' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BI044' AND fcs in (NULL, -999999, -999999.0), 'fcs', ''),if(f_code='BI044' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BI044' AND mcc in (NULL, -999999, -999999.0), 'mcc', '')"),
		"hydrographypnt" -> List("wst,mns,tid,aoo,azc,dof,iwt,dmd,ocs,zi024_hyp,pcf", "(f_code='BD115' AND (aoo in (NULL, -999999, -999999.0))) OR (f_code='BD115' AND (ocs in (NULL, -999999, -999999.0))) OR (f_code='BD115' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BD181' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BH012' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BH082' AND (dmd in (NULL, -999999, -999999.0))) OR (f_code='BH082' AND (iwt in (NULL, -999999, -999999.0))) OR (f_code='BH082' AND (azc in (NULL, -999999, -999999.0))) OR (f_code='BH082' AND (mns in (NULL, -999999, -999999.0))) OR (f_code='BH082' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BH082' AND (tid in (NULL, -999999, -999999.0))) OR (f_code='BH082' AND (zi024_hyp in (NULL, -999999, -999999.0))) OR (f_code='BH145' AND (wst in (NULL, -999999, -999999.0))) OR (f_code='BH170' AND (dof in (NULL, -999999, -999999.0))) OR (f_code='BH170' AND (zi024_hyp in (NULL, -999999, -999999.0))) OR (f_code='BH230' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BH230' AND (zi024_hyp in (NULL, -999999, -999999.0))) OR (f_code='BI010' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BI050' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='BD115' AND aoo in (NULL, -999999, -999999.0), 'aoo', ''),if(f_code='BD115' AND ocs in (NULL, -999999, -999999.0), 'ocs', ''),if(f_code='BD115' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BD181' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BH012' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BH082' AND dmd in (NULL, -999999, -999999.0), 'dmd', ''),if(f_code='BH082' AND iwt in (NULL, -999999, -999999.0), 'iwt', ''),if(f_code='BH082' AND azc in (NULL, -999999, -999999.0), 'azc', ''),if(f_code='BH082' AND mns in (NULL, -999999, -999999.0), 'mns', ''),if(f_code='BH082' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BH082' AND tid in (NULL, -999999, -999999.0), 'tid', ''),if(f_code='BH082' AND zi024_hyp in (NULL, -999999, -999999.0), 'zi024_hyp', ''),if(f_code='BH145' AND wst in (NULL, -999999, -999999.0), 'wst', ''),if(f_code='BH170' AND dof in (NULL, -999999, -999999.0), 'dof', ''),if(f_code='BH170' AND zi024_hyp in (NULL, -999999, -999999.0), 'zi024_hyp', ''),if(f_code='BH230' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BH230' AND zi024_hyp in (NULL, -999999, -999999.0), 'zi024_hyp', ''),if(f_code='BI010' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BI050' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"hydrographysrf" -> List("inu", "(f_code='BH090' AND (inu in (NULL, -999999, -999999.0)))", "if(f_code='BH090' AND inu in (NULL, -999999, -999999.0), 'inu', '')"),
		"industrycrv" -> List("pcf", "(f_code='AF020' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='AF020' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"industrypnt" -> List("zi014_ppo,srl,ppo,crm,cra,ffn,pby,rip,loc,trs,pcf", "(f_code='AA010' AND (ffn in (NULL, -999999, -999999.0))) OR (f_code='AA010' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AA010' AND (loc in (NULL, -999999, -999999.0))) OR (f_code='AA020' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AA020' AND (ppo in (NULL, -999999, -999999.0))) OR (f_code='AA040' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AA040' AND (ppo in (NULL, -999999, -999999.0))) OR (f_code='AA040' AND (srl in (NULL, -999999, -999999.0))) OR (f_code='AA054' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AA054' AND (ppo in (NULL, -999999, -999999.0))) OR (f_code='AA054' AND (rip in (NULL, -999999, -999999.0))) OR (f_code='AB000' AND (pby in (NULL, -999999, -999999.0))) OR (f_code='AB000' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AC010' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AC020' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AC060' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AF040' AND (crm in (NULL, -999999, -999999.0))) OR (f_code='AF040' AND (cra in (NULL, -999999, -999999.0))) OR (f_code='AF040' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AF040' AND (srl in (NULL, -999999, -999999.0))) OR (f_code='AF040' AND (trs in (NULL, -999999, -999999.0))) OR (f_code='AF070' AND (srl in (NULL, -999999, -999999.0))) OR (f_code='AF070' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AF080' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AJ055' AND (ffn in (NULL, -999999, -999999.0))) OR (f_code='AJ055' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AJ055' AND (zi014_ppo in (NULL, -999999, -999999.0))) OR (f_code='BH155' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='AA010' AND ffn in (NULL, -999999, -999999.0), 'ffn', ''),if(f_code='AA010' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AA010' AND loc in (NULL, -999999, -999999.0), 'loc', ''),if(f_code='AA020' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AA020' AND ppo in (NULL, -999999, -999999.0), 'ppo', ''),if(f_code='AA040' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AA040' AND ppo in (NULL, -999999, -999999.0), 'ppo', ''),if(f_code='AA040' AND srl in (NULL, -999999, -999999.0), 'srl', ''),if(f_code='AA054' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AA054' AND ppo in (NULL, -999999, -999999.0), 'ppo', ''),if(f_code='AA054' AND rip in (NULL, -999999, -999999.0), 'rip', ''),if(f_code='AB000' AND pby in (NULL, -999999, -999999.0), 'pby', ''),if(f_code='AB000' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AC010' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AC020' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AC060' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AF040' AND crm in (NULL, -999999, -999999.0), 'crm', ''),if(f_code='AF040' AND cra in (NULL, -999999, -999999.0), 'cra', ''),if(f_code='AF040' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AF040' AND srl in (NULL, -999999, -999999.0), 'srl', ''),if(f_code='AF040' AND trs in (NULL, -999999, -999999.0), 'trs', ''),if(f_code='AF070' AND srl in (NULL, -999999, -999999.0), 'srl', ''),if(f_code='AF070' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AF080' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AJ055' AND ffn in (NULL, -999999, -999999.0), 'ffn', ''),if(f_code='AJ055' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AJ055' AND zi014_ppo in (NULL, -999999, -999999.0), 'zi014_ppo', ''),if(f_code='BH155' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"industrysrf" -> List("pcf,ppo", "(f_code='AA052' AND (ppo in (NULL, -999999, -999999.0))) OR (f_code='AB010' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AC030' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BH040' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='AA052' AND ppo in (NULL, -999999, -999999.0), 'ppo', ''),if(f_code='AB010' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AC030' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BH040' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"informationcrv" -> List("zi006_mem", "(f_code='ZD045' AND (zi006_mem in (NULL, 'noInformation', 'Null', 'NULL', 'None', '')))", "if(f_code='ZD045' AND zi006_mem in (NULL, 'noInformation', 'Null', 'NULL', 'None', ''), 'zi006_mem', '')"),
		"informationsrf" -> List("vca,vct", "(f_code='ZD020' AND (vca in (NULL, -999999, -999999.0))) OR (f_code='ZD020' AND (vct in (NULL, -999999, -999999.0)))", "if(f_code='ZD020' AND vca in (NULL, -999999, -999999.0), 'vca', ''),if(f_code='ZD020' AND vct in (NULL, -999999, -999999.0), 'vct', '')"),
		"militarycrv" -> List("mcc,eet,pcf", "(f_code='AH025' AND (eet in (NULL, -999999, -999999.0))) OR (f_code='AH025' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL060' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL060' AND (mcc in (NULL, -999999, -999999.0))) OR (f_code='GB050' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='GB050' AND (mcc in (NULL, -999999, -999999.0)))", "if(f_code='AH025' AND eet in (NULL, -999999, -999999.0), 'eet', ''),if(f_code='AH025' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL060' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL060' AND mcc in (NULL, -999999, -999999.0), 'mcc', ''),if(f_code='GB050' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='GB050' AND mcc in (NULL, -999999, -999999.0), 'mcc', '')"),
		"militarypnt" -> List("rle,ffn,caa,pcf", "(f_code='AH055' AND (ffn in (NULL, -999999, -999999.0))) OR (f_code='AH055' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AH055' AND (rle in (NULL, -999999, -999999.0))) OR (f_code='AH060' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL375' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL376' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AM060' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='FA015' AND (ffn in (NULL, -999999, -999999.0))) OR (f_code='FA015' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='FA165' AND (ffn in (NULL, -999999, -999999.0))) OR (f_code='FA165' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='FA165' AND (caa in (NULL, -999999, -999999.0))) OR (f_code='SU001' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='AH055' AND ffn in (NULL, -999999, -999999.0), 'ffn', ''),if(f_code='AH055' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AH055' AND rle in (NULL, -999999, -999999.0), 'rle', ''),if(f_code='AH060' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL375' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL376' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AM060' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='FA015' AND ffn in (NULL, -999999, -999999.0), 'ffn', ''),if(f_code='FA015' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='FA165' AND ffn in (NULL, -999999, -999999.0), 'ffn', ''),if(f_code='FA165' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='FA165' AND caa in (NULL, -999999, -999999.0), 'caa', ''),if(f_code='SU001' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"militarysrf" -> List("pcf", "(f_code='AL065' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='FA100' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='AL065' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='FA100' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"physiographycrv" -> List("mcc,fic,azc,gft,trs,pcf", "(f_code='BA010' AND (azc in (NULL, -999999, -999999.0))) OR (f_code='DB090' AND (fic in (NULL, -999999, -999999.0))) OR (f_code='DB090' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='DB090' AND (mcc in (NULL, -999999, -999999.0))) OR (f_code='DB090' AND (trs in (NULL, -999999, -999999.0))) OR (f_code='DB110' AND (gft in (NULL, -999999, -999999.0)))", "if(f_code='BA010' AND azc in (NULL, -999999, -999999.0), 'azc', ''),if(f_code='DB090' AND fic in (NULL, -999999, -999999.0), 'fic', ''),if(f_code='DB090' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='DB090' AND mcc in (NULL, -999999, -999999.0), 'mcc', ''),if(f_code='DB090' AND trs in (NULL, -999999, -999999.0), 'trs', ''),if(f_code='DB110' AND gft in (NULL, -999999, -999999.0), 'gft', '')"),
		"physiographypnt" -> List("mcc,pcf,aoo,got", "(f_code='BJ060' AND (mcc in (NULL, -999999, -999999.0))) OR (f_code='DB029' AND (aoo in (NULL, -999999, -999999.0))) OR (f_code='DB115' AND (got in (NULL, -999999, -999999.0))) OR (f_code='DB150' AND (aoo in (NULL, -999999, -999999.0))) OR (f_code='EC020' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='BJ060' AND mcc in (NULL, -999999, -999999.0), 'mcc', ''),if(f_code='DB029' AND aoo in (NULL, -999999, -999999.0), 'aoo', ''),if(f_code='DB115' AND got in (NULL, -999999, -999999.0), 'got', ''),if(f_code='DB150' AND aoo in (NULL, -999999, -999999.0), 'aoo', ''),if(f_code='EC020' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"physiographysrf" -> List("sdo,sad,sdt,tsm,sic", "(f_code='BJ100' AND (sic in (NULL, -999999, -999999.0))) OR (f_code='DA010' AND (tsm in (NULL, -999999, -999999.0))) OR (f_code='DB170' AND (sad in (NULL, -999999, -999999.0))) OR (f_code='DB170' AND (sdo in (NULL, -999999, -999999.0))) OR (f_code='DB170' AND (sdt in (NULL, -999999, -999999.0)))", "if(f_code='BJ100' AND sic in (NULL, -999999, -999999.0), 'sic', ''),if(f_code='DA010' AND tsm in (NULL, -999999, -999999.0), 'tsm', ''),if(f_code='DB170' AND sad in (NULL, -999999, -999999.0), 'sad', ''),if(f_code='DB170' AND sdo in (NULL, -999999, -999999.0), 'sdo', ''),if(f_code='DB170' AND sdt in (NULL, -999999, -999999.0), 'sdt', '')"),
		"portharbourcrv" -> List("wle,pwc,pcf", "(f_code='BB081' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BB081' AND (pwc in (NULL, -999999, -999999.0))) OR (f_code='BB082' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BB082' AND (wle in (NULL, -999999, -999999.0)))", "if(f_code='BB081' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BB081' AND pwc in (NULL, -999999, -999999.0), 'pwc', ''),if(f_code='BB082' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BB082' AND wle in (NULL, -999999, -999999.0), 'wle', '')"),
		"portharbourpnt" -> List("zi025_wle,ffn,tid,pcf", "(f_code='BB009' AND (ffn in (NULL, -999999, -999999.0))) OR (f_code='BB009' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BB009' AND (tid in (NULL, -999999, -999999.0))) OR (f_code='BD100' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BD100' AND (zi025_wle in (NULL, -999999, -999999.0)))", "if(f_code='BB009' AND ffn in (NULL, -999999, -999999.0), 'ffn', ''),if(f_code='BB009' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BB009' AND tid in (NULL, -999999, -999999.0), 'tid', ''),if(f_code='BD100' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BD100' AND zi025_wle in (NULL, -999999, -999999.0), 'zi025_wle', '')"),
		"portharboursrf" -> List("mcc,pcf", "(f_code='BB090' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BB199' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BB199' AND (mcc in (NULL, -999999, -999999.0))) OR (f_code='BI005' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='BB090' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BB199' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BB199' AND mcc in (NULL, -999999, -999999.0), 'mcc', ''),if(f_code='BI005' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"recreationcrv" -> List("ama,pcf", "(f_code='AK020' AND (ama in (NULL, -999999, -999999.0))) OR (f_code='AK020' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AK130' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AK150' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='AK020' AND ama in (NULL, -999999, -999999.0), 'ama', ''),if(f_code='AK020' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AK130' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AK150' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"recreationpnt" -> List("ffn,pcf", "(f_code='AK030' AND (ffn in (NULL, -999999, -999999.0))) OR (f_code='AK030' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AK040' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AK060' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AK080' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AK110' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AK160' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AK161' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AK164' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AK170' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AK180' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='AK030' AND ffn in (NULL, -999999, -999999.0), 'ffn', ''),if(f_code='AK030' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AK040' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AK060' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AK080' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AK110' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AK160' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AK161' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AK164' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AK170' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AK180' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"recreationsrf" -> List("pcf", "(f_code='AK070' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AK090' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AK100' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AK101' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL170' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='AK070' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AK090' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AK100' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AK101' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL170' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"settlementpnt" -> List("bac,ffn,pcf", "(f_code='AL020' AND (bac in (NULL, -999999, -999999.0))) OR (f_code='AL020' AND (ffn in (NULL, -999999, -999999.0))) OR (f_code='AL020' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL105' AND (ffn in (NULL, -999999, -999999.0))) OR (f_code='AL105' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='AL020' AND bac in (NULL, -999999, -999999.0), 'bac', ''),if(f_code='AL020' AND ffn in (NULL, -999999, -999999.0), 'ffn', ''),if(f_code='AL020' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL105' AND ffn in (NULL, -999999, -999999.0), 'ffn', ''),if(f_code='AL105' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"settlementsrf" -> List("pcf", "(f_code='AI020' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AI021' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL208' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='AI020' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AI021' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL208' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"storagepnt" -> List("cbp,spt,ffn,lun,ssc,pcf", "(f_code='AM010' AND (ffn in (NULL, -999999, -999999.0))) OR (f_code='AM010' AND (lun in (NULL, -999999, -999999.0))) OR (f_code='AM010' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AM030' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AM065' AND (lun in (NULL, -999999, -999999.0))) OR (f_code='AM065' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AM070' AND (cbp in (NULL, -999999, -999999.0))) OR (f_code='AM070' AND (lun in (NULL, -999999, -999999.0))) OR (f_code='AM070' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AM070' AND (spt in (NULL, -999999, -999999.0))) OR (f_code='AM070' AND (ssc in (NULL, -999999, -999999.0))) OR (f_code='AM071' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AM075' AND (ffn in (NULL, -999999, -999999.0))) OR (f_code='AM075' AND (lun in (NULL, -999999, -999999.0))) OR (f_code='AM075' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AM080' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='AM010' AND ffn in (NULL, -999999, -999999.0), 'ffn', ''),if(f_code='AM010' AND lun in (NULL, -999999, -999999.0), 'lun', ''),if(f_code='AM010' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AM030' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AM065' AND lun in (NULL, -999999, -999999.0), 'lun', ''),if(f_code='AM065' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AM070' AND cbp in (NULL, -999999, -999999.0), 'cbp', ''),if(f_code='AM070' AND lun in (NULL, -999999, -999999.0), 'lun', ''),if(f_code='AM070' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AM070' AND spt in (NULL, -999999, -999999.0), 'spt', ''),if(f_code='AM070' AND ssc in (NULL, -999999, -999999.0), 'ssc', ''),if(f_code='AM071' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AM075' AND ffn in (NULL, -999999, -999999.0), 'ffn', ''),if(f_code='AM075' AND lun in (NULL, -999999, -999999.0), 'lun', ''),if(f_code='AM075' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AM080' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"structurecrv" -> List("bsu,wti,pcf", "(f_code='AL018' AND (bsu in (NULL, -999999, -999999.0))) OR (f_code='AL018' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL070' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL140' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL260' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL260' AND (wti in (NULL, -999999, -999999.0)))", "if(f_code='AL018' AND bsu in (NULL, -999999, -999999.0), 'bsu', ''),if(f_code='AL018' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL070' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL140' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL260' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL260' AND wti in (NULL, -999999, -999999.0), 'wti', '')"),
		"structurepnt" -> List("zi014_ppo,crm,rle,ffn,zi037_rfa,zi037_rel,tos,ttc,pcf", "(f_code='AL013' AND (ffn in (NULL, -999999, -999999.0))) OR (f_code='AL013' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL013' AND (rle in (NULL, -999999, -999999.0))) OR (f_code='AL013' AND (zi037_rel in (NULL, -999999, -999999.0))) OR (f_code='AL013' AND (zi037_rfa in (NULL, -999999, -999999.0))) OR (f_code='AL014' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL014' AND (zi014_ppo in (NULL, -999999, -999999.0))) OR (f_code='AL019' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL080' AND (crm in (NULL, -999999, -999999.0))) OR (f_code='AL080' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL099' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL110' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL110' AND (tos in (NULL, -999999, -999999.0))) OR (f_code='AL142' AND (ffn in (NULL, -999999, -999999.0))) OR (f_code='AL142' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL241' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL241' AND (ttc in (NULL, -999999, -999999.0))) OR (f_code='AL250' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='AL013' AND ffn in (NULL, -999999, -999999.0), 'ffn', ''),if(f_code='AL013' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL013' AND rle in (NULL, -999999, -999999.0), 'rle', ''),if(f_code='AL013' AND zi037_rel in (NULL, -999999, -999999.0), 'zi037_rel', ''),if(f_code='AL013' AND zi037_rfa in (NULL, -999999, -999999.0), 'zi037_rfa', ''),if(f_code='AL014' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL014' AND zi014_ppo in (NULL, -999999, -999999.0), 'zi014_ppo', ''),if(f_code='AL019' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL080' AND crm in (NULL, -999999, -999999.0), 'crm', ''),if(f_code='AL080' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL099' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL110' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL110' AND tos in (NULL, -999999, -999999.0), 'tos', ''),if(f_code='AL142' AND ffn in (NULL, -999999, -999999.0), 'ffn', ''),if(f_code='AL142' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL241' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL241' AND ttc in (NULL, -999999, -999999.0), 'ttc', ''),if(f_code='AL250' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"transportationgroundcrv" -> List("ltn,bsc,sep,rta,fco,zi016_wd1,zi017_rra,zi017_gaw,tst,sbb,rfd,cat,rrc,rwc,bot,owo,pcf,zi016_roc,mcc,rsa,zi016_wtc,rle,tra,gtc,rty,wid,ror,mes,cwt,zi017_rgc,rin_rtn,wle,rin_roi,one,loc,zi017_rir,trs,trp", "(f_code='AL211' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AL211' AND (trp in (NULL, -999999, -999999.0))) OR (f_code='AL211' AND (trs in (NULL, -999999, -999999.0))) OR (f_code='AN010' AND (cwt in (NULL, -999999, -999999.0))) OR (f_code='AN010' AND (loc in (NULL, -999999, -999999.0))) OR (f_code='AN010' AND (ltn in (NULL, -999999, -999999.0))) OR (f_code='AN010' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AN010' AND (rle in (NULL, -999999, -999999.0))) OR (f_code='AN010' AND (rrc in (NULL, -999999, -999999.0))) OR (f_code='AN010' AND (rta in (NULL, -999999, -999999.0))) OR (f_code='AN010' AND (rwc in (NULL, -999999, -999999.0))) OR (f_code='AN010' AND (sbb in (NULL, -999999, -999999.0))) OR (f_code='AN010' AND (zi017_gaw in (NULL, -999999, -999999.0))) OR (f_code='AN010' AND (zi017_rgc in (NULL, -999999, -999999.0))) OR (f_code='AN010' AND (zi017_rir in (NULL, -999999, -999999.0))) OR (f_code='AN010' AND (zi017_rra in (NULL, -999999, -999999.0))) OR (f_code='AN050' AND (cwt in (NULL, -999999, -999999.0))) OR (f_code='AN050' AND (loc in (NULL, -999999, -999999.0))) OR (f_code='AN050' AND (ltn in (NULL, -999999, -999999.0))) OR (f_code='AN050' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AN050' AND (rle in (NULL, -999999, -999999.0))) OR (f_code='AN050' AND (rsa in (NULL, -999999, -999999.0))) OR (f_code='AN050' AND (sbb in (NULL, -999999, -999999.0))) OR (f_code='AN050' AND (zi017_gaw in (NULL, -999999, -999999.0))) OR (f_code='AN050' AND (zi017_rgc in (NULL, -999999, -999999.0))) OR (f_code='AN050' AND (zi017_rir in (NULL, -999999, -999999.0))) OR (f_code='AN050' AND (zi017_rra in (NULL, -999999, -999999.0))) OR (f_code='AP010' AND (cwt in (NULL, -999999, -999999.0))) OR (f_code='AP010' AND (loc in (NULL, -999999, -999999.0))) OR (f_code='AP010' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AP010' AND (rle in (NULL, -999999, -999999.0))) OR (f_code='AP010' AND (sbb in (NULL, -999999, -999999.0))) OR (f_code='AP010' AND (trs in (NULL, -999999, -999999.0))) OR (f_code='AP010' AND (wid in (NULL, -999999, -999999.0))) OR (f_code='AP010' AND (zi016_wtc in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (cwt in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (fco in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (loc in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (ltn in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (mes in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (one in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (rle in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (ror in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (rty in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (rin_roi in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (rin_rtn in (NULL, 'noInformation', 'Null', 'NULL', 'None', ''))) OR (f_code='AP030' AND (sbb in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (sep in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (zi016_roc in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (zi016_wd1 in (NULL, -999999, -999999.0))) OR (f_code='AP030' AND (zi016_wtc in (NULL, -999999, -999999.0))) OR (f_code='AP040' AND (gtc in (NULL, -999999, -999999.0))) OR (f_code='AP040' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AP040' AND (trs in (NULL, -999999, -999999.0))) OR (f_code='AP041' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AP041' AND (trs in (NULL, -999999, -999999.0))) OR (f_code='AP050' AND (cwt in (NULL, -999999, -999999.0))) OR (f_code='AP050' AND (loc in (NULL, -999999, -999999.0))) OR (f_code='AP050' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AP050' AND (rle in (NULL, -999999, -999999.0))) OR (f_code='AP050' AND (sbb in (NULL, -999999, -999999.0))) OR (f_code='AP050' AND (wid in (NULL, -999999, -999999.0))) OR (f_code='AP050' AND (zi016_wtc in (NULL, -999999, -999999.0))) OR (f_code='AQ040' AND (bot in (NULL, -999999, -999999.0))) OR (f_code='AQ040' AND (bsc in (NULL, -999999, -999999.0))) OR (f_code='AQ040' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AQ040' AND (rfd in (NULL, -999999, -999999.0))) OR (f_code='AQ040' AND (trs in (NULL, -999999, -999999.0))) OR (f_code='AQ040' AND (wid in (NULL, -999999, -999999.0))) OR (f_code='AQ063' AND (mcc in (NULL, -999999, -999999.0))) OR (f_code='AQ063' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AQ063' AND (trs in (NULL, -999999, -999999.0))) OR (f_code='AQ063' AND (wle in (NULL, -999999, -999999.0))) OR (f_code='AQ065' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AQ075' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AQ075' AND (wid in (NULL, -999999, -999999.0))) OR (f_code='AQ130' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AQ130' AND (tra in (NULL, -999999, -999999.0))) OR (f_code='AQ130' AND (trs in (NULL, -999999, -999999.0))) OR (f_code='AQ130' AND (wid in (NULL, -999999, -999999.0))) OR (f_code='AT041' AND (tst in (NULL, -999999, -999999.0))) OR (f_code='AT041' AND (cat in (NULL, -999999, -999999.0))) OR (f_code='AT041' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AT041' AND (owo in (NULL, -999999, -999999.0))) OR (f_code='BH070' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BH070' AND (zi016_roc in (NULL, -999999, -999999.0))) OR (f_code='BH070' AND (trs in (NULL, -999999, -999999.0)))", "if(f_code='AL211' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AL211' AND trp in (NULL, -999999, -999999.0), 'trp', ''),if(f_code='AL211' AND trs in (NULL, -999999, -999999.0), 'trs', ''),if(f_code='AN010' AND cwt in (NULL, -999999, -999999.0), 'cwt', ''),if(f_code='AN010' AND loc in (NULL, -999999, -999999.0), 'loc', ''),if(f_code='AN010' AND ltn in (NULL, -999999, -999999.0), 'ltn', ''),if(f_code='AN010' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AN010' AND rle in (NULL, -999999, -999999.0), 'rle', ''),if(f_code='AN010' AND rrc in (NULL, -999999, -999999.0), 'rrc', ''),if(f_code='AN010' AND rta in (NULL, -999999, -999999.0), 'rta', ''),if(f_code='AN010' AND rwc in (NULL, -999999, -999999.0), 'rwc', ''),if(f_code='AN010' AND sbb in (NULL, -999999, -999999.0), 'sbb', ''),if(f_code='AN010' AND zi017_gaw in (NULL, -999999, -999999.0), 'zi017_gaw', ''),if(f_code='AN010' AND zi017_rgc in (NULL, -999999, -999999.0), 'zi017_rgc', ''),if(f_code='AN010' AND zi017_rir in (NULL, -999999, -999999.0), 'zi017_rir', ''),if(f_code='AN010' AND zi017_rra in (NULL, -999999, -999999.0), 'zi017_rra', ''),if(f_code='AN050' AND cwt in (NULL, -999999, -999999.0), 'cwt', ''),if(f_code='AN050' AND loc in (NULL, -999999, -999999.0), 'loc', ''),if(f_code='AN050' AND ltn in (NULL, -999999, -999999.0), 'ltn', ''),if(f_code='AN050' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AN050' AND rle in (NULL, -999999, -999999.0), 'rle', ''),if(f_code='AN050' AND rsa in (NULL, -999999, -999999.0), 'rsa', ''),if(f_code='AN050' AND sbb in (NULL, -999999, -999999.0), 'sbb', ''),if(f_code='AN050' AND zi017_gaw in (NULL, -999999, -999999.0), 'zi017_gaw', ''),if(f_code='AN050' AND zi017_rgc in (NULL, -999999, -999999.0), 'zi017_rgc', ''),if(f_code='AN050' AND zi017_rir in (NULL, -999999, -999999.0), 'zi017_rir', ''),if(f_code='AN050' AND zi017_rra in (NULL, -999999, -999999.0), 'zi017_rra', ''),if(f_code='AP010' AND cwt in (NULL, -999999, -999999.0), 'cwt', ''),if(f_code='AP010' AND loc in (NULL, -999999, -999999.0), 'loc', ''),if(f_code='AP010' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AP010' AND rle in (NULL, -999999, -999999.0), 'rle', ''),if(f_code='AP010' AND sbb in (NULL, -999999, -999999.0), 'sbb', ''),if(f_code='AP010' AND trs in (NULL, -999999, -999999.0), 'trs', ''),if(f_code='AP010' AND wid in (NULL, -999999, -999999.0), 'wid', ''),if(f_code='AP010' AND zi016_wtc in (NULL, -999999, -999999.0), 'zi016_wtc', ''),if(f_code='AP030' AND cwt in (NULL, -999999, -999999.0), 'cwt', ''),if(f_code='AP030' AND fco in (NULL, -999999, -999999.0), 'fco', ''),if(f_code='AP030' AND loc in (NULL, -999999, -999999.0), 'loc', ''),if(f_code='AP030' AND ltn in (NULL, -999999, -999999.0), 'ltn', ''),if(f_code='AP030' AND mes in (NULL, -999999, -999999.0), 'mes', ''),if(f_code='AP030' AND one in (NULL, -999999, -999999.0), 'one', ''),if(f_code='AP030' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AP030' AND rle in (NULL, -999999, -999999.0), 'rle', ''),if(f_code='AP030' AND ror in (NULL, -999999, -999999.0), 'ror', ''),if(f_code='AP030' AND rty in (NULL, -999999, -999999.0), 'rty', ''),if(f_code='AP030' AND rin_roi in (NULL, -999999, -999999.0), 'rin_roi', ''),if(f_code='AP030' AND rin_rtn in (NULL, 'noInformation', 'Null', 'NULL', 'None', ''), 'rin_rtn', ''),if(f_code='AP030' AND sbb in (NULL, -999999, -999999.0), 'sbb', ''),if(f_code='AP030' AND sep in (NULL, -999999, -999999.0), 'sep', ''),if(f_code='AP030' AND zi016_roc in (NULL, -999999, -999999.0), 'zi016_roc', ''),if(f_code='AP030' AND zi016_wd1 in (NULL, -999999, -999999.0), 'zi016_wd1', ''),if(f_code='AP030' AND zi016_wtc in (NULL, -999999, -999999.0), 'zi016_wtc', ''),if(f_code='AP040' AND gtc in (NULL, -999999, -999999.0), 'gtc', ''),if(f_code='AP040' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AP040' AND trs in (NULL, -999999, -999999.0), 'trs', ''),if(f_code='AP041' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AP041' AND trs in (NULL, -999999, -999999.0), 'trs', ''),if(f_code='AP050' AND cwt in (NULL, -999999, -999999.0), 'cwt', ''),if(f_code='AP050' AND loc in (NULL, -999999, -999999.0), 'loc', ''),if(f_code='AP050' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AP050' AND rle in (NULL, -999999, -999999.0), 'rle', ''),if(f_code='AP050' AND sbb in (NULL, -999999, -999999.0), 'sbb', ''),if(f_code='AP050' AND wid in (NULL, -999999, -999999.0), 'wid', ''),if(f_code='AP050' AND zi016_wtc in (NULL, -999999, -999999.0), 'zi016_wtc', ''),if(f_code='AQ040' AND bot in (NULL, -999999, -999999.0), 'bot', ''),if(f_code='AQ040' AND bsc in (NULL, -999999, -999999.0), 'bsc', ''),if(f_code='AQ040' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AQ040' AND rfd in (NULL, -999999, -999999.0), 'rfd', ''),if(f_code='AQ040' AND trs in (NULL, -999999, -999999.0), 'trs', ''),if(f_code='AQ040' AND wid in (NULL, -999999, -999999.0), 'wid', ''),if(f_code='AQ063' AND mcc in (NULL, -999999, -999999.0), 'mcc', ''),if(f_code='AQ063' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AQ063' AND trs in (NULL, -999999, -999999.0), 'trs', ''),if(f_code='AQ063' AND wle in (NULL, -999999, -999999.0), 'wle', ''),if(f_code='AQ065' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AQ075' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AQ075' AND wid in (NULL, -999999, -999999.0), 'wid', ''),if(f_code='AQ130' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AQ130' AND tra in (NULL, -999999, -999999.0), 'tra', ''),if(f_code='AQ130' AND trs in (NULL, -999999, -999999.0), 'trs', ''),if(f_code='AQ130' AND wid in (NULL, -999999, -999999.0), 'wid', ''),if(f_code='AT041' AND tst in (NULL, -999999, -999999.0), 'tst', ''),if(f_code='AT041' AND cat in (NULL, -999999, -999999.0), 'cat', ''),if(f_code='AT041' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AT041' AND owo in (NULL, -999999, -999999.0), 'owo', ''),if(f_code='BH070' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BH070' AND zi016_roc in (NULL, -999999, -999999.0), 'zi016_roc', ''),if(f_code='BH070' AND trs in (NULL, -999999, -999999.0), 'trs', '')"),
		"transportationgroundpnt" -> List("zi017_rgc,ffn,dgc,pym,zi016_roc,zi017_gaw,zi017_rra,trs,pcf", "(f_code='AH070' AND (ffn in (NULL, -999999, -999999.0))) OR (f_code='AH070' AND (trs in (NULL, -999999, -999999.0))) OR (f_code='AN075' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AN075' AND (zi017_gaw in (NULL, -999999, -999999.0))) OR (f_code='AN075' AND (zi017_rgc in (NULL, -999999, -999999.0))) OR (f_code='AN075' AND (zi017_rra in (NULL, -999999, -999999.0))) OR (f_code='AN076' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AN076' AND (zi017_gaw in (NULL, -999999, -999999.0))) OR (f_code='AN076' AND (zi017_rgc in (NULL, -999999, -999999.0))) OR (f_code='AN076' AND (zi017_rra in (NULL, -999999, -999999.0))) OR (f_code='AQ055' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AQ055' AND (pym in (NULL, -999999, -999999.0))) OR (f_code='AQ068' AND (dgc in (NULL, -999999, -999999.0))) OR (f_code='AQ068' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AQ068' AND (trs in (NULL, -999999, -999999.0))) OR (f_code='AQ125' AND (ffn in (NULL, -999999, -999999.0))) OR (f_code='AQ125' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AQ125' AND (trs in (NULL, -999999, -999999.0))) OR (f_code='AQ135' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AQ135' AND (zi016_roc in (NULL, -999999, -999999.0))) OR (f_code='AQ141' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AQ170' AND (ffn in (NULL, -999999, -999999.0))) OR (f_code='AQ170' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='AH070' AND ffn in (NULL, -999999, -999999.0), 'ffn', ''),if(f_code='AH070' AND trs in (NULL, -999999, -999999.0), 'trs', ''),if(f_code='AN075' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AN075' AND zi017_gaw in (NULL, -999999, -999999.0), 'zi017_gaw', ''),if(f_code='AN075' AND zi017_rgc in (NULL, -999999, -999999.0), 'zi017_rgc', ''),if(f_code='AN075' AND zi017_rra in (NULL, -999999, -999999.0), 'zi017_rra', ''),if(f_code='AN076' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AN076' AND zi017_gaw in (NULL, -999999, -999999.0), 'zi017_gaw', ''),if(f_code='AN076' AND zi017_rgc in (NULL, -999999, -999999.0), 'zi017_rgc', ''),if(f_code='AN076' AND zi017_rra in (NULL, -999999, -999999.0), 'zi017_rra', ''),if(f_code='AQ055' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AQ055' AND pym in (NULL, -999999, -999999.0), 'pym', ''),if(f_code='AQ068' AND dgc in (NULL, -999999, -999999.0), 'dgc', ''),if(f_code='AQ068' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AQ068' AND trs in (NULL, -999999, -999999.0), 'trs', ''),if(f_code='AQ125' AND ffn in (NULL, -999999, -999999.0), 'ffn', ''),if(f_code='AQ125' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AQ125' AND trs in (NULL, -999999, -999999.0), 'trs', ''),if(f_code='AQ135' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AQ135' AND zi016_roc in (NULL, -999999, -999999.0), 'zi016_roc', ''),if(f_code='AQ141' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AQ170' AND ffn in (NULL, -999999, -999999.0), 'ffn', ''),if(f_code='AQ170' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"transportationgroundsrf" -> List("zi017_rgc,ffn,zi016_roc,vet,zi017_rra,zi017_gaw,pcf", "(f_code='AN060' AND (ffn in (NULL, -999999, -999999.0))) OR (f_code='AN060' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AN060' AND (zi017_gaw in (NULL, -999999, -999999.0))) OR (f_code='AN060' AND (zi017_rgc in (NULL, -999999, -999999.0))) OR (f_code='AN060' AND (zi017_rra in (NULL, -999999, -999999.0))) OR (f_code='AQ140' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AQ140' AND (vet in (NULL, -999999, -999999.0))) OR (f_code='AQ140' AND (zi016_roc in (NULL, -999999, -999999.0)))", "if(f_code='AN060' AND ffn in (NULL, -999999, -999999.0), 'ffn', ''),if(f_code='AN060' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AN060' AND zi017_gaw in (NULL, -999999, -999999.0), 'zi017_gaw', ''),if(f_code='AN060' AND zi017_rgc in (NULL, -999999, -999999.0), 'zi017_rgc', ''),if(f_code='AN060' AND zi017_rra in (NULL, -999999, -999999.0), 'zi017_rra', ''),if(f_code='AQ140' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AQ140' AND vet in (NULL, -999999, -999999.0), 'vet', ''),if(f_code='AQ140' AND zi016_roc in (NULL, -999999, -999999.0), 'zi016_roc', '')"),
		"transportationwatercrv" -> List("cda,sbb,cwt,rle,fer,loc,trs,zi024_hyp,pcf", "(f_code='AQ070' AND (fer in (NULL, -999999, -999999.0))) OR (f_code='AQ070' AND (trs in (NULL, -999999, -999999.0))) OR (f_code='BH020' AND (cwt in (NULL, -999999, -999999.0))) OR (f_code='BH020' AND (cda in (NULL, -999999, -999999.0))) OR (f_code='BH020' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BH020' AND (rle in (NULL, -999999, -999999.0))) OR (f_code='BH020' AND (sbb in (NULL, -999999, -999999.0))) OR (f_code='BH020' AND (zi024_hyp in (NULL, -999999, -999999.0))) OR (f_code='BH020' AND (loc in (NULL, -999999, -999999.0))) OR (f_code='BI006' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BI030' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='BI045' AND (pcf in (NULL, -999999, -999999.0)))", "if(f_code='AQ070' AND fer in (NULL, -999999, -999999.0), 'fer', ''),if(f_code='AQ070' AND trs in (NULL, -999999, -999999.0), 'trs', ''),if(f_code='BH020' AND cwt in (NULL, -999999, -999999.0), 'cwt', ''),if(f_code='BH020' AND cda in (NULL, -999999, -999999.0), 'cda', ''),if(f_code='BH020' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BH020' AND rle in (NULL, -999999, -999999.0), 'rle', ''),if(f_code='BH020' AND sbb in (NULL, -999999, -999999.0), 'sbb', ''),if(f_code='BH020' AND zi024_hyp in (NULL, -999999, -999999.0), 'zi024_hyp', ''),if(f_code='BH020' AND loc in (NULL, -999999, -999999.0), 'loc', ''),if(f_code='BI006' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BI030' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='BI045' AND pcf in (NULL, -999999, -999999.0), 'pcf', '')"),
		"transportationwaterpnt" -> List("trs,pcf", "(f_code='AQ080' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AQ080' AND (trs in (NULL, -999999, -999999.0)))", "if(f_code='AQ080' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AQ080' AND trs in (NULL, -999999, -999999.0), 'trs', '')"),
		"utilityinfrastructurecrv" -> List("cab,sbb,cwt,plt,cst,rle,spt,rta,loc,owo,tst,pcf", "(f_code='AQ113' AND (cwt in (NULL, -999999, -999999.0))) OR (f_code='AQ113' AND (loc in (NULL, -999999, -999999.0))) OR (f_code='AQ113' AND (owo in (NULL, -999999, -999999.0))) OR (f_code='AQ113' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AQ113' AND (plt in (NULL, -999999, -999999.0))) OR (f_code='AQ113' AND (rle in (NULL, -999999, -999999.0))) OR (f_code='AQ113' AND (rta in (NULL, -999999, -999999.0))) OR (f_code='AQ113' AND (sbb in (NULL, -999999, -999999.0))) OR (f_code='AQ113' AND (spt in (NULL, -999999, -999999.0))) OR (f_code='AT005' AND (tst in (NULL, -999999, -999999.0))) OR (f_code='AT005' AND (cab in (NULL, -999999, -999999.0))) OR (f_code='AT005' AND (cst in (NULL, -999999, -999999.0))) OR (f_code='AT005' AND (owo in (NULL, -999999, -999999.0))) OR (f_code='AT005' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AT005' AND (loc in (NULL, -999999, -999999.0)))", "if(f_code='AQ113' AND cwt in (NULL, -999999, -999999.0), 'cwt', ''),if(f_code='AQ113' AND loc in (NULL, -999999, -999999.0), 'loc', ''),if(f_code='AQ113' AND owo in (NULL, -999999, -999999.0), 'owo', ''),if(f_code='AQ113' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AQ113' AND plt in (NULL, -999999, -999999.0), 'plt', ''),if(f_code='AQ113' AND rle in (NULL, -999999, -999999.0), 'rle', ''),if(f_code='AQ113' AND rta in (NULL, -999999, -999999.0), 'rta', ''),if(f_code='AQ113' AND sbb in (NULL, -999999, -999999.0), 'sbb', ''),if(f_code='AQ113' AND spt in (NULL, -999999, -999999.0), 'spt', ''),if(f_code='AT005' AND tst in (NULL, -999999, -999999.0), 'tst', ''),if(f_code='AT005' AND cab in (NULL, -999999, -999999.0), 'cab', ''),if(f_code='AT005' AND cst in (NULL, -999999, -999999.0), 'cst', ''),if(f_code='AT005' AND owo in (NULL, -999999, -999999.0), 'owo', ''),if(f_code='AT005' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AT005' AND loc in (NULL, -999999, -999999.0), 'loc', '')"),
		"utilityinfrastructurepnt" -> List("zi032_pym,srl,ppo,hgt,pos,zi032_pyc,at005_cab,zi032_tos,pcf", "(f_code='AD010' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AD010' AND (pos in (NULL, -999999, -999999.0))) OR (f_code='AD025' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AD025' AND (ppo in (NULL, -999999, -999999.0))) OR (f_code='AD030' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AD060' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AF010' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AF030' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AJ051' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AJ051' AND (srl in (NULL, -999999, -999999.0))) OR (f_code='AQ116' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AQ116' AND (ppo in (NULL, -999999, -999999.0))) OR (f_code='AT010' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AT012' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AT042' AND (at005_cab in (NULL, -999999, -999999.0))) OR (f_code='AT042' AND (pcf in (NULL, -999999, -999999.0))) OR (f_code='AT042' AND (hgt in (NULL, -999999, -999999.0))) OR (f_code='AT042' AND (zi032_pyc in (NULL, -999999, -999999.0))) OR (f_code='AT042' AND (zi032_pym in (NULL, -999999, -999999.0))) OR (f_code='AT042' AND (zi032_tos in (NULL, -999999, -999999.0)))", "if(f_code='AD010' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AD010' AND pos in (NULL, -999999, -999999.0), 'pos', ''),if(f_code='AD025' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AD025' AND ppo in (NULL, -999999, -999999.0), 'ppo', ''),if(f_code='AD030' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AD060' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AF010' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AF030' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AJ051' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AJ051' AND srl in (NULL, -999999, -999999.0), 'srl', ''),if(f_code='AQ116' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AQ116' AND ppo in (NULL, -999999, -999999.0), 'ppo', ''),if(f_code='AT010' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AT012' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AT042' AND at005_cab in (NULL, -999999, -999999.0), 'at005_cab', ''),if(f_code='AT042' AND pcf in (NULL, -999999, -999999.0), 'pcf', ''),if(f_code='AT042' AND hgt in (NULL, -999999, -999999.0), 'hgt', ''),if(f_code='AT042' AND zi032_pyc in (NULL, -999999, -999999.0), 'zi032_pyc', ''),if(f_code='AT042' AND zi032_pym in (NULL, -999999, -999999.0), 'zi032_pym', ''),if(f_code='AT042' AND zi032_tos in (NULL, -999999, -999999.0), 'zi032_tos', '')"),
		"vegetationcrv" -> List("dmt,tre", "(f_code='EC015' AND (dmt in (NULL, -999999, -999999.0))) OR (f_code='EC015' AND (tre in (NULL, -999999, -999999.0)))", "if(f_code='EC015' AND dmt in (NULL, -999999, -999999.0), 'dmt', ''),if(f_code='EC015' AND tre in (NULL, -999999, -999999.0), 'tre', '')"),
		"vegetationsrf" -> List("tid,tre,veg,zi024_hyp,vsp", "(f_code='EB010' AND (veg in (NULL, -999999, -999999.0))) OR (f_code='ED010' AND (zi024_hyp in (NULL, -999999, -999999.0))) OR (f_code='ED010' AND (tid in (NULL, -999999, -999999.0))) OR (f_code='ED020' AND (tid in (NULL, -999999, -999999.0))) OR (f_code='ED020' AND (tre in (NULL, -999999, -999999.0))) OR (f_code='ED020' AND (vsp in (NULL, -999999, -999999.0)))", "if(f_code='EB010' AND veg in (NULL, -999999, -999999.0), 'veg', ''),if(f_code='ED010' AND zi024_hyp in (NULL, -999999, -999999.0), 'zi024_hyp', ''),if(f_code='ED010' AND tid in (NULL, -999999, -999999.0), 'tid', ''),if(f_code='ED020' AND tid in (NULL, -999999, -999999.0), 'tid', ''),if(f_code='ED020' AND tre in (NULL, -999999, -999999.0), 'tre', ''),if(f_code='ED020' AND vsp in (NULL, -999999, -999999.0), 'vsp', '')")
	)
)

// create the dataframe for the pop grids
var dfMinMaxPG = spark.read.format("jdbc").options(jdbcOptionsOutput).option("query", "SELECT MIN(objectid) objectid_min, MAX(objectid) objectid_max FROM sotd.pop_grid").load()
spark.read.format("jdbc").options(jdbcOptionsOutput).option("lowerBound", dfMinMaxPG.head().getInt(0)).option("upperBound", dfMinMaxPG.head().getInt(1)).option("numPartitions", numPartitions).option("partitionColumn", "objectid").option("dbtable", """
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
spark.read.format("jdbc").options(jdbcOptionsOutput).option("lowerBound", dfMinMaxRPM.head().getInt(0)).option("upperBound", dfMinMaxRPM.head().getInt(1)).option("numPartitions", numPartitions).option("partitionColumn", "objectid").option("dbtable", s"""
(
	SELECT
		objectid
		, CONCAT(FLOOR((st_x(st_centroid(shape)) - -180.0) / 0.25), ':', FLOOR((st_y(st_centroid(shape)) - -90.0) / 0.25)) grid_id
		, ${1998.to(2019).map(f => s"y${f}").mkString(", ")}
	FROM sotd.rpm
	WHERE st_x(st_centroid(shape)) IS NOT NULL
) tbl_rpm
""").load().createOrReplaceTempView("df_rpm")

List("aeronauticcrv", "aeronauticpnt", "aeronauticsrf", "agriculturepnt", "agriculturesrf", "boundarypnt", "culturecrv", "culturepnt", "culturesrf", "facilitypnt", "facilitysrf", "hydroaidnavigationpnt", "hydroaidnavigationsrf", "hydrographycrv", "hydrographypnt", "hydrographysrf", "industrycrv", "industrypnt", "industrysrf", "informationcrv", "informationpnt", "informationsrf", "militarycrv", "militarypnt", "militarysrf", "physiographycrv", "physiographypnt", "physiographysrf", "portharbourcrv", "portharbourpnt", "portharboursrf", "recreationcrv", "recreationpnt", "recreationsrf", "settlementpnt", "settlementsrf", "storagepnt", "storagesrf", "structurecrv", "structurepnt", "structuresrf", "transportationgroundcrv", "transportationgroundpnt", "transportationgroundsrf", "transportationwatercrv", "transportationwaterpnt", "transportationwatersrf", "utilityinfrastructurecrv", "utilityinfrastructurepnt", "utilityinfrastructuresrf", "vegetationcrv", "vegetationpnt", "vegetationsrf").foreach(fcName => {
	try {
		// fetch the partitioning values
		var dfMinMax = spark.read.format("jdbc").options(jdbcOptionsInput).option("query", s"SELECT MIN(objectid) objectid_min, MAX(objectid) objectid_max FROM tds.${fcName}").load()
		var oidMin = dfMinMax.head().getInt(0)
		var oidMax = dfMinMax.head().getInt(1)

		// create the initial dataframe that can be partitioned by objectid
		var dfMain = spark.read.format("jdbc").options(jdbcOptionsInput).option("lowerBound", oidMin).option("upperBound", oidMax).option("numPartitions", numPartitions).option("partitionColumn", "objectid").option("dbtable", s"""
		(
			SELECT
				objectid
				, CONCAT(FLOOR((st_x(st_centroid(shape)) - -180.0) / 0.25), ':', FLOOR((st_y(st_centroid(shape)) - -90.0) / 0.25)) grid_id
				, zi001_sdp
				, zi026_ctuu
				, zi001_sdv
				, aha
			FROM tds.${fcName}
			WHERE st_x(st_centroid(shape)) IS NOT NULL
		) tbl
		""").load()
		dfMain.cache()
		dfMain.createOrReplaceTempView("df_mn")

		spark.sql("""
			SELECT
				grid_id
				, COUNT(1) feature_cnt
				, MIN(aha) pa_min
				, MAX(aha) pa_max
				, AVG(aha) pa_mean
				, MIN(zi026_ctuu) min_scale
				, MAX(zi026_ctuu) max_scale
				, AVG(zi026_ctuu) them_mean
				, percentile_approx(aha, 0.5) pa_median
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
					, zi001_sdp pri_source
					, COUNT(1) pri_source_count
					, row_number() OVER (PARTITION BY grid_id ORDER BY COUNT(1) DESC) my_rank
				FROM df_mn
				GROUP BY grid_id, zi001_sdp
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
					, zi001_sdp sec_source
					, COUNT(1) sec_source_count
					, row_number() OVER (PARTITION BY grid_id ORDER BY COUNT(1) DESC) my_rank
				FROM df_mn
				GROUP BY grid_id, zi001_sdp
			) t
			WHERE my_rank = 2
		""").createOrReplaceTempView("df_ss")

		spark.sql("""
			SELECT
				grid_id
				, concat_ws(',', collect_set(zi001_sdp)) source_list
			FROM (
				SELECT
					grid_id
					, zi001_sdp
				FROM df_mn
				GROUP BY grid_id, zi001_sdp
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
					, zi001_sdv dom_date
					, COUNT(1) dom_date_cnt
					, row_number() OVER (PARTITION BY grid_id ORDER BY COUNT(1) DESC) my_rank
				FROM df_mn
				WHERE zi001_sdv <> 'noInformation'
				GROUP BY grid_id, zi001_sdv
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
					, SUBSTR(TRIM(zi001_sdv), 0, 4) dom_year
					, COUNT(1) dom_year_count
					, row_number() OVER (PARTITION BY grid_id ORDER BY COUNT(1) DESC) my_rank
				FROM df_mn
				WHERE zi001_sdv <> 'noInformation'
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
				, MAX(TO_DATE(zi001_sdv, 'yyyy-MM-dd')) newest_date
				, MIN(TO_DATE(zi001_sdv, 'yyyy-MM-dd')) oldest_date
			FROM df_mn
			WHERE zi001_sdv <> 'noInformation'
			GROUP BY grid_id
		""").createOrReplaceTempView("df_dl")

		spark.sql("""
			SELECT
				grid_id
				, COUNT(1) no_date_cnt
			FROM df_mn
			WHERE zi001_sdv = 'noInformation'
			GROUP BY grid_id
		""").createOrReplaceTempView("df_dn")

		spark.sql("""
			SELECT
				grid_id
				, COUNT(1) cnt_2_year
			FROM df_mn
			WHERE zi001_sdv <> 'noInformation' AND TO_DATE(zi001_sdv, 'yyyy-MM-dd') BETWEEN date_sub(current_date, 365 * 2) AND current_date
			GROUP BY grid_id
		""").createOrReplaceTempView("df_d2")

		List(List(5, 2), List(10, 5), List(15, 10)).foreach(yearDiff => {
			spark.sql(s"""
				SELECT
					grid_id
					, COUNT(1) cnt_${yearDiff(0)}_year
				FROM df_mn
				WHERE zi001_sdv <> 'noInformation' AND TO_DATE(zi001_sdv, 'yyyy-MM-dd') BETWEEN date_sub(current_date, 365 * ${yearDiff(0)}) AND date_sub(current_date, 365 * ${yearDiff(1)})
				GROUP BY grid_id
			""").createOrReplaceTempView(s"df_d${yearDiff(0)}")
		})

		spark.sql("""
			SELECT
				grid_id
				, COUNT(1) cnt_15_year_plus
			FROM df_mn
			WHERE zi001_sdv <> 'noInformation' AND TO_DATE(zi001_sdv, 'yyyy-MM-dd') < date_sub(current_date, 365 * 15)
			GROUP BY grid_id
		""").createOrReplaceTempView("df_d15_plus")

		spark.sql("""
			SELECT
				grid_id
				, aha pa_mode
			FROM (
				SELECT
					grid_id
					, aha
					, COUNT(aha) my_count
					, row_number() OVER (PARTITION BY grid_id ORDER BY COUNT(1) DESC) my_rank
				FROM df_mn
				GROUP BY grid_id, aha
			) t
			WHERE my_rank = 1
		""").createOrReplaceTempView("df_am")

		spark.sql("""
			SELECT
				grid_id
				, COUNT(1) pa_null_cnt
			FROM df_mn
			WHERE aha IS NULL
			GROUP BY grid_id
		""").createOrReplaceTempView("df_an")

		List(2500, 5000, 12500, 25000, 50000, 100000, 250000, 500000, 1000000).foreach(scale => {
			spark.sql(s"""
				SELECT
					grid_id
					, COUNT(1) cnt_$scale
				FROM df_mn
				WHERE zi026_ctuu = $scale
				GROUP BY grid_id
			""").createOrReplaceTempView(s"df_$scale")
		})

		spark.sql("""
			SELECT
				grid_id
				, zi026_ctuu dom_scale
				, my_count dom_count
			FROM (
				SELECT
					grid_id
					, zi026_ctuu
					, COUNT(zi026_ctuu) my_count
					, row_number() OVER (PARTITION BY grid_id ORDER BY COUNT(1) DESC) my_rank
				FROM df_mn
				GROUP BY grid_id, zi026_ctuu
			) t
			WHERE my_rank = 1
		""").createOrReplaceTempView("df_ds")

		List("min", "max").foreach(limit => {
			spark.sql(s"""
				SELECT
					l.grid_id
					, COUNT(1) ${limit}_scale_count
				FROM df_mn l
				JOIN (
					SELECT
						grid_id
						, ${limit}(zi026_ctuu) ${limit}_scale
					FROM df_mn
					GROUP BY grid_id
				) r ON l.grid_id = r.grid_id AND l.zi026_ctuu = r.${limit}_scale
				GROUP BY l.grid_id
			""").createOrReplaceTempView(s"df_s${limit.reverse(0)}")
		})

		List("hadr", "psg").foreach(lcType => {
			// check the feature class name in the type lookups
			var clausesLookup = clausesLookupSets(lcType)
			if (clausesLookup.contains(fcName)) {
				var attrList = clausesLookup(fcName)(0)
				var postgresSql = clausesLookup(fcName)(1)
				var sparkSql = clausesLookup(fcName)(2)
				
				// create the initial dataframe that can be partitioned by objectid
				var dfRead = spark.read.format("jdbc").options(jdbcOptionsInput).option("lowerBound", oidMin).option("upperBound", oidMax).option("numPartitions", numPartitions).option("partitionColumn", "objectid").option("dbtable", s"""
				(
					SELECT
						objectid
						, CONCAT(FLOOR((st_x(st_centroid(shape)) - -180.0) / 0.25), ':', FLOOR((st_y(st_centroid(shape)) - -90.0) / 0.25)) grid_id
						, f_code
						, ${attrList}
					FROM tds.${fcName}
					WHERE st_x(st_centroid(shape)) IS NOT NULL
						AND (${postgresSql})
				) tbl
				""").load()
				dfRead.createOrReplaceTempView(s"df_${lcType}_read")

				var dfAttrs = spark.sql(s"""
					SELECT
						grid_id
						, array_remove(array(${sparkSql}), '') my_defs
					FROM df_${lcType}_read
				""")

				var dfMainLC = dfAttrs.repartition(numPartitions, $"grid_id")
				dfMainLC.createOrReplaceTempView(s"df_${lcType}_main")
				dfMainLC.cache()

				spark.sql(s"""
					SELECT
						grid_id
						, COUNT(1) feature_cnt
						, MAX(size(my_defs)) max_def_cnt
						, MIN(size(my_defs)) min_def_cnt
						, AVG(size(my_defs)) mean_def_cnt
						, percentile_approx(size(my_defs), 0.5) median_def_cnt
					FROM df_${lcType}_main
					GROUP BY grid_id
				""").createOrReplaceTempView(s"df_${lcType}_stats")

				spark.sql(s"""
					SELECT
						grid_id
						, pri_num_def
						, pri_num_def_cnt
					FROM (
						SELECT
							grid_id
							, size(my_defs) pri_num_def
							, COUNT(1) pri_num_def_cnt
							, row_number() OVER (PARTITION BY grid_id ORDER BY COUNT(1) DESC) my_rank
						FROM df_${lcType}_main
						GROUP BY grid_id, size(my_defs)
					) t
					WHERE my_rank = 1
				""").createOrReplaceTempView(s"df_${lcType}_pri_num")

				spark.sql(s"""
					SELECT
						grid_id
						, sec_num_def
						, sec_num_def_cnt
					FROM (
						SELECT
							grid_id
							, size(my_defs) sec_num_def
							, COUNT(1) sec_num_def_cnt
							, row_number() OVER (PARTITION BY grid_id ORDER BY COUNT(1) DESC) my_rank
						FROM df_${lcType}_main
						GROUP BY grid_id, size(my_defs)
					) t
					WHERE my_rank = 2
				""").createOrReplaceTempView(s"df_${lcType}_sec_num")

				spark.sql(s"""
					SELECT
						grid_id
						, explode(my_defs) my_def
					FROM df_${lcType}_main
				""").createOrReplaceTempView(s"df_${lcType}_exploded")

				spark.sql(s"""
					SELECT
						grid_id
						, pri_attr_def
						, pri_attr_def_cnt
					FROM (
						SELECT
							grid_id
							, my_def pri_attr_def
							, COUNT(1) pri_attr_def_cnt
							, row_number() OVER (PARTITION BY grid_id ORDER BY COUNT(1) DESC) my_rank
						FROM df_${lcType}_exploded
						GROUP BY grid_id, my_def
					) t
					WHERE my_rank = 1
				""").createOrReplaceTempView(s"df_${lcType}_pri_attr")

				spark.sql(s"""
					SELECT
						grid_id
						, sec_attr_def
						, sec_attr_def_cnt
					FROM (
						SELECT
							grid_id
							, my_def sec_attr_def
							, COUNT(1) sec_attr_def_cnt
							, row_number() OVER (PARTITION BY grid_id ORDER BY COUNT(1) DESC) my_rank
						FROM df_${lcType}_exploded
						GROUP BY grid_id, my_def
					) t
					WHERE my_rank = 2
				""").createOrReplaceTempView(s"df_${lcType}_sec_attr")
			} else {
				// just create a set of empty table to join to so the end sql will not break
				var schemaStats = StructType(Array(StructField("grid_id", StringType, true), StructField("feature_cnt", IntegerType, true), StructField("max_def_cnt", IntegerType, true), StructField("min_def_cnt", IntegerType, true), StructField("mean_def_cnt", IntegerType, true), StructField("median_def_cnt", IntegerType, true)))
				spark.createDataFrame(Seq(Row("-1", 0, 0, 0, 0, 0)), schemaStats).registerTempTable(s"df_${lcType}_stats")
				
				var schemaPriAttr = StructType(Array(StructField("grid_id", StringType, true), StructField("pri_attr_def", IntegerType, true), StructField("pri_attr_def_cnt", IntegerType, true)))
				spark.createDataFrame(Seq(Row("-1", 0, 0)), schemaPriAttr).registerTempTable(s"df_${lcType}_pri_attr")
				
				var schemaSecAttr = StructType(Array(StructField("grid_id", StringType, true), StructField("sec_attr_def", IntegerType, true), StructField("sec_attr_def_cnt", IntegerType, true)))
				spark.createDataFrame(Seq(Row("-1", 0, 0)), schemaSecAttr).registerTempTable(s"df_${lcType}_sec_attr")
				
				var schemaPriNum = StructType(Array(StructField("grid_id", StringType, true), StructField("pri_num_def", IntegerType, true), StructField("pri_num_def_cnt", IntegerType, true)))
				spark.createDataFrame(Seq(Row("-1", 0, 0)), schemaPriNum).registerTempTable(s"df_${lcType}_pri_num")
				
				var schemaSecNum = StructType(Array(StructField("grid_id", StringType, true), StructField("sec_num_def", IntegerType, true), StructField("sec_num_def_cnt", IntegerType, true)))
				spark.createDataFrame(Seq(Row("-1", 0, 0)), schemaSecNum).registerTempTable(s"df_${lcType}_sec_num")
			}
		})

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
					WHEN (l.pa_median >= 0 AND l.pa_median < 15) THEN 5
					WHEN (l.pa_median >= 15 AND l.pa_median <= 25) THEN 4
					WHEN (l.pa_median > 25 AND l.pa_median <= 50) THEN 3
					WHEN (l.pa_median > 50 AND l.pa_median <= 100) THEN 2
					WHEN (l.pa_median > 100) THEN 1
					ELSE 0
				END AS pa_score
				, CASE
					WHEN (l.pa_median >= 0 AND l.pa_median < 15) THEN 'Platinum'
					WHEN (l.pa_median >= 15 AND l.pa_median <= 25) THEN 'Gold'
					WHEN (l.pa_median > 25 AND l.pa_median <= 50) THEN 'Silver'
					WHEN (l.pa_median > 50 AND l.pa_median <= 100) THEN 'Bronze'
					WHEN (l.pa_median > 100) THEN 'Tin'
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
				, ifnull(c2500.cnt_2500, 0) cnt_2500
				, ifnull(c2500.cnt_2500, 0) / l.feature_cnt * 100.0 per_2500
				, ifnull(c5000.cnt_5000, 0) cnt_5000
				, ifnull(c5000.cnt_5000, 0) / l.feature_cnt * 100.0 per_5000
				, ifnull(c12500.cnt_12500, 0) cnt_12500
				, ifnull(c12500.cnt_12500, 0) / l.feature_cnt * 100.0 per_12500
				, ifnull(c25000.cnt_25000, 0) cnt_25000
				, ifnull(c25000.cnt_25000, 0) / l.feature_cnt * 100.0 per_25000
				, ifnull(c50000.cnt_50000, 0) cnt_50000
				, ifnull(c50000.cnt_50000, 0) / l.feature_cnt * 100.0 per_50000
				, ifnull(c100000.cnt_100000, 0) cnt_100000
				, ifnull(c100000.cnt_100000, 0) / l.feature_cnt * 100.0 per_100000
				, ifnull(c250000.cnt_250000, 0) cnt_250000
				, ifnull(c250000.cnt_250000, 0) / l.feature_cnt * 100.0 per_250000
				, ifnull(c500000.cnt_500000, 0) cnt_500000
				, ifnull(c500000.cnt_500000, 0) / l.feature_cnt * 100.0 per_500000
				, ifnull(c1000000.cnt_1000000, 0) cnt_1000000
				, ifnull(c1000000.cnt_1000000, 0) / l.feature_cnt * 100.0 per_1000000
				, ds.dom_scale
				, ds.dom_count
				, l.min_scale
				, sn.min_scale_count
				, l.max_scale
				, sx.max_scale_count
				, CASE
					WHEN (ds.dom_scale >= 500000) THEN 'STRATEGIC'
					WHEN (ds.dom_scale >= 250000) THEN 'OPERATIONAL'
					WHEN (ds.dom_scale >= 25000) THEN 'TACTICAL'
					WHEN (ds.dom_scale >= 5000) THEN 'URBAN'
					ELSE 'UNDEFINED'
				END AS mission_planning
				, pg.grls_score
				, CASE
					WHEN ds.dom_scale =  5000   AND pg.grls_score = 'G'   THEN 5
					WHEN ds.dom_scale =  5000   AND pg.grls_score = 'R'   THEN 5
					WHEN ds.dom_scale =  5000   AND pg.grls_score = 'L'   THEN 5
					WHEN ds.dom_scale =  5000   AND pg.grls_score = 'S/U' THEN 5
					WHEN ds.dom_scale =  10000  AND pg.grls_score = 'G'   THEN 5
					WHEN ds.dom_scale =  10000  AND pg.grls_score = 'R'   THEN 5
					WHEN ds.dom_scale =  10000  AND pg.grls_score = 'L'   THEN 5
					WHEN ds.dom_scale =  10000  AND pg.grls_score = 'S/U' THEN 5
					WHEN ds.dom_scale =  12500  AND pg.grls_score = 'G'   THEN 5
					WHEN ds.dom_scale =  12500  AND pg.grls_score = 'R'   THEN 5
					WHEN ds.dom_scale =  12500  AND pg.grls_score = 'L'   THEN 5
					WHEN ds.dom_scale =  12500  AND pg.grls_score = 'S/U' THEN 5
					WHEN ds.dom_scale =  25000  AND pg.grls_score = 'G'   THEN 5
					WHEN ds.dom_scale =  25000  AND pg.grls_score = 'R'   THEN 5
					WHEN ds.dom_scale =  25000  AND pg.grls_score = 'L'   THEN 5
					WHEN ds.dom_scale =  25000  AND pg.grls_score = 'S/U' THEN 5
					WHEN ds.dom_scale =  50000  AND pg.grls_score = 'G'   THEN 4
					WHEN ds.dom_scale =  50000  AND pg.grls_score = 'R'   THEN 4
					WHEN ds.dom_scale =  50000  AND pg.grls_score = 'L'   THEN 4
					WHEN ds.dom_scale =  50000  AND pg.grls_score = 'S/U' THEN 2
					WHEN ds.dom_scale =  100000 AND pg.grls_score = 'G'   THEN 3
					WHEN ds.dom_scale =  100000 AND pg.grls_score = 'R'   THEN 3
					WHEN ds.dom_scale =  100000 AND pg.grls_score = 'L'   THEN 2
					WHEN ds.dom_scale =  100000 AND pg.grls_score = 'S/U' THEN 1
					WHEN ds.dom_scale =  250000 AND pg.grls_score = 'G'   THEN 3
					WHEN ds.dom_scale =  250000 AND pg.grls_score = 'R'   THEN 3
					WHEN ds.dom_scale =  250000 AND pg.grls_score = 'L'   THEN 2
					WHEN ds.dom_scale =  250000 AND pg.grls_score = 'S/U' THEN 1
					WHEN ds.dom_scale >= 500000 AND pg.grls_score = 'G'   THEN 3
					WHEN ds.dom_scale >= 500000 AND pg.grls_score = 'R'   THEN 2
					WHEN ds.dom_scale >= 500000 AND pg.grls_score = 'L'   THEN 1
					WHEN ds.dom_scale >= 500000 AND pg.grls_score = 'S/U' THEN 1
					ELSE 0
				END AS them_acc_score
				, ifnull(dhs.feature_cnt, 0)                                hadr_feature_cnt
				, dhs.max_def_cnt                                           hadr_max_def_cnt
				, dhs.min_def_cnt                                           hadr_min_def_cnt
				, dhs.mean_def_cnt                                          hadr_mean_def_cnt
				, dhs.median_def_cnt                                        hadr_median_def_cnt
				, dhpa.pri_attr_def                                         hadr_pri_attr_def
				, dhpa.pri_attr_def_cnt                                     hadr_pri_attr_def_cnt
				, round(dhpa.pri_attr_def_cnt / dhs.feature_cnt * 100.0, 1) hadr_per_pri
				, dhpn.pri_num_def                                          hadr_pri_num_def
				, dhpn.pri_num_def_cnt                                      hadr_pri_num_def_cnt
				, dhsa.sec_attr_def                                         hadr_sec_attr_def
				, dhsa.sec_attr_def_cnt                                     hadr_sec_attr_def_cnt
				, round(dhsa.sec_attr_def_cnt / dhs.feature_cnt * 100.0, 1) hadr_per_sec
				, dhsn.sec_num_def                                          hadr_sec_num_def
				, dhsn.sec_num_def_cnt                                      hadr_sec_num_def_cnt
				, CASE
					WHEN dhs.median_def_cnt = 0 THEN 5
					WHEN dhs.median_def_cnt = 1 THEN 4
					WHEN dhs.median_def_cnt IN (2,3) THEN 3
					WHEN dhs.median_def_cnt IN (4,5) THEN 2
					WHEN dhs.median_def_cnt > 5 THEN 1
					ELSE 0
				END hadr_lc_score
				, ifnull(dps.feature_cnt, 0)                                psg_feature_cnt
				, dps.max_def_cnt                                           psg_max_def_cnt
				, dps.min_def_cnt                                           psg_min_def_cnt
				, dps.mean_def_cnt                                          psg_mean_def_cnt
				, dps.median_def_cnt                                        psg_median_def_cnt
				, dppa.pri_attr_def                                         psg_pri_attr_def
				, dppa.pri_attr_def_cnt                                     psg_pri_attr_def_cnt
				, round(dppa.pri_attr_def_cnt / dps.feature_cnt * 100.0, 1) psg_per_pri
				, dppn.pri_num_def                                          psg_pri_num_def
				, dppn.pri_num_def_cnt                                      psg_pri_num_def_cnt
				, dpsa.sec_attr_def                                         psg_sec_attr_def
				, dpsa.sec_attr_def_cnt                                     psg_sec_attr_def_cnt
				, round(dpsa.sec_attr_def_cnt / dps.feature_cnt * 100.0, 1) psg_per_sec
				, dpsn.sec_num_def                                          psg_sec_num_def
				, dpsn.sec_num_def_cnt                                      psg_sec_num_def_cnt
				, CASE
					WHEN dps.median_def_cnt = 0 THEN 5
					WHEN dps.median_def_cnt = 1 THEN 4
					WHEN dps.median_def_cnt IN (2,3) THEN 3
					WHEN dps.median_def_cnt IN (4,5) THEN 2
					WHEN dps.median_def_cnt > 5 THEN 1
					ELSE 0
				END psg_lc_score
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
			LEFT JOIN df_2500          c2500    ON l.grid_id=c2500.grid_id
			LEFT JOIN df_5000          c5000    ON l.grid_id=c5000.grid_id
			LEFT JOIN df_12500         c12500   ON l.grid_id=c12500.grid_id
			LEFT JOIN df_25000         c25000   ON l.grid_id=c25000.grid_id
			LEFT JOIN df_50000         c50000   ON l.grid_id=c50000.grid_id
			LEFT JOIN df_100000        c100000  ON l.grid_id=c100000.grid_id
			LEFT JOIN df_250000        c250000  ON l.grid_id=c250000.grid_id
			LEFT JOIN df_500000        c500000  ON l.grid_id=c500000.grid_id
			LEFT JOIN df_1000000       c1000000 ON l.grid_id=c1000000.grid_id
			LEFT JOIN df_ds            ds       ON l.grid_id=ds.grid_id
			LEFT JOIN df_sn            sn       ON l.grid_id=sn.grid_id
			LEFT JOIN df_sx            sx       ON l.grid_id=sx.grid_id
			LEFT JOIN df_pg            pg       ON l.grid_id=pg.grid_id
			LEFT JOIN df_hadr_stats    dhs      ON l.grid_id=dhs.grid_id
			LEFT JOIN df_hadr_pri_attr dhpa     ON l.grid_id=dhpa.grid_id
			LEFT JOIN df_hadr_sec_attr dhsa     ON l.grid_id=dhsa.grid_id
			LEFT JOIN df_hadr_pri_num  dhpn     ON l.grid_id=dhpn.grid_id
			LEFT JOIN df_hadr_sec_num  dhsn     ON l.grid_id=dhsn.grid_id
			LEFT JOIN df_psg_stats     dps      ON l.grid_id=dps.grid_id
			LEFT JOIN df_psg_pri_attr  dppa     ON l.grid_id=dppa.grid_id
			LEFT JOIN df_psg_sec_attr  dpsa     ON l.grid_id=dpsa.grid_id
			LEFT JOIN df_psg_pri_num   dppn     ON l.grid_id=dppn.grid_id
			LEFT JOIN df_psg_sec_num   dpsn     ON l.grid_id=dpsn.grid_id
			LEFT JOIN df_cd            cd       ON l.grid_id=cd.grid_id
			LEFT JOIN df_ta            ta       ON l.grid_id=ta.grid_id
		""").write.format("jdbc").option("driver", "org.postgresql.Driver").option("url", "jdbc:postgresql://@@@emr_processing.destinationServer@@@:@@@emr_processing.destinationPort@@@/@@@emr_processing.destinationDatabaseTDS@@@").option("user", "@@@emr_processing.destinationUsernameTDS@@@").option("password", "@@@emr_processing.destinationPasswordTDS@@@").option("truncate", true).option("stringtype", "unspecified").option("dbtable", s"sotd.sotd_${fcName}").mode(SaveMode.Overwrite).save()
		println(s"---------------------------------------------------------\ncompleted ${fcName}")
	} catch {
		case e: Exception => println(s"---------------------------------------------------------\nexception on ${fcName}, ${e}")

		// Attempt to seed the table that doesn't exist in topo with a geometry value (this will cause issues publishing our services/aprxs otherwise)
		try {
			// Read in table that was just created via pre-sql - SHOULD BE EMPTY
			var outputDf = spark.read.format("jdbc").options(jdbcOptionsOutput).option("query", s"SELECT * FROM sotd.sotd_${fcName}").load()
		
			// If it is empty, create a df, insert the placeholder, and then join it to a new DF to write to the db
			if (outputDf.count() == 0) {
		
				var schemaStats = StructType(Array(StructField("objectid", IntegerType, true), StructField("shape", StringType, true)))
				var seedDf = spark.createDataFrame(Seq(Row(1, s"SRID=4326;POLYGON ((-0.01 -0.01, -0.01 0.01, 0.01 0.01, 0.01 -0.01, -0.01 -0.01))")), schemaStats)//.registerTempTable(s"df_${fcName}_postsql")
			
				// Join the seeded geom record to the empty schema table created from outputDf
				var resultDf = outputDf.join(seedDf, Seq("objectid","shape"), "fullouter")
				resultDf.write.format("jdbc").option("driver", "org.postgresql.Driver").option("url", "jdbc:postgresql://@@@emr_processing.destinationServer@@@:@@@emr_processing.destinationPort@@@/@@@emr_processing.destinationDatabaseTDS@@@").option("user", "@@@emr_processing.destinationUsernameTDS@@@").option("password", "@@@emr_processing.destinationPasswordTDS@@@").option("truncate", true).option("stringtype", "unspecified").option("dbtable", s"sotd.sotd_${fcName}").mode(SaveMode.Overwrite).save()
			}
		} catch {
			case e: Exception => println(s"Could not add seeded geom record to table ${fcName}, ${e}")
		}
	}

})
System.exit(0)