import arcpy
import os
import sys
import datetime
import logging

def copy_sotd_features(feat_list, stamp, log):
    for feat in feat_list:
        try:
            if arcpy.Exists(feat+'_'+stamp):
                pass
            else:
                arcpy.CopyFeatures_management(feat, feat+'_'+stamp)
                now = datetime.datetime.now()
                log.info(f"{feat} finished at: {now}")
        except:
            log.info(f"{feat} FAILED at: {now}")
    return feat_list

def main():
    arcpy.env.overwriteOutput = True

    start = datetime.datetime.now()
    print(f"starting at:{start}")
    # can replace with sys.argv[1]

    #Common
    workspace =         sys.argv[1]
    stamp =             sys.argv[2]
    backup_location =   sys.argv[3]

    arcpy.env.workspace = workspace

    log = f"backup_{stamp}.log"
    print(log)
    logging.basicConfig(filename=os.path.join(backup_location, log), level=logging.INFO)
    logging.info(f"start: {start}")

    logging.info(f"Copying Classification")
    feat_list = arcpy.ListFeatureClasses("*Classification")
    print(feat_list)
    copy_sotd_features(feat_list, stamp, logging)
    now = datetime.datetime.now()
    logging.info(f"Classification finished at: {now}")


    logging.info(f"Copying FeatureDensity")
    feat_list = arcpy.ListFeatureClasses("*FeatureDensity")
    print(feat_list)
    copy_sotd_features(feat_list, stamp, logging)
    now = datetime.datetime.now()
    logging.info(f"FeatureDensity finished at: {now}")

    logging.info(f"Copying HADR")
    feat_list = arcpy.ListFeatureClasses("*hadr")
    print(feat_list)
    copy_sotd_features(feat_list, stamp, logging)
    now = datetime.datetime.now()
    logging.info(f"HADR finished at: {now}")

    logging.info(f"Copying PSG")
    feat_list = arcpy.ListFeatureClasses("*psg")
    print(feat_list)
    copy_sotd_features(feat_list, stamp, logging)
    now = datetime.datetime.now()
    logging.info(f"PSG finished at: {now}")

    logging.info(f"Copying PositionalAccuracy")
    feat_list = arcpy.ListFeatureClasses("*PositionalAccuracy")
    print(feat_list)
    copy_sotd_features(feat_list, stamp, logging)
    now = datetime.datetime.now()
    logging.info(f"PositionalAccuracy finished at: {now}")

    logging.info(f"Copying SourceLineage")
    feat_list = arcpy.ListFeatureClasses("*SourceLineage")
    print(feat_list)
    copy_sotd_features(feat_list, stamp, logging)
    now = datetime.datetime.now()
    logging.info(f"SourceLineage finished at: {now}")

    logging.info(f"Copying TemporalAccuracy")
    feat_list = arcpy.ListFeatureClasses("*TemporalAccuracy")
    print(feat_list)
    copy_sotd_features(feat_list, stamp, logging)
    now = datetime.datetime.now()
    logging.info(f"TemporalAccuracy finished at: {now}")

    logging.info(f"Copying TemporalCurrency")
    feat_list = arcpy.ListFeatureClasses("*TemporalCurrency")
    print(feat_list)
    copy_sotd_features(feat_list, stamp, logging)
    now = datetime.datetime.now()
    logging.info(f"TemporalCurrency finished at: {now}")

if __name__ == '__main__':
    main()
