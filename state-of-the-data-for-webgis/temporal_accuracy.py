"""-----------------------------------------------------------------------------
Name: temporal_accuracy.py
Purpose: Scores every 15" cell according to the temporal accuracy of the
        data at that cell.  The temporal accuracy score is determined
        from the source currency and the change detection data.
Description: This tool uses the change detection features and the temporal
        currency to calculate the temporal accuracy. The temporal accuracy is a
        statistical layer that ranks cells in terms of how much change has
        occurred within the cell since the source data.
Requirements: Python 2.7.x/Python3.x, ArcGIS 10.4+/Pro 1.2+
Author(s): Gregory Brunner, Contractor for National Geospatial-Intelligence
        Agency (NGA) | Andrew Chapkowski, Contractor NGA
Program Manager: Derek Silva, NGA (Derek.A.Silva@nga.mil)
Created: July 24, 2014
Modified: August, 2016 | April, 2017
Copyright: Esri
License: TBD
-----------------------------------------------------------------------------"""

from __future__ import division
from __future__ import print_function
#Native imports
import os
import sys
#3rd party imports
import numpy as np
import arcpy
from arcpy import env
from arcpy import da

arcpy.env.overwriteOutput = True
###########################################################################
#--------------------------------------------------------------------------
class FunctionError(Exception):
    """ raised when a function fails to run """
    pass
#--------------------------------------------------------------------------
def trace():
    """
        trace finds the line, the filename
        and error message and returns it
        to the user
    """
    import traceback
    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]
    # script name + line number
    line = tbinfo.split(", ")[1]
    # Get Python syntax error
    #
    synerror = traceback.format_exc().splitlines()[-1]
    return line, __file__, synerror
#--------------------------------------------------------------------------
class TemporalAccuracy(object):
    """performs temporal accuracy calculations on a given feature class"""
    CURRENCY_FIELD = 'DOM_YEAR'
    CD_SCORE = 'CD_SINCE_DOM_YEAR'
    SCORE_FIELD = 'TEMP_ACC_SCORE'
    MINUS_2_SIGMA = 2.2
    MINUS_1_SIGMA = 15.8
    PLUS_1_SIGMA = 84.2
    PLUS_2_SIGMA = 97.8
    minus2Sigma = None
    minus1Sigma = None
    median = None
    plus2Sigma = None
    plus1Sigma = None
    #----------------------------------------------------------------------
    def __init__(self, change_detection_features, currency_features,
                 temporal_accuracy_features, years):

        yl = []
        fields = arcpy.ListFields(change_detection_features)
        for f in fields:
            if f.name[0] == 'y' and (f.name[1:3] == '19' or f.name[1:3]=='20'):
                year = int(f.name[1:])
                if year in years:
                    yl.append(f.name)
                #arcpy.AddMessage(f.name[1:3])

        self.year_list = yl
        self.years = [int(self.str_to_int(year)) for year in self.year_list]
        self.num_years = len(self.year_list)
        self.change_detection_features = change_detection_features
        self.currency_features = currency_features
        self.temporal_accuracy_features = temporal_accuracy_features
        self.change_detection_as_points = os.path.join('in_memory', 'cd_as_points')
    #----------------------------------------------------------------------
    #def __del__(self):
    #    """overwrite the delete operation"""
    #    arcpy.Delete_management(self.change_detection_as_points)
    #----------------------------------------------------------------------
    def str_to_int(self, val):
        """converts a list of strings to a list of integers"""
        try:
            return ''.join(v for v in val if v.isdigit())
        except:
            line, filename, synerror = trace()
            raise FunctionError({
                "function": "str_to_int",
                "line": line,
                "filename": __file__,
                "synerror": synerror,
                "arc" : str(arcpy.GetMessages(2))
            })
    #----------------------------------------------------------------------
    def append_fields_to_year_list(self):
        """appends a constant value to a list"""
        try:
            #Append the new field name to the field array
            arcpy.AddMessage("Appending new fields to the field list")
            self.year_list.append(self.CURRENCY_FIELD)
            self.year_list.append(self.SCORE_FIELD)
            self.year_list.append(self.CD_SCORE)
        except:
            line, filename, synerror = trace()
            raise FunctionError({
                "function": "append_fields_to_year_list",
                "line": line,
                "filename": __file__,
                "synerror": synerror,
                "arc" : str(arcpy.GetMessages(2))
            })
    #----------------------------------------------------------------------
    def get_change_detection_statistics(self, fc):
        """calculates the change detection statistic"""
        try:
            #FeatureToNumPyArray
            sum_array = da.FeatureClassToNumPyArray(fc,
                                                    self.year_list,
                                                    skip_nulls=True)

            arcpy.AddMessage("Creating CD SUM array")
            change_detection_sum = []
            for i in range(0, len(sum_array[self.year_list[0]])):
                results = 0
                for year in self.year_list:
                    results = sum_array[i][year]+ results
                change_detection_sum.append(results)

            arcpy.AddMessage('Using default SUM statistics')
            self.minus2Sigma = np.percentile(change_detection_sum,
                                             self.MINUS_2_SIGMA)
            self.minus1Sigma = np.percentile(change_detection_sum,
                                             self.MINUS_1_SIGMA)
            self.median = np.median(change_detection_sum)
            self.plus1Sigma = np.percentile(change_detection_sum,
                                            self.PLUS_1_SIGMA)
            self.plus2Sigma = np.percentile(change_detection_sum,
                                            self.PLUS_2_SIGMA)

            arcpy.AddMessage('Minus 2 Sigma = ' + str(self.minus2Sigma))
            arcpy.AddMessage('Minus 1 Sigma = ' + str(self.minus1Sigma))
            arcpy.AddMessage('Median = ' + str(self.median))
            arcpy.AddMessage('Plus 1 Sigma = ' + str(self.plus1Sigma))
            arcpy.AddMessage('Plus 2 Sigma = ' + str(self.plus2Sigma))
        except:
            line, filename, synerror = trace()
            raise FunctionError({
                "function": "get_change_detection_statistics",
                "line": line,
                "filename": __file__,
                "synerror": synerror,
                "arc" : str(arcpy.GetMessages(2))
            })
    #----------------------------------------------------------------------
    #getScore accesor function
    def get_score(self, array):
        """calculates the scores"""
        try:
            raw_score = np.sum(array)
            #arcpy.AddMessage rawScore
            if raw_score <= self.minus2Sigma:
                return 5, raw_score
            elif (raw_score > self.minus2Sigma and raw_score <= self.minus1Sigma):
                return 4, raw_score
            elif (raw_score > self.minus1Sigma and raw_score <= self.plus1Sigma):
                return 3, raw_score
            elif (raw_score > self.plus1Sigma and raw_score <= self.plus2Sigma):
                return 2, raw_score
            elif raw_score > self.plus2Sigma:
                return 1, raw_score
        except:
            line, filename, synerror = trace()
            raise FunctionError({
                "function": "get_score",
                "line": line,
                "filename": __file__,
                "synerror": synerror,
                "arc" : str(arcpy.GetMessages(2))
            })
    #----------------------------------------------------------------------
    def add_fields(self):
        #Create the new field
        try:
            arcpy.AddMessage('Creating ' + self.CD_SCORE + " and " + self.SCORE_FIELD)
            arcpy.AddField_management(self.temporal_accuracy_features,
                                      self.CD_SCORE, "DOUBLE", "", "", "",
                                      self.CD_SCORE, "NULLABLE")
            arcpy.AddField_management(self.temporal_accuracy_features,
                                      self.SCORE_FIELD, "SHORT", 10, "", "",
                                      self.SCORE_FIELD, "NULLABLE")
        except:
            line, filename, synerror = trace()
            raise FunctionError({
                "function": "add_fields",
                "line": line,
                "filename": __file__,
                "synerror": synerror,
                "arc" : str(arcpy.GetMessages(2))
            })
    #----------------------------------------------------------------------
    #Algorithm used to create the temporal accuracy score
    def get_temporal_accuracy(self):
        """assigns the temporal accuracy"""
        try:
            #score the features
            arcpy.AddMessage('Scoring the features')
            with da.UpdateCursor(self.temporal_accuracy_features,
                                 self.year_list) as cursor:
                for row in cursor:
                    if row[self.num_years] is None or row[self.num_years] == 0:
                        row[self.num_years+1] = None
                        row[self.num_years+2] = None
                    elif None in row[0:self.num_years-2]:
                        row[self.num_years+1] = None
                        row[self.num_years+2] = None
                    else:
                        if row[self.num_years] >= self.years[self.num_years-1]:
                            row[self.num_years+1] = 6
                            row[self.num_years+2] = 0
                        #orinially 2012, should be indexed at 12?
                        else:
                            try:
                                for x in xrange(0,self.num_years):
                                    if (row[self.num_years] >= self.years[x] and row[self.num_years] < self.years[x+1]):
                                        row[self.num_years+1], row[self.num_years+2] = self.get_score(row[x:self.num_years])
                            except:
                                for x in range(0,self.num_years-2):
                                    if (row[self.num_years] >= self.years[x] and row[self.num_years] < self.years[x+1]):
                                        row[self.num_years+1], row[self.num_years+2] = self.get_score(row[x:self.num_years])

                    cursor.updateRow(row)
        except:
            line, filename, synerror = trace()
            raise FunctionError({
                "function": "get_temporal_accuracy",
                "line": line,
                "filename": __file__,
                "synerror": synerror,
                "arc" : str(arcpy.GetMessages(2))
            })
    #----------------------------------------------------------------------
    def create_temporal_accuracy(self):
        """creates the temporal accuracy"""
        try:
            arcpy.AddMessage("Getting change detection statsitics")

            fc = self.change_detection_features
            self.get_change_detection_statistics(fc)

            arcpy.AddMessage('Running Feature To Point')
            arcpy.FeatureToPoint_management(self.change_detection_features,
                                            self.change_detection_as_points,
                                            "INSIDE")

            arcpy.AddMessage('Running Spatial Join')
            arcpy.SpatialJoin_analysis(self.currency_features,
                                       self.change_detection_as_points,
                                       self.temporal_accuracy_features, "JOIN_ONE_TO_ONE",
                                       "KEEP_ALL", "#", "INTERSECT")

            arcpy.AddMessage("Adding fields that will be populated")
            self.append_fields_to_year_list()

            arcpy.AddMessage("Adding fields to temporal accuracy")
            self.add_fields()

            #Calculate the temporal accuracy
            arcpy.AddMessage("Running temporal accuracy algorithm")
            self.get_temporal_accuracy()

        except:
            line, filename, synerror = trace()
            raise FunctionError({
                "function": "create_temporal_accuracy",
                "line": line,
                "filename": __file__,
                "synerror": synerror,
                "arc" : str(arcpy.GetMessages(2))
            })
