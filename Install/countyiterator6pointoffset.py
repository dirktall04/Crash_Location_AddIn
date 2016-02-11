#!/usr/bin/env python
# -*- coding: utf-8 -*-
# countyiterator6pointoffset.py

# The goals for this script are to:

#1 Attempt to create a RoadChecks gdb for each county with a County_Final gdb. -- Try using Kyle's,
#   but in the except block, try to delete the roadchecks gdb if there's an error.

#2 Separate out the accidents for each county with a Final gdb. ##### - Complete.

#3 Fix the Road Name errors in the separated accidents for that county. # Put the unique road name tables in SQL.

#4 Attempt to create an address locator for each county with a RoadChecks gdb. ## Place these in a new folder.

#5 Geolocate the accidents to intersections for each county with 
#   a RoadChecks gdb & an address locator.

#6 Offset the accidents for each county with geolocated accidents.

#7 Accomplish the above with loops, as it is fairly repetitive.

#8 Break these functions into separate scripts, because
# trying to do them all with one script execution is cool,
# but it's probably not the most effective way to
# get things done.
##
# It would make more sense to try to do them
# separately so that each function could be individually
# run for the entire group.

# countyiterator1accsplit.py
# countyiterator2roadchecks.py
# countyiterator3roadnamefixes.py
# countyiterator4addresslocators.py
# countyiterator5accintersect.py
# countyiterator6pointoffset.py
# countyiterator7statusreports.py


### Here are the options that the script receives when it is called from
### a button:


'''
    if (option0 is not None and option0 != ""): # Geocoded to Intersection (accidentDataAtIntersections)
        optionsObject.accidentDataAtIntersections = option0
    else:
        pass
    if (option1 is not None and option1 != ""): # Where the roads features are (roadsFeaturesLocation)
        optionsObject.roadsFeaturesLocation = option1
    else:
        pass
    if (option2 is not None and option2 != ""): # Where the alias table for the roads features is (aliasTable)
        optionsObject.aliasTable = option2
    else:
        pass
    if (option3 is not None and option3 != ""): # Output location after offset (accidentDataWithOffsetOutput)
        optionsObject.accidentDataWithOffsetOutput = option3
    else:
        pass
    if (option4 is not None and option4 != ""): # Boolean choice of whether or not to use KDOT fields
        optionsObject.useKDOTFields = option4
    else:
        pass
    
'''


# The accidentDataAtIntersections information can be found by
# iterating through the feature classes in the crashLocation
# sql database instance and finding the ones which match
# the pattern of: 
# for KDOT fields Feature Classes and 
# for non-KDOT fields Feature Classes.
# Use whichever one of these it matches to decide whether
# or not the useKDOTFields option is true or false.

# CrashLocation.GEO.ACC_PTS_<CountyNo>
# CrashLocation.GEO.ACC_PTS_<CountyNo>_NK

# The roadsFeaturesLocation and
# aliasTable paths can be found by matching the
# county number and the county abbreviation
# for the points feature class and the
# roadchecks gdb.

# If both exist, then call the
# AccidentDirectionMatrixOffsetCode.py caller function with
# the preceding parameters and name the output
# in the following manner:

# But check first to see if the output
# already exists.
# If it does, write a way to see if there are rows
# that are missing from the output data that
# exist in the source data and then
# add only those rows.

# Then, make updates to only the rows that
# don't have their offset field already set
# to true. -- No need to redo calculations
# that have already been done.
# Also, this allows for manual offsetting to
# not be overwritten when the person who
# made the manual offset sets the field
# to show that it has already been offset.

# Maybe allow the field to be set to something
# like Manual instead of True or
# Auto instead of True to show that it was
# done automatically and should potentially
# be reviewed?

# Then, if the field is either "Manual" or
# "Auto", or "Reviewed" do not attempt to offset
# it again.

# More importantly, "Manual" and "Reviewed" rows
# need to be saved out somewhere so that they
# can be reimported back in if you ever decide
# to get rid of the feature class entirely
# and then rebuild it.


import os

from arcpy import (env, ListFeatureClasses)
from arcpy.da import (SearchCursor)  # @UnresolvedImport

from AccidentDirectionMatrixOffsetCode import offsetdirectioncaller


# Use a searchCursor to get all of the County abbreviations
# and County numbers from the Shared.Counties layer in SDEPROD.
coAbbrAndNoList = list()

cursorFields = ["COUNTY_ABBR", "COUNTY_NO"]

coSCursor = SearchCursor(r'Database Connections\GIS@sdeprod.sde\SHARED.COUNTIES', cursorFields)

for cursorItem in coSCursor:
    coAbbrAndNoList.append(cursorItem)

extractDataGDBPath = r'Database Connections\geo@crashLocation.sde'

def iteratorprocess():
    
    roadChecksPath = r'\\gisdata\ArcGIS\GISdata\Accident Geocode\Python\RoadChecks'
    roadChecksFileList = os.listdir(roadChecksPath)
    
    env.workspace = extractDataGDBPath
    accDataPointsList = ListFeatureClasses("CrashLocation.GEO.ACC_PTS_*")
    useKDOTIntersect = True
    
    for gdbName in roadChecksFileList:
        if len(gdbName) > 23 and gdbName[0] != "x" and gdbName[-15] == "_" and gdbName[-15:].lower() == "_roadchecks.gdb":
            countyAbbr = gdbName[-23] + gdbName[-22]
            
            roadChecksGDBPath = os.path.join(roadChecksPath, gdbName)
            roadsFeatures = os.path.join(roadChecksGDBPath, r'NG911\RoadCenterline')
            roadsAlias = os.path.join(roadChecksGDBPath, r'RoadAlias')
            
            # If the translation of the roadChecks gdb's county abbr to countyNo
            # exists as geocoded point features in the crashLocation sql
            # instance with either KDOT fields or non-KDOT fields, then
            # call the offsetdirectioncaller function with the appropriate
            # parameters for that roadChecks gdb and  geocoded point
            # features.
            
            #try:
            for countyItem in coAbbrAndNoList:
                if countyAbbr.lower() == countyItem[0].lower():
                    countyNumber = countyItem[1]
                    
                    accDataPointsKDOT = "CrashLocation.GEO.ACC_PTS_" + countyNumber
                    
                    if (accDataPointsKDOT) in accDataPointsList:
                        
                        useKDOTIntersect = True
                        offsetFeaturesNameKDOT = "CrashLocation.GEO.ACC_OFS_PTS_" + countyNumber                 
                        print ("Calling offsetdirectioncaller function for " + roadChecksGDBPath + "\n" +
                               ", " + accDataPointsKDOT + ", and " + offsetFeaturesNameKDOT + ".")
                        
                        accDataPathKDOT = os.path.join(extractDataGDBPath, accDataPointsKDOT)
                        offsetFeaturesPathKDOT = os.path.join(extractDataGDBPath, offsetFeaturesNameKDOT)
                        
                        offsetdirectioncaller(accDataPathKDOT, roadsAlias, roadsFeatures, offsetFeaturesPathKDOT, useKDOTIntersect)
                    else:
                        pass
                    
                    accDataPointsNK = "CrashLocation.GEO.ACC_PTS_" + countyNumber + "_NK"
                    
                    if (accDataPointsNK) in accDataPointsList:
                        
                        useKDOTIntersect = False
                        offsetFeaturesNameNK = "CrashLocation.GEO.ACC_OFS_PTS_" + countyNumber + "_NK"
                        
                        print ("Calling offsetdirectioncaller function for " + roadChecksGDBPath + "\n" +
                               ", " + accDataPointsNK + ", and " + offsetFeaturesNameNK + ".")
                        
                        accDataPathNK = os.path.join(extractDataGDBPath, accDataPointsNK)
                        offsetFeaturesPathNK = os.path.join(extractDataGDBPath, offsetFeaturesNameNK)
                        
                        offsetdirectioncaller(accDataPathNK, roadsAlias, roadsFeatures, offsetFeaturesPathNK, useKDOTIntersect)
                    else:
                        pass
                else:
                    pass
            else:
                pass
            #except:
                #print "An error occurred."
                #print ""
            
        else:
            pass
    
    
def getGDBLocationFromFC(fullFeatureClassPath):
    test1 = os.path.split(fullFeatureClassPath)
    
    if test1[0][-4:] == ".sde":
        gdbPath = test1[0]
        print "The SDE GDB path is " + str(gdbPath)
    elif test1[0][-4:] == ".gdb":
        gdbPath = os.path.dirname(fullFeatureClassPath)
    else:
        gdbPath = os.path.dirname(os.path.dirname(fullFeatureClassPath))
    
    return gdbPath


# This needs to call AccidentDirectionMatrixOffsetCode.py and give it
# the necessary parameters for each feature class of intersected points
# that is in the crashLocation sql instance.

# The names for these feature classes are to be in the style of: 
# CrashLocation.GEO.ACC_PTS_<CountyNo>_OFS
# for the KDOT field offset points and 
# CrashLocation.GEO.ACC_PTS_<CountyNo>_NK_OFS
# for the non-KDOT field offset points.

# TODO: Build a reporting script that gives me the information on the 
# number of points that were successfully geocoded per county and
# also as an aggregate, for both KDOT and non-KDOT fields.

# Should also gives information on how many of them that have
# been successfully offset from those geocoded points.

if __name__ == "__main__":
    iteratorprocess()
else:
    pass