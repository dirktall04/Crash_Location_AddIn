#!/usr/bin/env python
# -*- coding: utf-8 -*-
# countyiterator5accintersect.py

#The goals for this script are to:

#1 Attempt to create a RoadChecks gdb for each county with a County_Final gdb. -- Try using Kyle's,
#   but in the except block, try to delete the roadchecks gdb if there's an error.

#####2 Separate out the accidents for each county with a Final gdb. ##### - Complete.

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
#
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

# The out_feature_class (KDOT) will be:
# \AddressLocators\..._Pts_###
# The out_feature_class (Non-KDOT) will be:
# \AddressLocators\..._Pts_###_NK

# Add a test to see if the output points already exist.
# If so, then just rematch the addresses using
# arcpy.RematchAddresses_geocoding(geocoded_feature_class, where_clause)
# instead of rerunning the entire process.


import os

from arcpy import (CreateTable_management, Copy_management, Describe, Delete_management, env, Exists,
                    ListTables, MakeTableView_management)
from arcpy.da import (InsertCursor, SearchCursor)  # @UnresolvedImport

from UseAnAccidentLocator import useanaccidentlocatorcaller


# Use a searchCursor to get all of the County abbreviations
# and County numbers from the Shared.Counties layer in SDEPROD.
coAbbrAndNoList = list()

cursorFields = ["COUNTY_ABBR", "COUNTY_NO"]

coSCursor = SearchCursor(r'Database Connections\GIS@sdeprod.sde\SHARED.COUNTIES', cursorFields)

for cursorItem in coSCursor:
    coAbbrAndNoList.append(cursorItem)

extractDataPathFull = r'Database Connections\geo@crashLocation.sde\GEO.GIS_GEOCODE_ACC'
extractDataGDBPath = r'Database Connections\geo@crashLocation.sde'
pointsFeaturesGDB = extractDataGDBPath

tableViewName = 'AccTable01'

countyNoAccDataList = list()

for coItem in coAbbrAndNoList:
    countyNoAccDataList.append(coItem[1])


def iteratorprocess():
    
    locatorsPath = r'\\gisdata\ArcGIS\GISdata\Accident Geocode\Python\AccidentLocators'
    locatorsFileList = os.listdir(locatorsPath)
    
    env.workspace = extractDataGDBPath
    accDataTablesList = ListTables("CrashLocation.GEO.ACC_*")
    
    NumbersList = set(range(1000))
    
    # Consider replacing the main if/elif here with regex.
    for locatorFile in locatorsFileList:
        if (len(locatorFile) > 5 and locatorFile[0] != "x" and locatorFile[-7:].lower() == "_nk.loc" and
             int(locatorFile[-10:-7]) in NumbersList):
            
            useKDOTIntersect = False
            print "Found a county _NK locator: " + locatorFile
            countyNoToCheckFor = locatorFile[-10:-7]
            print "The county number to check for an accident data table is: " + countyNoToCheckFor
            accDataTableToUse = "CrashLocation.GEO.ACC_" + countyNoToCheckFor
            
            if accDataTableToUse in accDataTablesList:
                print "Calling the Acc Locator with " + countyNoToCheckFor + " for the acc data table."
                # Added CrashLocation.GEO.
                # Changed from just "ACC_PTS_" + countyNoToCheckFor + "_NK" 2015-07-14
                pointFeaturesEnd = "CrashLocation.GEO.ACC_PTS_" + countyNoToCheckFor + "_NK"
                locatedPointsOutput = os.path.join(pointsFeaturesGDB, pointFeaturesEnd)
                locatorFileNoExt = locatorFile[:-4]
                locatorFullPath = os.path.join(locatorsPath, locatorFileNoExt)
                useanaccidentlocatorcaller(extractDataGDBPath, accDataTableToUse, locatorFullPath, locatedPointsOutput, useKDOTIntersect)
            else:
                pass
        elif (len(locatorFile) > 5  and locatorFile[0] != "x" and locatorFile[-4:].lower() == ".loc" and
             int(locatorFile[-7:-4]) in NumbersList):
            
            useKDOTIntersect = True
            print "Found a county locator: " + locatorFile
            countyNoToCheckFor = locatorFile[-7:-4]
            print "The county number to check for an accident data table is: " + countyNoToCheckFor
            accDataTableToUse = "CrashLocation.GEO.ACC_" + countyNoToCheckFor
            
            if accDataTableToUse in accDataTablesList:
                print "Calling Acc Locator with " + countyNoToCheckFor + " for the acc data table."
                # Added CrashLocation.GEO.
                # Changed from just "ACC_PTS_"  + countyNoToCheckFor 2015-07-14
                pointFeaturesEnd = "CrashLocation.GEO.ACC_PTS_" + countyNoToCheckFor
                locatedPointsOutput = os.path.join(pointsFeaturesGDB, pointFeaturesEnd)
                locatorFileNoExt = locatorFile[:-4]
                locatorFullPath = os.path.join(locatorsPath, locatorFileNoExt)
                useanaccidentlocatorcaller(extractDataGDBPath, accDataTableToUse, locatorFullPath, locatedPointsOutput, useKDOTIntersect)
            else:
                pass
        else:
            #print "locatorFile did not pass the test: " + locatorFile
            pass


# Make the function that geolocates these points use the form of
# Acc_Pts_<No> for the output feature class name.


# For this one, I just need to make sure that the accident data table for
# that county exists, and also that the address locator for the county
# exists. Then, I can use the address locator and the accident data
# to create points via geocoding.


if __name__ == "__main__":
    iteratorprocess()
else:
    pass