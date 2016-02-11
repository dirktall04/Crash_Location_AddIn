#!/usr/bin/env python
# -*- coding: utf-8 -*-
# countyiterator7statusreport.py
# function definition names are given in lowercase to be compatible
# with the multiprocessing package's fork function for spawning
# additional processes.

# The goals for this script are to:

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
##
# It would make more sense to try to do them
# separately so that each function could be individually
# run for the entire group.

#9 Give a report on what the accident location progress looks like.

# countyiterator1accsplit.py
# countyiterator2roadchecks.py
# countyiterator3roadnamefixes.py
# countyiterator4addresslocators.py
# countyiterator5accintersect.py
# countyiterator6pointoffset.py
# countyiterator7statusreports.py


# The kind of output that we should receive:

# What percentage of each county's accident data has been geocoded to
# an intersection.
# Total number counties that have some amount of geolocated accidents:
# Total percentage of accident data that has been geocoded
# to an intersection, for counties with NG911_Final roads that don't
# cause issues with the script.
# Total percentage of all county's accident data that has been geocoded
# to an intersection.
# Total percentage of accident data that has been offset from an
# intersection, for counties with NG911_Final roads that don't
# cause issues with the script.
# Total percentage of all county's accident data that has been offset
# from an intersection.

## Write the information to a table and give the last time that
## it was updated, so that it is possible to just view it instead
## of having to run this script to view it. Of course, to get
## updated information, the script will have to be run. Try to
## get it task scheduled.


### Here are the options that the script receives when it is called from
### a button:

# To start, get find the intersection points data for each county.
# Then, figure out what the total number of intersection points that
# are geocoded to an intersection are, out of the total number of
# accident rows that exist for that county.
# Store the name of the county, the number of geocoded points for it,
# and the total possible geocoded points for it.
# Then, after all of the county information is reviewed, add the
# total number of geocoded points and the total possible geocoded
# points for just the counties that have some amount of geocoded
# points.
# Then, calculate what percentage of the possible points for
# those counties the geocoded points are and store it.
# Next, find the total amount of possible geocoded points for
# all counties, including the ones that don't have NG911_Final
# roads that work well with the script, or don't have NG911_Final
# roads at all.
# Then calculate what percentage of the possible points for
# all counties have been geocoded.


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
# "Auto" do not attempt to offset it.

# Fields Needed:
# County - 105 counties + TotalWithRoads + TotalOverall
# IntersectPoints
# OffsetPoints
# IntersectPercent
# OffsetPercent

# Only need to list the counties in the crashLocation sql instance.

import os

from arcpy import (AddField_management, env, CreateTable_management, Delete_management,
                    GetCount_management, ListFeatureClasses, MakeFeatureLayer_management,
                    MakeTableView_management, SelectLayerByAttribute_management)
from arcpy.da import (InsertCursor, SearchCursor)  # @UnresolvedImport

# Use a searchCursor to get all of the County abbreviations
# and County numbers from the Shared.Counties layer in SDEPROD.
coAbbrAndNoList = list()

cursorFields = ["COUNTY_ABBR", "COUNTY_NO", "COUNTY_NAME"]

coSCursor = SearchCursor(r'Database Connections\GIS@sdeprod.sde\SHARED.COUNTIES', cursorFields)

for cursorItem in coSCursor:
    coAbbrAndNoList.append(cursorItem)

extractDataGDBPath = r'Database Connections\geo@crashLocation.sde'

reportResultsList = list()

insertCursorFields = ["Name", "TotalAccidents", "GcPercent", "GcNKPercent", "OfsPercent", "OfsNKPercent"]


def iteratorprocess():
    
    env.workspace = extractDataGDBPath
    accDataFeaturesList = ListFeatureClasses("CrashLocation.GEO.ACC*")
    # Use the FullTable for the overall total.
    accDataFullTable = os.path.join(extractDataGDBPath, r'CrashLocation.GEO.GIS_GEOCODE_ACC')
    
    withRoadsTotal = 0
    
    gcKTotal = 0
    gcNKTotal = 0
    ofsKTotal = 0
    ofsNKTotal = 0
    NG911CoAccidents = 0
    
    inMemoryTempLayer = 'inMemoryTempFC'
    
    for countyItem in coAbbrAndNoList:
        countyNumber = countyItem[1]
        countyName = countyItem[2]
        
        accDataPointsKDOT = "CrashLocation.GEO.ACC_PTS_" + countyNumber
        accDataPointsNK = "CrashLocation.GEO.ACC_PTS_" + countyNumber + "_NK"
        accDataOffsetKDOT = "CrashLocation.GEO.ACC_OFS_PTS_" + countyNumber
        accDataOffsetNK = "CrashLocation.GEO.ACC_OFS_PTS_" + countyNumber + "_NK"
        
        # Re-zero the loop variables here so that the table doesn't get incorrect information in it.
        
        totalAccidents = 0
        
        geocodedAccidents = 0
        geocodedAccidentsNK = 0
        offsetAccidents = 0
        offsetAccidentsNK = 0
        
        gcPercent = '0.00'
        gcNKPercent = '0.00'
        ofsPercent = '0.00'
        ofsNKPercent = '0.00'
        
        if (accDataPointsKDOT in accDataFeaturesList) or (accDataPointsNK in accDataFeaturesList) or \
            (accDataOffsetKDOT in accDataFeaturesList) or (accDataOffsetNK in accDataFeaturesList):
            
            if accDataPointsKDOT in accDataFeaturesList:
                
                try:
                    Delete_management(inMemoryTempLayer)
                except:
                    pass
                
                accDataPointsKDOTPath = os.path.join(extractDataGDBPath, accDataPointsKDOT)
                
                MakeFeatureLayer_management(accDataPointsKDOTPath, inMemoryTempLayer)
                
                #SelectLayerByAttribute_management(inMemoryTempLayer, 'CLEAR_SELECTION')
                
                tempResult = GetCount_management(inMemoryTempLayer)
                
                totalAccidents = int(tempResult.getOutput(0))
                
                if totalAccidents > 0:
                    withRoadsTotal += totalAccidents
                else:
                    pass
                
                selectWhereClause = """ Status <> 'U' """
                
                SelectLayerByAttribute_management(inMemoryTempLayer, 'NEW_SELECTION', selectWhereClause)
                
                tempResult = GetCount_management(inMemoryTempLayer)
                
                geocodedAccidents = int(tempResult.getOutput(0))
                
            else:
                pass
            
            if accDataPointsNK in accDataFeaturesList:
                
                try:
                    Delete_management(inMemoryTempLayer)
                except:
                    pass
                
                accDataPointsNKPath = os.path.join(extractDataGDBPath, accDataPointsNK)
                
                MakeFeatureLayer_management(accDataPointsNKPath, inMemoryTempLayer)
                
                selectWhereClause = """ Status <> 'U' """
                
                SelectLayerByAttribute_management(inMemoryTempLayer, 'NEW_SELECTION', selectWhereClause)
                
                tempResult = GetCount_management(inMemoryTempLayer)
                
                geocodedAccidentsNK = int(tempResult.getOutput(0))
                
            else:
                pass
            
            if accDataOffsetKDOT in accDataFeaturesList:
                
                try:
                    Delete_management(inMemoryTempLayer)
                except:
                    pass
                
                accDataOffsetKDOTPath = os.path.join(extractDataGDBPath, accDataOffsetKDOT)
                
                MakeFeatureLayer_management(accDataOffsetKDOTPath, inMemoryTempLayer)
                
                selectWhereClause = """ isOffset IS NOT NULL """
                
                SelectLayerByAttribute_management(inMemoryTempLayer, 'NEW_SELECTION', selectWhereClause)
                
                tempResult = GetCount_management(inMemoryTempLayer)
                
                offsetAccidents = int(tempResult.getOutput(0))
                
            else:
                pass
            
            if accDataOffsetNK in accDataFeaturesList:
                
                try:
                    Delete_management(inMemoryTempLayer)
                except:
                    pass
                
                accDataOffsetNKPath = os.path.join(extractDataGDBPath, accDataOffsetNK)
                
                MakeFeatureLayer_management(accDataOffsetNKPath, inMemoryTempLayer)
                
                selectWhereClause = """ isOffset IS NOT NULL """
                
                SelectLayerByAttribute_management(inMemoryTempLayer, 'NEW_SELECTION', selectWhereClause)
                
                tempResult = GetCount_management(inMemoryTempLayer)
                
                offsetAccidentsNK = int(tempResult.getOutput(0))
                
            else:
                pass
            
            try:
                gcPercent = "{0:.2f}".format((float(geocodedAccidents) / totalAccidents) * 100)
                gcNKPercent = "{0:.2f}".format((float(geocodedAccidentsNK) / totalAccidents) * 100)
                ofsPercent = "{0:.2f}".format((float(offsetAccidents) / totalAccidents) * 100)
                ofsNKPercent = "{0:.2f}".format((float(offsetAccidentsNK) / totalAccidents) * 100)
            except ZeroDivisionError:
                gcPercent = None
                gcNKPercent = None
                ofsPercent = None
                ofsNKPercent = None
            except:
                pass
            
            gcKTotal += geocodedAccidents
            gcNKTotal += geocodedAccidentsNK
            ofsKTotal += offsetAccidents
            ofsNKTotal += offsetAccidentsNK
            NG911CoAccidents += totalAccidents
            
            print("\n" + countyName + " County has " + str(totalAccidents) + " totalAccidents.")
            print("gcPercent: " + gcPercent + " gcNKPercent: " + gcNKPercent +
                  " ofsPercent: " + ofsPercent + " ofsNKPercent: " + ofsNKPercent)
            
        # To get the withRoadsTotal, sum the number for each county that
        # returned a non-zero result for totalAccidents.
        
        else:
            pass
        
        reportResult = [countyName, totalAccidents, gcPercent, gcNKPercent, ofsPercent, ofsNKPercent]
        reportResultsList.append(reportResult)
    
    try:
        Delete_management(inMemoryTempLayer)
    except:
        pass
    
    MakeTableView_management(accDataFullTable, inMemoryTempLayer)
    
    tempResult = GetCount_management(inMemoryTempLayer)
    
    overallTotal = int(tempResult.getOutput(0))
    
    for reportResultItem in reportResultsList:
        print str(reportResultItem[0])
    
    gcNG911Percent = "{0:.2f}".format((float(gcKTotal) / NG911CoAccidents) * 100)
    gcNKNG911Percent = "{0:.2f}".format((float(gcNKTotal) / NG911CoAccidents) * 100)
    ofsNG911Percent = "{0:.2f}".format((float(ofsKTotal) / NG911CoAccidents) * 100)
    ofsNKNG911Percent = "{0:.2f}".format((float(ofsNKTotal) / NG911CoAccidents) * 100)
    
    print "\n" + "The NG911Total is: " + str(NG911CoAccidents)
    print( " with gcPercent: " + gcNG911Percent + " gcNKPercent: " + gcNKNG911Percent + 
           " ofsPercent: " + ofsNG911Percent + " ofsNKPercent: " + ofsNKNG911Percent)
    
    reportResult = ["NG911Total", NG911CoAccidents, gcNG911Percent, gcNKNG911Percent, ofsNG911Percent, ofsNKNG911Percent]
    reportResultsList.append(reportResult)
    
    gcOverallPercent = "{0:.2f}".format((float(gcKTotal) / overallTotal) * 100)
    gcNKOverallPercent = "{0:.2f}".format((float(gcNKTotal) / overallTotal) * 100)
    ofsOverallPercent = "{0:.2f}".format((float(ofsKTotal) / overallTotal) * 100)
    ofsNKOverallPercent = "{0:.2f}".format((float(ofsNKTotal) / overallTotal) * 100)
    
    print "\n" + "The OverallTotal is: " + str(overallTotal)
    print (" with gcPercent: " + gcOverallPercent + " gcNKPercent: " + gcNKOverallPercent +
           " ofsPercent: " + ofsOverallPercent + " ofsNKPercent: " + ofsNKOverallPercent)
    
    reportResult = ["OverallTotal", overallTotal, gcOverallPercent, gcNKOverallPercent, ofsOverallPercent, ofsNKOverallPercent]
    reportResultsList.append(reportResult)
    
    resultsTablePath = recreateResultsTable()
    
    # Delete the previous table information, if any, then create an insert cursor
    # and place all of the report result items in the table.
    
    newICursor = InsertCursor(resultsTablePath, insertCursorFields)
    
    for reportResultItem in reportResultsList:
        insertedRowID = newICursor.insertRow(reportResultItem)
        print "Inserted a new row into the REPORT_INFO table with OID: " + str(insertedRowID)
    
    
def recreateResultsTable():
    print "\n" + "Recreating the REPORT_INFO table."
    outTableGDB = r'Database Connections\geo@crashLocation.sde'
    outTableName = r'CrashLocation.GEO.REPORT_INFO'
    env.workspace = outTableGDB
    
    outTableFullPath = os.path.join(outTableGDB, outTableName)
    
    try:
        Delete_management(outTableFullPath)
    except:
        pass
    
    CreateTable_management(outTableGDB, outTableName)
    
    AddField_management(outTableName, "Name", "TEXT", "", "", "25")
    AddField_management(outTableName, "TotalAccidents", "TEXT", "", "",  "12")
    # Changed these from 5 to 6 because 100.00 is 6 characters.
    AddField_management(outTableName, "GcPercent", "TEXT", "", "",  "6") 
    AddField_management(outTableName, "GcNKPercent", "TEXT", "", "",  "6")
    AddField_management(outTableName, "OfsPercent", "TEXT", "", "",  "6")
    AddField_management(outTableName, "OfsNKPercent", "TEXT", "", "",  "6")
    
    return outTableFullPath
    
    
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


# TODO: Build a reporting script that gives me the information on the 
# number of points that were successfully geocoded per county and
# also as an aggregate, for both KDOT and non-KDOT fields.

# Should also gives information on how many of them that have
# been successfully offset from those geocoded points.

if __name__ == "__main__":
    iteratorprocess()
else:
    pass