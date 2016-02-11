#!/usr/bin/env python
# -*- coding: utf-8 -*-
# countyiterator.py
# function definition names are given in lowercase to be compatible
# with the multiprocessing package's fork function for spawning
# additional processes.

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
##
# It would make more sense to try to do them
# separately so that each function could be individually
# run for the entire group.

# countyiterator1accsplit.py
# countyiterator2roadchecks.py # multiprocess this
# countyiterator3roadnamefixes.py
# countyiterator4addresslocators.py
# countyiterator5accintersect.py
# countyiterator6intersectoffset.py

import os

from arcpy import (CreateTable_management, Copy_management, Describe, Delete_management, Exists, MakeTableView_management)
from arcpy.da import (InsertCursor, SearchCursor)  # @UnresolvedImport

from NGfLRSMethod import CalledUpon


# Use a searchCursor to get all of the County abbreviations
# and County numbers from the Shared.Counties layer in SDEPROD.
coAbbrAndNoList = list()

cursorFields = ["COUNTY_ABBR", "COUNTY_NO"]

coSCursor = SearchCursor(r'Database Connections\GIS@sdeprod.sde\SHARED.COUNTIES', cursorFields)

for cursorItem in coSCursor:
    coAbbrAndNoList.append(cursorItem)

extractDataInPathFull = r'Database Connections\geo@crashLocation.sde\GEO.GIS_GEOCODE_ACC'

accidentTableDesc = Describe(extractDataInPathFull)
# Make a list of all of the fields except for OBJECTID.
accidentTableFieldNameList = [field.name for field in accidentTableDesc.fields if field.name != "OBJECTID"]

accKeyCounter = 0

for accidentTableFieldName in accidentTableFieldNameList:
    if accidentTableFieldName.upper() == "ACCIDENT_KEY":
        accKeyPosition = accKeyCounter
    else:
        accKeyCounter += 1

print "The accident_key field is in position " + str(accKeyPosition) + "."

tableViewName = 'AccTable01'

def iteratorprocess():
    for fieldName in accidentTableFieldNameList:
        print fieldName
    
    finalPath = r'\\gisdata\arcgis\GISdata\DASC\NG911\Final'
    
    # Get a list of the *_Final.gdb folders here.
    gdbFileList = os.listdir(finalPath)    
    
    for gdbName in gdbFileList:
        
        if len(gdbName) > 10 and gdbName[0] != "x" and gdbName[-13] == "_" and gdbName[-10:].lower() == "_final.gdb":
            countyAbbr = gdbName[-12] + gdbName[-11]
            # Try Using fileName[-12] and fileName[-11] as a county abbreviation.
            # Lookup the county abbreviation in the COUNTY_ABBR
            # column of the Shared.Counties layer in SDEPROD.
            # If there's a match, add the county abbreviation
            # to the finalizedCounties list.
            # Use the COUNTY_NO to try selecting
            # accidents from the accidents table and
            # exporting them to a new table.
            
            print gdbName
            
            try:
                for countyItem in coAbbrAndNoList:
                    if countyAbbr.lower() == countyItem[0].lower():
                        print countyAbbr + " : " + countyItem[1]
                        # Call a different function to split out the accident data for this county's number.
                        ### Re-enable later.
                        ###accidentdataextractor(countyItem[1])
                        
            except:
                pass
            
            # Call a different function to create the roadchecks gdb for this county's abbreviation.
            try:
                if countyAbbr.lower() != "cl" and countyAbbr.lower() != "ro":
                    finalGDBPath = os.path.join(finalPath, gdbName)
                    print "Using the CalledUpon function from the NGfLRS script for gdb: " + gdbName + "."
                    CalledUpon(finalGDBPath)
                else:
                    pass
            except:
                # Delete the roadchecks gdb since there was an error.
                # Maybe also write the error to a log file so that
                # we can take some action to try to correct it.
                pass
            
        else:
            pass
        
    # Get a list of the *_RoadChecks.gdb folders here.
    gdbFileList = os.listdir(finalPath)
    roadChecksPath = r'\\gisdata\ArcGIS\GISdata\Accident Geocode\Python\RoadChecks'
    
    for gdbName in gdbFileList:
        if len(gdbName) > 14 and gdbName[0] != "x" and gdbName[-15] == "_" and gdbName[-15:].lower() == "_roadchecks.gdb":
            
            try:
                gdbToCopy = os.path.join(finalPath, gdbName)
                gdbOutputLocation = os.path.join(roadChecksPath, gdbName)
                try:
                    Delete_management(gdbOutputLocation)
                except:
                    pass
                # Copy the gdb to a new location
                Copy_management (gdbToCopy, gdbOutputLocation)
                # Delete the roadChecks gdb from the "Final" folder
                if(Exists(gdbOutputLocation) and Exists(gdbToCopy)):
                    # The gdb was copied to the new location
                    try:
                        Delete_management(gdbToCopy)
                    except:
                        print 'Could not delete roadChecks gdb from the "Final" folder.'
                else:
                    pass
                
            except:
                pass
            pass

### After the RoadChecks gdbs are created, they need to be moved out of this folder
### and placed elsewhere.

### Then, the road name repair and build address locator scripts can run
### while targeting the post-move locations.


# Make a function to either create the table and insert all records for that
# county into it, or to update the table with only the accident data
# that it doesn't already have.
def accidentdataextractor(countyNo):
    extractDataOutGDB = r'Database Connections\geo@crashLocation.sde'
    extractDataOutName = r'GEO.ACC_' + countyNo
    extractDataOutPathFull = os.path.join(extractDataOutGDB, extractDataOutName)
    
    tableSelectSQL = """ "COUNTY_NBR" = '""" + countyNo + """' """
    MakeTableView_management(extractDataInPathFull, tableViewName, tableSelectSQL)
    accidentDataFull = list()
    accSCursor = SearchCursor(tableViewName, accidentTableFieldNameList)
    
    
    for accItem in accSCursor:
        #print "Adding the data with acc_key of: "  + str(accItem[accKeyPosition]) + " to the accidentDataFull list."
        accidentDataFull.append(accItem)
    
    try:
        del accSCursor
    except:
        pass
    
    try:
        Delete_management(tableViewName)
    except:
        pass
    
    
    if Exists(extractDataOutPathFull):
        print extractDataOutPathFull + " already exists."
        existingKeysList = list()
        
        outSCursor = SearchCursor(extractDataOutPathFull, "ACCIDENT_KEY")
        for outSItem in outSCursor:
            #print "This key already exists in the output table: "  + str(outSItem[0])
            existingKeysList.append(outSItem[0])
        
        accDataKeysFull = [accDataRow[accKeyPosition] for accDataRow in accidentDataFull]
        
        # Use a set because finding membership in a set is O(1)
        # whereas finding membership in a list is O(n).
        # That's a problem when trying to find membership in
        # Johnson county, as it currently takes 65,000 * 65,000 * t
        # instead of just 65,000 * t.
        # This causes the script to lag for a long time on
        # county no 046.
        # When using a set instead, it takes about 3 seconds.
        existingKeysSet = set(existingKeysList)
        
        accDataKeysToInsert = [accDataKey for accDataKey in accDataKeysFull if accDataKey not in existingKeysSet]
        
        extractDataOutCursor = InsertCursor(extractDataOutPathFull, accidentTableFieldNameList)
        for accDataItem in accidentDataFull:
            if str(accDataItem[accKeyPosition]) in accDataKeysToInsert:
                insertedItem = extractDataOutCursor.insertRow(accDataItem)
                print "Inserted a row with OBJECTID " + str(insertedItem) + "."
            
            else:
                pass
        
        #searchcursor here to get all the existing accident keys from the current output table.
        # comparison to see which rows from the tableView don't already
        # exist in the current output table
        # insertcursor here to place only missing accident key rows
        # from the searchCursor into the extractDataOutPathFull.
        
    else:
        CreateTable_management(extractDataOutGDB, extractDataOutName, extractDataInPathFull)
        extractDataOutCursor = InsertCursor(extractDataOutPathFull, accidentTableFieldNameList)
        for accDataItem in accidentDataFull:
            extractDataOutCursor.insertRow(accDataItem)
            #insertedItem = extractDataOutCursor.insertRow(accDataItem)
            #print "Inserted a row with OBJECTID " + str(insertedItem) + "."
        
        #Insert all of the rows without a check, since the table is new.


# Make the function that geolocates these points use the form of
# Acc_Pts_<No> for the output feature class name.


# Make sure that the location for the address locator is outside of
# a gdb. That's the only way to use multithreading with them
# and some of the counties definitely need to be multithreaded,
# such as Johnson county. Hopefully cleaning up the intersect road name data
# will help fix the slowness, but it still has 60k+ accidents just by itself.


if __name__ == "__main__":
    print "Please call the numbered countyiterator scripts (e.g. countyiterator1accsplits.py, countyiterator2roadchecks.py) instead of this one."
else:
    pass