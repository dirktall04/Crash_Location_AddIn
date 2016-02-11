#!/usr/bin/env python
# -*- coding: utf-8 -*-
# countyiterator1accsplit.py

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
# countyiterator2roadchecks.py
# countyiterator3roadnamefixes.py
# countyiterator4addresslocators.py
# countyiterator5accintersect.py
# countyiterator6pointoffset.py
# countyiterator7statusreports.py

import os

from arcpy import (CreateTable_management, Copy_management, Describe, Delete_management, Exists, MakeTableView_management)
from arcpy.da import (InsertCursor, SearchCursor)  # @UnresolvedImport

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
            
            print "The County Abbreviation is: " + countyAbbr
            
            # Call a different function to create the roadchecks gdb for this county's abbreviation.
            for countyItem in coAbbrAndNoList:
                if countyAbbr.upper() == countyItem[0]:
                    countyNum = countyItem[1]
                    try:
                        accidentdataextractor(countyNum)
                    except:
                        print ("An error occurred while calling the accidentdataextractor function " + 
                               "for county number " + str(countyNum) + ".")
                else:
                    pass
        else:
            pass


# Make a function to either create the table and insert all records for that
# county into it, or to update the table with only the accident data
# that it doesn't already have.
def accidentdataextractor(countyNo):
    extractDataOutGDB = r'Database Connections\geo@crashLocation.sde'
    extractDataOutName = r'GEO.ACC_' + countyNo
    extractDataOutPathFull = os.path.join(extractDataOutGDB, extractDataOutName)
    
    tableSelectSQL = """ "COUNTY_NBR" = '""" + countyNo + """'"""
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
        # Johnson county, as it currently takes 65,000 * 65,000 * ExecTime,
        # where ExecTime is execution time for a single check.
        # instead of just 65,000 * ExecTime.
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


if __name__ == "__main__":
    iteratorprocess()
    accLocatorPath = r'\\gisdata\ArcGIS\GISdata\Accident Geocode\Python\AccidentLocators'
else:
    pass