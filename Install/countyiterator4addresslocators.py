#!/usr/bin/env python
# -*- coding: utf-8 -*-
# countyiterator4addresslocators.py

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

from arcpy import (Describe, env, ListTables)
from arcpy.da import (SearchCursor)  # @UnresolvedImport

from CreateAccidentLocator import CreateAccidentLocatorCaller

# Use a searchCursor to get all of the County abbreviations
# and County numbers from the Shared.Counties layer in SDEPROD.
coAbbrAndNoList = list()

cursorFields = ["COUNTY_ABBR", "COUNTY_NO"]

coSCursor = SearchCursor(r'Database Connections\GIS@sdeprod.sde\SHARED.COUNTIES', cursorFields)

for cursorItem in coSCursor:
    coAbbrAndNoList.append(cursorItem)

extractDataInPathFull = r'Database Connections\geo@crashLocation.sde\GEO.GIS_GEOCODE_ACC'

extractDataOutGDB = r'Database Connections\geo@crashLocation.sde'

env.overwriteOutput = True

env.workspace = extractDataOutGDB

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

def iteratorprocess():
    # Get a list of the *_RoadChecks.gdb folders here.
    roadChecksPath = r'\\gisdata\ArcGIS\GISdata\Accident Geocode\Python\RoadChecks'
    newLocatorPath = r'\\gisdata\ArcGIS\GISdata\Accident Geocode\Python\AccidentLocators'
    roadChecksFileList = os.listdir(roadChecksPath)
    env.workspace = extractDataOutGDB
    accDataTablesList = ListTables("CrashLocation.GEO.ACC_*")
    useKDOTIntersect = True
    
    for gdbName in roadChecksFileList:
        if len(gdbName) > 23 and gdbName[0] != "x" and gdbName[-15] == "_" and gdbName[-15:].lower() == "_roadchecks.gdb":
            countyAbbr = gdbName[-23] + gdbName[-22]
            
            #try:
            for countyItem in coAbbrAndNoList:
                if countyAbbr.lower() == countyItem[0].lower():
                    #print countyAbbr + " : " + countyItem[1]
                    countyNumber = countyItem[1]
                    
                    accDataTableOutName = "CrashLocation.GEO.ACC_" + countyNumber
                    newLocatorName = "Crash_Locator_CO_" + countyNumber
                    #newLocatorName = "TEST_004"
                    if (accDataTableOutName) in accDataTablesList:
                        roadChecksGDBPath = os.path.join(roadChecksPath, gdbName)
                        print ("Calling CreateAccidentLocatorCaller function for " + roadChecksGDBPath + "\n" +
                               " to be called "+ newLocatorName + " in " + newLocatorPath  + ".")
                        useKDOTIntersect = True
                        CreateAccidentLocatorCaller(roadChecksGDBPath, newLocatorPath, newLocatorName, useKDOTIntersect)
                        
                        
                        newLocatorName = newLocatorName + "_NK"
                        useKDOTIntersect = False
                        CreateAccidentLocatorCaller(roadChecksGDBPath, newLocatorPath, newLocatorName, useKDOTIntersect)
                    else:
                        print "accDataTableOutName: " + accDataTableOutName + " not found in the tableNameList"
                else:
                    pass
            #except:
                #print "An error occurred."
                #print ""
            
        else:
            pass


# Make the function that geolocates these points use the form of
# Acc_Pts_<No> for the output feature class name.


# Make sure that the location for the address locator is outside of
# a gdb. That's the only way to use multithreading with them
# and some of the counties definitely need to be multithreaded,
# such as Johnson county. Hopefully cleaning up the intersect road name data
# will help fix the slowness, but it still has 60k+ accidents just by itself.


if __name__ == "__main__":
    iteratorprocess()
else:
    pass