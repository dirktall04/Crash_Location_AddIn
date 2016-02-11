#!/usr/bin/env python
# -*- coding: utf-8 -*-
# countyiterator2roadchecks.py

#The goals for this script are to:

#1 Attempt to create a RoadChecks gdb for each county with a County_Final gdb. -- Try using Kyle's,
#   but in the except block, try to delete the roadchecks gdb if there's an error.

#2 Separate out the accidents for each county with a Final gdb. - Complete.

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
import time

from arcpy import (CreateTable_management, Copy_management, Describe, Delete_management, Exists, MakeTableView_management)
from arcpy.da import (InsertCursor, SearchCursor)  # @UnresolvedImport

from NGfLRSMethod import CalledUpon

nGfLRSMethodInPlace = None

# Use a searchCursor to get all of the County abbreviations
# and County numbers from the Shared.Counties layer in SDEPROD.
coAbbrList = list()

cursorFields = ["COUNTY_ABBR"]

coSCursor = SearchCursor(r'Database Connections\GIS@sdeprod.sde\SHARED.COUNTIES', cursorFields)

for cursorItem in coSCursor:
    coAbbrList.append(cursorItem[0])

# Need to make sure that the countyAbbr is within the list of county abbreviations
# that we're supposed to have. Make a list from the shared.counties layer.

#-----------------------------------------------------------------------
# Rewrite this script so that it can be flagged to make the copy happen
# before the NGfLRSMethod is applied to the gdb.

# Rewrite the NGfLRSMethod script so that it can be flagged to work without
# making a new copy IF the proper flag is applied, and the geodatabase
# name ends with "RoadChecks.gdb"
#-----------------------------------------------------------------------


def iteratorprocess(nGfLRSInPlace):
    
    finalPath = r'\\gisdata\arcgis\GISdata\DASC\NG911\Final'
    
    # Get a list of the *_Final.gdb folders here.
    gdbFileList = os.listdir(finalPath)
    
    roadChecksPath = r'\\gisdata\ArcGIS\GISdata\Accident Geocode\Python\RoadChecks'
    
    gdbFileListRc = list()
    
    # Uncomment to include only a few counties.
    #includeList = ["CM"]
    # Uncomment to include all the possible counties.
    includeList = coAbbrList
    # Uncomment to exclude only a few counties.
    excludeList = ["CL", "CX", "MS", "MX", "RO", "RX"]
    
    # Have the NGfLRSMethod create the *_RoadChecks.gdb files.
    if nGfLRSInPlace == False:
        print "Why is the nGfLRSInPlace value: " + str(nGfLRSInPlace)
        for gdbName1 in gdbFileList:
            
            if len(gdbName1) > 10 and gdbName1[0] != "x" and gdbName1[-13] == "_" and gdbName1[-10:].lower() == "_final.gdb":
                # Try Using fileName[-12] and fileName[-11] as a county abbreviation.
                countyAbbr = gdbName1[-12] + gdbName1[-11]
                
                print "The countyAbbr is: " + countyAbbr
                
                if countyAbbr in coAbbrList and countyAbbr.upper() in includeList and countyAbbr.upper() not in excludeList:
                    print "The County Abbreviation is: " + countyAbbr
                    
                    try:
                            finalGDBPath = os.path.join(finalPath, gdbName1)
                            print "Using the CalledUpon function from the NGfLRS script for gdb: " + gdbName1 + "."
                            print "The nGfLRSInPlace value is: " + str(nGfLRSInPlace)
                            CalledUpon(finalGDBPath, nGfLRSInPlace)
                    except:
                        # Delete the roadchecks gdb since there was an error.
                        # Maybe also write the error to a log file so that
                        # we can take some action to try to correct it.
                        rcFinalGDBPath = finalGDBPath[:-4] + "_RoadChecks.gdb"
                        print "An error occurred when attempting to create " + rcFinalGDBPath + "."
                        try:
                            Delete_management(rcFinalGDBPath)
                        except:
                            print "Could not delete the RoadChecks gdb which may contain data errors."
                else:
                    pass
            else:
                pass
            
        # Get a list of the *_RoadChecks.gdb folders here.
        gdbFileList = os.listdir(finalPath)
        
        for gdbName2 in gdbFileList:
            if len(gdbName2) > 15 and gdbName2[0] != "x" and gdbName2[-15] == "_" and gdbName2[-15:].lower() == "_roadchecks.gdb":
                gdbFileListRc.append(gdbName2)
            else:
                pass
        
        # First loop to try to remove issue with not deleting the last gdb copied.
        for gdbNameRc1 in gdbFileListRc:
            
            try:
                gdbToCopy = os.path.join(finalPath, gdbNameRc1)
                gdbOutputLocation = os.path.join(roadChecksPath, gdbNameRc1)
                
                try:
                    Delete_management(gdbOutputLocation)
                except:
                    pass
                # Copy the gdb to a new location
                print "Copying " + gdbToCopy + " to " +  gdbOutputLocation + "."
                Copy_management(gdbToCopy, gdbOutputLocation)
                # Delete the roadChecks gdb from the "Final" folder after it's copied
                # Wait just a moment for the locks to clean up.
                
                ## Break the loop here and restart it to make sure that the
                ## last gdb in the list is properly deleted after being copied.
                ## Not sure why only the last one doesn't get released, but
                ## it seems pretty consistent.
                
            except:
                pass
        
        # Second loop to try to remove issue with not deleting the last gdb copied.
        for gdbNameRc2 in gdbFileListRc:
            
            try:
                gdbToCopy = os.path.join(finalPath, gdbNameRc2)
                gdbOutputLocation = os.path.join(roadChecksPath, gdbNameRc2)
                
                # Pick up the loop here and try to delete the processed
                # gdbs.
                # If this still doesn't work, start creating the processed
                # gdbs in a different folder instead of moving them
                # there after the processing occurs. Then, just do the
                # processing in the new folder.
                if(Exists(gdbOutputLocation) and Exists(gdbToCopy)):
                    # The gdb was copied to the new location
                    try:
                        Delete_management(gdbToCopy)
                    except:
                        try:
                            time.sleep(210)
                            # After waiting for locks to clear, try again.
                            Delete_management(gdbToCopy)
                        except:
                            print "Could not delete the roadChecks gdb called " + gdbToCopy + " from the 'Final' folder."
                else:
                    pass
                
            except:
                pass
    
    # Create the *_RoadChecks.gdb files prior to calling the NGfLRSMethod.
    elif nGfLRSInPlace == True:
        ## Replace this area with a copy of the _Final.gdb in the Final folder
        ## to a copy of the _RoadChecks.gdb in the RoadChecks folder.
        
        for gdbName1 in gdbFileList:
            
            if len(gdbName1) > 10 and gdbName1[0] != "x" and gdbName1[-13] == "_" and gdbName1[-10:].lower() == "_final.gdb":
                # Try Using fileName[-12] and fileName[-11] as a county abbreviation.
                countyAbbr = gdbName1[-12] + gdbName1[-11]
                
                print "The countyAbbr is: " + countyAbbr
                
                if countyAbbr in coAbbrList and countyAbbr.upper() in includeList and countyAbbr.upper() not in excludeList:
                    
                    print "The County Abbreviation is: " + countyAbbr
                    
                    try:
                        
                        gdbToCopy = os.path.join(finalPath, gdbName1)
                        gdbNameRc = gdbName1[:-4] + "_RoadChecks.gdb"
                        gdbOutputLocation = os.path.join(roadChecksPath, gdbNameRc)
                        
                        try:
                            Delete_management(gdbOutputLocation)
                        except:
                            pass
                        # Copy the gdb to a new location
                        print "Copying " + gdbToCopy + " to " +  gdbOutputLocation + "."
                        Copy_management(gdbToCopy, gdbOutputLocation)
                        
                    except:
                        pass
                    
                else:
                    pass
            else:
                pass
            
        # Get a list of the *_RoadChecks.gdb folders here.
        gdbFileList = os.listdir(roadChecksPath)
        
        for gdbName2 in gdbFileList:
            if len(gdbName2) > 15 and gdbName2[0] != "x" and gdbName2[-15] == "_" and gdbName2[-15:].lower() == "_roadchecks.gdb":
                gdbFileListRc.append(gdbName2)
            else:
                pass
        
        for gdbNameRc1 in gdbFileListRc:
            countyAbbr = gdbNameRc1[-23] + gdbNameRc1[-22]
            rcGDBPath = os.path.join(roadChecksPath, gdbNameRc1)
            if countyAbbr in coAbbrList and countyAbbr.upper() in includeList and countyAbbr.upper() not in excludeList:
                try:
                    print "Using the CalledUpon function from the NGfLRS script for gdb: " + gdbNameRc1 + "."
                    print "The nGfLRSInPlace value is: " + str(nGfLRSInPlace)
                    CalledUpon(rcGDBPath, nGfLRSInPlace)
                    
                except:
                    print "There was an error in calling the NGfLRSMEthod CalledUpon function for " + str(rcGDBPath)
            else:
                print countyAbbr + " is not a valid county abbreviation, or was excluded from processing."
        
    else:
        print "You must specify either True or False for the value of nGfLRSInPlace prior to calling this function."
### After the RoadChecks gdbs are created, they need to be moved out of this folder and placed elsewhere.

### Then, the road name repair and build address locator scripts can run while targeting the post-move locations.


if __name__ == "__main__":
    nGfLRSMethodInPlace = True
    iteratorprocess(nGfLRSMethodInPlace)
else:
    pass