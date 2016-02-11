#!/usr/bin/env python
# -*- coding: utf-8 -*-
# kcarsprdToCrashLocation.py
'''
@author: dtalley
Created: 2015-06-09
'''


# This script is to create and/or update the GIS_GEOCODE_ACC_V table in
# the crashLocation MSSQL database and populate it with data from the
# kcarsprd Oracle database.

# For now, just create the table and insert the records.
# Add comparison/appending later.


from arcpy import (CreateTable_management, #AddField_management, AddIndex_management,
                   Exists, env, ListFields, MakeTableView_management)
from arcpy.da import InsertCursor, SearchCursor  # @UnresolvedImport
import os

inTableGDB = r"Database Connections\readonly@kcarsprd.sde"
outTableGDB = r"Database Connections\geo@crashLocation.sde"

inTableName = "KCARS.GIS_GEOCODE_ACC_V"
outTableName = "GEO.GIS_GEOCODE_ACC"


def createAndUpdateAccTable(sourceTableGDB, sourceTableName, destinationTableGDB, destinationTableName):
    
    sourceTableFullPath = os.path.join(sourceTableGDB, sourceTableName)
    destinationTableFullPath = os.path.join(destinationTableGDB, destinationTableName)
    print "Starting the AccTable transfer."
    
    if Exists(destinationTableFullPath):
        pass
    else:
        env.workspace = sourceTableGDB
        MakeTableView_management(sourceTableFullPath, "sourceView")
        # Uses the sourceView table view as a template for the table creation to carry over the field information.
        CreateTable_management(destinationTableGDB, destinationTableName, "sourceView")
        pass
    
    env.workspace = destinationTableGDB
    
    fieldObjectList = ListFields(sourceTableFullPath)
    
    fieldList = [field.name for field in fieldObjectList if field.name != "OBJECTID"]
    
    tableDataList = list()
    
    # Use a searchCursor to read the data in from the sourceTable.
    sCursor = SearchCursor(sourceTableFullPath, fieldList)
    
    for cursorItem in sCursor:
        tableDataList.append(cursorItem)
        
    try:
        del sCursor
    except:
        pass
    
    # Use an insertCursor to insert the data into the destinationTable.
    iCursor = InsertCursor(destinationTableFullPath, fieldList)
    
    for tableDataItem in tableDataList:
        returnedOID = iCursor.insertRow(tableDataItem)
        print "Inserted a row with an objectID of: " + str(returnedOID)
        
    try:
        del iCursor
    except:
        pass

if __name__ == "__main__":
    createAndUpdateAccTable(inTableGDB, inTableName, outTableGDB, outTableName)

else:
    pass