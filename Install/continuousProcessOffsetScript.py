#!/usr/bin/env/ python
#continousProcessOffsetScript.py
# Created by dirktall04 on 2017-07-26
# Updated by dirktall04 on 2017-09-27
# Large portions of this code were adapted from:
# AccidentDirectionMatrixOffsetCode.py

# Rebuild the accident offset script to use a fixed source of Roadway data (NG911).
# 2017-09-27 update removed several in_memory\* feature classes in an attempt
# to reduce memory consumption as the script seems to be running into
# problems due to a lack of available memory.


import gc
import os
import pyproj
import random
from arcpy import (AddField_management, CalculateField_management, ConvertTimeField_management,
    CopyFeatures_management, CopyRows_management, CreateFeatureclass_management, Delete_management,
    DeleteField_management, Describe, env, Exists, GetCount_management, ListFeatureClasses, ListFields,
    JoinField_management, MakeFeatureLayer_management, SelectLayerByAttribute_management,
    SelectLayerByLocation_management, Sort_management, SpatialReference, TruncateTable_management)
from arcpy.da import (SearchCursor as daSearchCursor, InsertCursor as daInsertCursor, UpdateCursor as daUpdateCursor,
    TableToNumPyArray, ExtendTable)
from arcpy.mapping import (AddLayer, MapDocument, ListDataFrames, ListLayers)
from AccidentDirectionMatrixOffsetCode import (continuousoffsetcaller, mainTest as accOffMainTest)
# Doesn't seem like the daily vs overall property will need to be implemented.
##from lastRun import dailyOrOverall

from pathFunctions import (returnGDBOrSDEPath,
    returnFeatureClass, returnGDBOrSDEName)


# I've phased out most of the feature classes and tables that were written to in_memory.
# The script seemed to be having trouble with all the space that they were taking up.
# Also added some calls to gc.collect() in this script and
# the AccidentDirectionMatrixOffsetCode script.
env.overwriteOutput = True
newSelection = "NEW_SELECTION"
maxCrashPointsToProcess = 10000

roadwayTableGCID = "NGKSSEGID"
aliasTableGCID = "NGKSSEGID"
aliasTableNameFields = ["A_RD", "LABEL"]
additionalNameColumnBaseName = "Alias_Road_Name_"
# Since I'm not super confident in the CHASM connection, there will 
# need to be a  location that I use for staging data that comes across.
# In fact I may need to pull data across in chunks so that it doesn't time out.
# Once all the data is across, it will be compared to the previous dataset.
# If the number of features is at least 75% of the previous number, the
# features will be used in the process. If not, they'll be ignored and
# the previous features will be used. There should be a max
# time limit on this, however it is not yet implemented.

gdbForStaging = r'D:\SCHED\OffsetCrashes\crashOffsetStaging.gdb'
offsetProjectionToUse = r'D:\SCHED\OffsetCrashes\NAD 1983 Decimal Degrees.prj'

chasmNG911Roadways = r'D:\CHASM\CHASM.sde\KDOT.ROADCENTERLINE_NG911'
pulledNG911Roadways = r'D:\SCHED\OffsetCrashes\crashOffsetStaging.gdb\pu_NG911Roadways'
stagedNG911Roadways = r'D:\SCHED\OffsetCrashes\crashOffsetStaging.gdb\sg_NG911Roadways'
##inMemNG911Roadways = r"in_memory\NG911Roadways"
##ng911RoadwaysLayer = 'ng911RoadwaysAsALayer' # Might not be needed.
##inMemRoadwaysSubset = r'in_memory\NG911RoadwaysSubset'

chasmAliasTable = r'D:\CHASM\CHASM.sde\KDOT.ROADALIAS_NG911'
pulledAliasTable = r'D:\SCHED\OffsetCrashes\crashOffsetStaging.gdb\pu_AliasTable'
stagedAliasTable = r'D:\SCHED\OffsetCrashes\crashOffsetStaging.gdb\sg_AliasTable'
# Will have to see if this needs special loading into the data frame.
##inMemAliasTable = r'in_memory\AliasTable'

# Since the logic for the pulled & staged tables doesn't apply to the Daily
# geocode, the only crashes that will be pulled & staged will be from the Overall.
# However, I will need to include the data from the original crash record
# since the number of columns in the output is greatly reduced from the
# original crash record source.
chasmOutputCrashPoints = r'D:\CHASM\CHASM.sde\KDOT.KGS_CRASH_OUTPUT'
pulledCrashPoints = r'D:\SCHED\OffsetCrashes\crashOffsetStaging.gdb\pu_CrashPoints'
stagedCrashPoints = r'D:\SCHED\OffsetCrashes\crashOffsetStaging.gdb\sg_CrashPoints'
inMemCrashPoints = r'in_memory\CrashPoints'
crashPointsLayer = 'CrashPointsLayer'
inMemCrashPointsSubset = r'in_memory\CrashPointsSubset'
##inMemCrashPointsSubsetLayer = 'inMemCrashPointsSubsetLayer'
##inMemCrashPointsReducedSubset = r'in_memory\CrashPointsReducedSubset'
#inMemCrashPointsReducedSubsetLayer = 'inMemCrashPointsReducedSubsetLayer'

useKDOTIntersect = 'True'

# Add the accident records as well since they have the information needed for offsetting
# and also needed to make the selection for which accidents have already been offset
# and which ones still need to be offset.
allKDOTCrashRecords = r'D:\SCHED\OffsetCrashes\readonly@kcarsprd.sde\KCARS.GIS_GEOCODE_ACC_V' # Local accident data source.
pulledCrashRecords = r'D:\SCHED\OffsetCrashes\crashOffsetStaging.gdb\pu_CrashRecords'
stagedCrashRecords = r'D:\SCHED\OffsetCrashes\crashOffsetStaging.gdb\sg_CrashRecords'
###inMemCrashRecords = r'in_memory\CrashRecords'
sortedInMemCrashRecords = r"in_memory/CrashRecordsSorted" # Fails now when used in anything with the value r"in_memory\SortedCrashRecords"
concatAccidentKey = 'ACCIDENT_UNIQUE_KEY'

testingTempOutputLocation = r'D:\SCHED\OffsetCrashes\crashOffsetTemp.gdb\Temp_OffsetOutputs'
testingOutputLocation = r'D:\SCHED\OffsetCrashes\crashOffsetOutput.gdb\OffsetOutput'
productionTempOutputLocation = r'D:\SCHED\OffsetCrashes\geo@CrashLocation.sde\CrashLocation.GEO.TEMP_ACC_OFFSET_PTS_ALL'
productionOutputLocation = r'D:\SCHED\OffsetCrashes\geo@CrashLocation.sde\CrashLocation.GEO.ACC_OFFSET_PTS_ALL'
offsetPointsTempOutputLocation = testingTempOutputLocation
offsetPointsOutputLocation = testingOutputLocation
####offsetPointsOutputLocation_NK = testingOutputLocation + "_NK"
offsetTempPointsTestLayerFile = r'D:\SCHED\OffsetCrashes\Temp_OffsetOutputs_Test.lyr'
offsetTempPointsProdLayerFile = r'D:\SCHED\OffsetCrashes\Temp_OffsetOutputs_Prod.lyr'
offsetTempPointsLayerFile = offsetTempPointsTestLayerFile

longitudeColumnName = 'ddLongitude'
latitudeColumnName = 'ddLatitude'
longLatColumnType = 'DOUBLE'

isOffsetFieldName = 'isOffset'

# Testing
crashPointsTestOutput = r'D:\SCHED\OffsetCrashes\crashOffsetTemp.gdb\inMemCrashPointsTestOutput'

# Pull all the crashes from the overall geocode. Once they're in the staging
# table, compare them to previously offset results and choose up to 50k to attempt
# to geocode per run.

# Open an mxd with the default dataframe set to the projection that you want to
# use and then add each set of data to that data frame with it's input projection
# being whatever was used to create it, but the output of all of the geoprocessing
# tasks being the 

#####from AccidentDirectionMatrixOffsetCode import offsetdirectioncaller
# pattern:
# offsetdirectioncaller(intersectPoints, aliasLocation, centerlineLocation, offsetPointOutput, useKDOTIntersect)
## - Doesn't allow for a projection to be passed in... modify so that it does.
# continuousoffsetcaller(intersectPoints, aliasLocation, centerlineLocation, offsetPointOutput, useKDOTIntersect, processingProjection)

# Looks like AccidentDirectionMatrixOffsetCode already contains a function
# to allow it to be called from an outside process.
# May need to model a new one, but it's good that there's already something
# to base the prototype off of.

# Terri will be X, Y locating to something similar to the projection used by the
# GIS_CANSYS.DBO.CMLRS or GIS_CANSYS.DBO.SMLRS layer in sqlgis_CANSYS_GIS.sde:
#GCS_North_American_1983
#WKID: 4269 Authority: EPSG

#Angular Unit: Degree (0.0174532925199433)
#Prime Meridian: Greenwich (0.0)
#Datum: D_North_American_1983
#  Spheroid: GRS_1980
#    Semimajor Axis: 6378137.0
#   Semiminor Axis: 6356752.314140356
#    Inverse Flattening: 298.257222101

# A copy has been located here and will need to be placed on AR60 for
# this process to access when it is running there against the CHASM
# connection:
# r'C:\GIS\Projections\NAD 1983 Decimal Degrees.prj'
# ||||
# vvvv
# On AR60, the location is r'D:\SCHED\OffsetCrashes\NAD 1983 Decimal Degrees.prj'

# Took 15 minutes to pull the roadways data across. :(
# However, it appears to have succeeded. :D


def moveCrashesFromTempToOutput(offsetTempPoints, offsetPoints, longColumn, latColumn, offsetProjection):
    if Exists(offsetPoints):
        offsetPointsOutputLocationFields = ListFields(offsetPoints)
        offsetPointsOutputLocationFieldNames = [x.name for x in offsetPointsOutputLocationFields]
        # Check to see if the fields exist and add them if not.
        if longColumn in offsetPointsOutputLocationFieldNames:
            print("longitudeColumn found. Will not re-add it.")
        else:
            AddField_management(outputWithOffsetLocations, longColumn, longLatColumnType, 0, 0)
            print("Added the longitudeColumn to the outputWithOffsetLocations FC with a type of " + str(longLatColumnType) + ".")
        
        if latColumn in offsetPointsOutputLocationFieldNames:
            print("latitudeColumn found. Will not re-add it.")
        else:
            AddField_management(outputWithOffsetLocations, latColumn, longLatColumnType, 0, 0)
            print("Added the latitudeColumn to the outputWithOffsetLocations FC with a type of " + str(longLatColumnType) + ".")
        
    else:
        print("The output offsetPoints FC, " + str(offsetPoints) + " does not exist. Will not add fields or transfer offset records.")
    
    offsetTempPointsDescription = Describe(offsetTempPoints)
    offsetTempPointsFields = offsetTempPointsDescription.fields
    offsetTempPointsOIDFieldName = offsetTempPointsDescription.OIDFieldName
    offsetTempPointsShapeFieldName = offsetTempPointsDescription.shapeFieldName
    offsetTempPointsFieldNames = [x.name for x in offsetTempPointsFields if x != offsetTempPointsOIDFieldName and x != offsetTempPointsShapeFieldName]
    
    offsetPointsDescription = Describe(offsetPoints)
    offsetPointsFields = offsetPointsDescription.fields
    offsetPointsOIDFieldName = offsetPointsDescription.OIDFieldName
    offsetPointsShapeFieldName = offsetPointsDescription.shapeFieldName
    offsetPointsFieldNames = [y.name for y in offsetPointsFields if y != offsetPointsOIDFieldName and y != offsetPointsShapeFieldName]
    
    transferFieldNames = [z for z in offsetTempPointsFieldNames if z in offsetPointsFieldNames]
    transferFieldNames.append('SHAPE@')
    
    transferList = list()
    searchCursorWhereClause = ''
    offsetPointsSpatialReference = offsetPointsDescription.SpatialReference
    newCursor = daSearchCursor(offsetTempPoints, transferFieldNames, searchCursorWhereClause, offsetPointsSpatialReference)
    
    for cursorItem in newCursor:
        cursorListItem = list(cursorItem)
        transferList.append(cursorListItem)
    
    try:
        del newCursor
    except:
        pass
    
    newCursor = daInsertCursor(offsetPoints, transferFieldNames)
    
    for transferItem in transferList:
        insertedOID = newCursor.insertRow(transferItem)
        print("Inserted a row with the OID of : " + str(insertedOID) + ".")
    
    try:
        del newCursor
    except:
        pass


def updateTheddLongAndddLatFields(pointLayerToUpdate, isOffsetField, longitudeField, latitudeField):
    print("Updating rows in the " + str(pointLayerToUpdate) + " FC.")
    #testingSearchFields = [isOffsetField, longitudeField, latitudeField, "SHAPE@X", "SHAPE@Y"]
    testingUpdateFields = [isOffsetField, "SHAPE@X", "SHAPE@Y", longitudeField, latitudeField]
    
    updateCursorClause = ''' ''' + str(isOffsetField) + ''' IS NOT NULL '''
    #searchCursorClause = ''' 1 = 1 '''
    # The X & Y Shape tokens should already be in the correct projection, so there is no need to
    # further specify the output spatial reference.
    newCursor = daUpdateCursor(pointLayerToUpdate, testingUpdateFields, updateCursorClause)
    
    for cursorItem in newCursor:
        cursorListItem = list(cursorItem)
        # Set the text fields to the string version of the Shape@X and Shape@Y tokens.
        cursorListItem[3] = cursorListItem[1]
        cursorListItem[4] = cursorListItem[2]
        print("The transferItem has an isOffsetField of: " + str(cursorListItem[0]) + ".")
        print("...and " + str(longitudeField) + "/" + str(latitudeField) + " of: " + str(cursorListItem[3]) + "/" + str(cursorListItem[4]) + ".\n")
        newCursor.updateRow(cursorListItem)
    
    try:
        del newCursor
    except:
        pass


def continuousiteratorprocessDecimalDegreesTest():
    moveCrashesFromTempToOutput(offsetPointsTempOutputLocation, offsetPointsOutputLocation, longitudeColumnName, latitudeColumnName, offsetProjectionToUse)
    updateTheddLongAndddLatFields(offsetPointsOutputLocation, isOffsetFieldName, longitudeColumnName, latitudeColumnName)


def pullDataFromChasm():
    # Pull the data from the CHASM
    # connection and put it into the staging tables.
    # Run the comparisons between the staging tables
    # and the slightlyvetted tables.
    # If the comparisons let the staging tables pass,
    # then move the data from the staging tables
    # to the slightlyvetted tables.
    print("Pulling data from CHASM and local connections.")
    dataLocationsListContainer = list()
    roadwaysLocationsList = [chasmNG911Roadways, pulledNG911Roadways, stagedNG911Roadways]
    aliasTableLocationsList = [chasmAliasTable, pulledAliasTable, stagedAliasTable]
    crashPointsLocationsList = [chasmOutputCrashPoints, pulledCrashPoints, stagedCrashPoints]
    crashRecordsLocationsList = [allKDOTCrashRecords, pulledCrashRecords, stagedCrashRecords]
    
    dataLocationsListContainer.append(roadwaysLocationsList)
    dataLocationsListContainer.append(aliasTableLocationsList)
    dataLocationsListContainer.append(crashPointsLocationsList)
    dataLocationsListContainer.append(crashRecordsLocationsList)
    
    for dataLocationsList in dataLocationsListContainer:
        chasmFCToPulledFC(dataLocationsList[0],dataLocationsList[1])
        pulledFCToStagedFC(dataLocationsList[1],dataLocationsList[2])


def chasmFCToPulledFC(chasmFC, pulledFC):
    print("Moving the data from " + str(chasmFC) + " to " + str(pulledFC) + ".")
    try:
        if Exists(chasmFC):
            deleteSecondAndOverwriteWithFirst(chasmFC, pulledFC)
        else:
            print("Could not connect to the chasmFC at: " + str(chasmFC) + ".")
    except:
        print("There was a problem moving the data from the Chasm FC: " + str(chasmFC) + ".")


def pulledFCToStagedFC(pulledFC, stagedFC):
    ''' If the pulledFC has at least 75% as many features as the stagedFC,
        overwrite the stagedFC with the pulledFC.'''
    print("Moving the data from " + str(pulledFC) + " to " + str(stagedFC) + ".")
    if Exists(pulledFC):
        if Exists(stagedFC):
            pulledResult = GetCount_management(pulledFC)
            pulledCount = int(pulledResult.getOutput(0))
            stagedResult = GetCount_management(stagedFC)
            stagedCount = int(stagedResult.getOutput(0))
            if int(stagedCount) <= 1:
                stagedCount = 1
            else:
                pass
            
            if (pulledCount >= int(.75 * stagedCount)):
                deleteSecondAndOverwriteWithFirst(pulledFC, stagedFC)
            else:
                pass
        else:
            deleteSecondAndOverwriteWithFirst(pulledFC, stagedFC)
    else:
        print("Will not delete the stagedFC, if it exists.")


def deleteSecondAndOverwriteWithFirst(firstFC, secondFC):
    try:
        Delete_management(secondFC)
    except:
        print("Could not delete the FC at: " + str(secondFC) + ".")
    try:
        firstFCDescription = Describe(firstFC)
        try:
            testDataType = firstFCDescription.dataType
        except:
            print("firstFCDescription.dataType does not exist.")
        if str(firstFCDescription.dataType).lower() == 'table':
            CopyRows_management(firstFC, secondFC)
        else:
            CopyFeatures_management(firstFC, secondFC)
    except:
        print("Could not copy features from " + str(firstFC) + " to " + str(secondFC) + ".")


def copyFeaturesAndCreateFeatureLayer(fcFirstLocation, fcSecondLocation, fcFeatureLayerName):
    CopyFeatures_management(fcFirstLocation, fcSecondLocation)
    MakeFeatureLayer_management(fcSecondLocation, fcFeatureLayerName)


def continuousiteratorprocess():
    print("Starting the continous iterator process.")
    # In order to quickly work with the features, they need to be moved to in_memory.
    # In order to select features, they need to be feature layers.
    
    useKDOTIntersect = True
    
    ##copyFeaturesAndCreateFeatureLayer(adminDistrictsLocation, inMemAdminDistricts, adminDistrictsLayer)
    ##print("Admin districts layer created.")
    #copyFeaturesAndCreateFeatureLayer(stagedNG911Roadways, inMemNG911Roadways, ng911RoadwaysLayer)
    #print("NG911 roadways layer created.")
    # Use this to prevent the joining of fields onto stagedCrashPoints from persisting.
    copyFeaturesAndCreateFeatureLayer(stagedCrashPoints, inMemCrashPoints, crashPointsLayer)
    ##MakeFeatureLayer_management(stagedCrashPoints, crashPointsLayer)
    print("Crash points layer created.")
    ##CopyFeatures_management(stagedNG911Roadways, inMemNG911Roadways)
    ##print("inMemNG911Roadways created.")
    ##CopyRows_management(stagedAliasTable, inMemAliasTable)
    ##CopyRows_management(stagedCrashRecords, inMemCrashRecords)
    
    # Need to reduce the potentially offsettable crash points here by comparing them to
    # the crash points output table so that we don't try to re-offset the crash points
    # that have already been offset and output.
    
    # Currently need to just match the ACCIDENT_KEY with the data from the original accident record.
    # Reverse sort the Join FC by created date because only the first occurrence of the
    sortFieldsAndOrders = [['CREATED_DATE', 'DESCENDING']]
    Sort_management(stagedCrashRecords, sortedInMemCrashRecords, sortFieldsAndOrders)
    # Replacement function since Sort_management stopped working.
    ###CopyRows_management(stagedCrashRecords, sortedInMemCrashRecords)
    ###TruncateTable_management(sortedInMemCrashRecords)
    ''' # Commented in the hope that Sort_management will work if memory usage is kept below 1.4GB.
    crashRecordsDescription = Describe(inMemCrashRecords)
    crashRecordFields = crashRecordsDescription.fields
    crashRecordOIDFieldName = crashRecordsDescription.OIDFieldName
    
    crashRecordFieldNames = [x.name for x in crashRecordFields]
    transferFields = [y for y in crashRecordFieldNames if y != crashRecordOIDFieldName]
    
    # Get the position in the list of transferFields for the sort field.
    sortFieldIndex = transferFields.index(sortFieldsAndOrders[0][0])
    
    newCursor = daSearchCursor(inMemCrashRecords, transferFields)
    
    transferRows = list()
    for cursorItem in newCursor:
        transferRows.append(cursorItem)
    
    try:
        del newCursor
    except:
        pass
    
    sortedTransferRows = sorted(transferRows, key=lambda transferRowItem: transferRowItem[sortFieldIndex], reverse=True)
    
    newCursor = daInsertCursor(sortedInMemCrashRecords, transferFields)
    for transferItem in sortedTransferRows:
        newCursor.Insert(transferItem)
    
    try:
        del newCursor
    except:
        pass
    ''' # Commented in the hope that Sort_management will work if memory usage is kept below 1.4GB.
    # Looks like the script was indeed crashing because it was using too much memory.
    # Without copying the Crash Records to in_memory, the script is going further.
    
    
    crashPointsKeyField = 'ACCIDENT_KEY'
    joinTable = sortedInMemCrashRecords
    joinTableKeyField = 'ACCIDENT_KEY'
    # Add the CREATED_DATE and the offset fields. Then, make a field for and calculate the concatAccidentKey.
    existingEndpointFCFieldObjects = ListFields(inMemCrashPoints)
    existingEndpointFCFieldNames = [x.name for x in existingEndpointFCFieldObjects]
    uncheckedFieldsToJoinFromFCToFC = ['ON_ROAD_KDOT_NAME', 'AT_ROAD_KDOT_DIRECTION', 'AT_ROAD_KDOT_DIST_FEET', 'AT_ROAD_KDOT_NAME', 'CREATED_DATE']
    fieldsToJoinFromFCToFC = [y for y in uncheckedFieldsToJoinFromFCToFC if y not in existingEndpointFCFieldNames]
    fieldsToUseForJoining = [joinTableKeyField]
    fieldsToUseForJoining += fieldsToJoinFromFCToFC
    accUniqueKeyFieldName = concatAccidentKey
    accUniqueKeyFieldLength = 25
    accUniqueKeyFieldType = 'Text'
    accUniqueKeyFieldNullsAllowed = 'NULLABLE'
    joinedTimeFieldName = 'CREATED_DATE'
    inputTimeFormatString = 'M/d/yyyy h:mm:ss tt'
    convertedTimeFieldName = 'Converted_Time'
    outputTimeFieldType = 'Text'
    outputTimeFormatString = 'yyyyMMddhhmmss' # the y/d used above are instead Y/D here. -- Help may be incorrect.
    ####  20100104666YYYY04DD044716 is an example of the concatenated output generated, when the Y and D are capitalized.
    
    #Test this portion#
    print("Joining the fields " + str(fieldsToJoinFromFCToFC) + " to the " + str(inMemCrashPoints) + " feature class.")
    #Use a dict that holds the info needed to add the fields.
    #See if you can get it from the field objects, but get it in by hand otherwise.
    joinTableFieldObjects = ListFields(joinTable)
    
    joinFieldsDict = dict()
    
    for fieldToJoinName in fieldsToJoinFromFCToFC:
        for fieldObject in joinTableFieldObjects:
            fieldObjectName = fieldObject.name
            if (fieldToJoinName.lower() == fieldObjectName.lower()):
                joinFieldsDict[fieldObject.name] = [fieldObject.type, fieldObject.precision, fieldObject.scale, fieldObject.length,
                    fieldObject.aliasName, fieldObject.isNullable, fieldObject.required, fieldObject.domain]
    
    joinFieldsDictKeys = joinFieldsDict.keys()
    
    # Give defaults for those columns if they were not found for some reason.
    if "ON_ROAD_KDOT_NAME" not in joinFieldsDictKeys:
        joinFieldsDict["ON_ROAD_KDOT_NAME"] = ["TEXT", "", "", 50, "ON_ROAD_KDOT_NAME", "NULLABLE", "", ""]
    if "AT_ROAD_KDOT_DIRECTION" not in joinFieldsDictKeys:
        joinFieldsDict["AT_ROAD_KDOT_DIRECTION"] = ["TEXT", "", "", 2, "AT_ROAD_KDOT_DIRECTION", "NULLABLE", "", ""]
    if "AT_ROAD_KDOT_DIST_FEET" not in joinFieldsDictKeys:
        joinFieldsDict["AT_ROAD_KDOT_DIST_FEET"] = ["DOUBLE", "", "", "", "AT_ROAD_KDOT_DIST_FEET", "NULLABLE", "", ""]
    if "AT_ROAD_KDOT_NAME" not in joinFieldsDictKeys:
        joinFieldsDict["AT_ROAD_KDOT_NAME"] = ["TEXT", "", "", 50, "AT_ROAD_KDOT_NAME", "NULLABLE", "", ""]
    if "AT_ROAD_KDOT_NAME" not in joinFieldsDictKeys:
        joinFieldsDict["AT_ROAD_KDOT_NAME"] = ["TEXT", "", "", 50, "AT_ROAD_KDOT_NAME", "NULLABLE", "", ""]
    if "CREATED_DATE" not in joinFieldsDictKeys:
        joinFieldsDict["CREATED_DATE"] = ["DATE", "", "", "", "CREATED_DATE", "NULLABLE", "", ""]
    
    print("Adding the fields " + str(fieldsToJoinFromFCToFC) + " to the " + str(inMemCrashPoints) + " feature class.")
    for joinFieldKey in joinFieldsDict.keys():
        AddField_management(inMemCrashPoints, joinFieldKey, joinFieldsDict[joinFieldKey][0], joinFieldsDict[joinFieldKey][1],
            joinFieldsDict[joinFieldKey][2], joinFieldsDict[joinFieldKey][3], joinFieldsDict[joinFieldKey][4], joinFieldsDict[joinFieldKey][5],
            joinFieldsDict[joinFieldKey][6], joinFieldsDict[joinFieldKey][7])
    
    newCursor = daSearchCursor(joinTable, fieldsToUseForJoining)
    
    # New dict to hold the data in the columns that were added earlier and specified for use in the search cursor.
    transferDataDict = dict()
    
    for cursorRow in newCursor:
        fieldsDataList = transferDataDict.get(cursorRow[0], None)
        if fieldsDataList is None:
            fieldsDataList = list()
        else:
            pass
        fieldsDataList.append(cursorRow)
        transferDataDict[cursorRow[0]] = fieldsDataList
    
    try:
        del newCursor
    except:
        pass
    
    print("Updating the fields " + str(fieldsToJoinFromFCToFC) + " in the " + str(inMemCrashPoints) + " feature class.")
    newCursor = daUpdateCursor(inMemCrashPoints, fieldsToUseForJoining)
    
    for cursorRow in newCursor:
        cursorListItem = list(cursorRow)
        matchingContainerItem = transferDataDict.get(cursorListItem[0], None)
        # The fieldsDataList from the joinFieldsDict should always be a list with at least one list inside of it.
        # If there is more than one list, we still just want the first one since the list is already sorted.
        if matchingContainerItem is not None:
            #print("The matchingContainerItem is: " + str(matchingContainerItem) + ".") # print for debugging
            matchingListItem = matchingContainerItem[0]
            if matchingListItem is not None:
                #print("The matchingListItem is: " + str(matchingListItem) + ".") # print for debugging
                matchingListLen = len(matchingListItem)
                cursorFieldsLen = len(fieldsToUseForJoining)
                if matchingListLen == cursorFieldsLen:
                    newCursor.updateRow(matchingListItem)
                else:
                    continue # go on to the next row without updating anything
            else:
                continue # go on to the next row without updating anything
        else:
            continue # go on to the next row without updating anything
    
    
    FCDescription = Describe(inMemCrashPoints)
    FieldObjectsList = FCDescription.fields
    FieldNamesList = [x.name for x in FieldObjectsList]
    if accUniqueKeyFieldName not in FieldNamesList:
        AddField_management(inMemCrashPoints, accUniqueKeyFieldName, accUniqueKeyFieldType, "", "", accUniqueKeyFieldLength,
            accUniqueKeyFieldName, accUniqueKeyFieldNullsAllowed)
        
        # Convert the CREATED_DATE to a string of numbers.
        ConvertTimeField_management(inMemCrashPoints, joinedTimeFieldName, inputTimeFormatString, convertedTimeFieldName, outputTimeFieldType, outputTimeFormatString)
        # Calculate the unique key from the accident key and converted time field.
        expressionText = "!" + str(crashPointsKeyField) + "! + !" + str(convertedTimeFieldName) + "!"
        CalculateField_management(inMemCrashPoints, accUniqueKeyFieldName, expressionText, "PYTHON_9.3")
        # After you calculate the concatenated unique key, delete the converted time field.
        #try:
        #    DeleteField_management(inMemCrashPoints, convertedTimeFieldName)
        #except:
        #    print("Could not delete the converted time field: " + str(convertedTimeFieldName) + ".")
        
    else:
        print("Great news! The " + str(accUniqueKeyFieldName) + " field already exists.")
        print("It does not need to be added and have its values calculated.")
    
    inputKeySearchFields = [accUniqueKeyFieldName]
    inputKeySearchQuery = """ """ + str(accUniqueKeyFieldName) + """ IS NOT NULL AND """ + str(convertedTimeFieldName) + """ IS NOT NULL """
    newCursor = daSearchCursor(inMemCrashPoints, inputKeySearchFields, inputKeySearchQuery)
    
    crashPointsSelectionList = []
    for cursorItem in newCursor:
        crashPointsSelectionList.append(str(cursorItem[0]))
    
    try:
        del newCursor
    except:
        pass
    
    # After you calculate the concatenated unique key, delete the converted time field.
    print("Deleting the field " + str(convertedTimeFieldName) + " from the FC: " + str(inMemCrashPoints) + ".")
    DeleteField_management(inMemCrashPoints, convertedTimeFieldName)
    
    if (Exists(offsetPointsOutputLocation)):
        print("Removing rows with unique keys from the selection list where those unique keys already exist in the output.")
        
        outputKeySearchFields = [accUniqueKeyFieldName]
        outputKeySearchQuery = """ """ + str(accUniqueKeyFieldName) + """ IS NOT NULL """
        newCursor = daSearchCursor(offsetPointsOutputLocation, outputKeySearchFields, outputKeySearchQuery)
        
        offsetPointsKeyValuesList = []
        for cursorItem in newCursor:
            offsetPointsKeyValuesList.append(str(cursorItem[0]))
        
        try:
            del newCursor
        except:
            pass
        
        crashPointsSelectionListReduced = [x for x in crashPointsSelectionList if x not in offsetPointsKeyValuesList]
        
        crashPointsSelectionList = crashPointsSelectionListReduced
        print("Done removing existing keys from the selection list.")
        
    else:
        print("The output crash points table: " + str(offsetPointsOutputLocation) + " does not exist.")
        print("Therefore, there are no unique keys to remove from the selection list.")
    
    # Create a random sample of the remaining values in the crashPointsSelectionList.
    
    uniqueKeyListReduced = random.sample(crashPointsSelectionList, maxCrashPointsToProcess)
    
    #### Commented out for testing.
    # Select only the reduced number of features in the feature class
    # Build a dynamic selectionQuery and reselect the features in the feature class
    ####createDynamicAttributeSelection(crashPointsLayer, uniqueKeyListReduced, accUniqueKeyFieldName, accUniqueKeyFieldType)
    
    print("Copying the selected features from the larger inMemory feature class to a smaller subset feature class.")
    CopyFeatures_management(crashPointsLayer, inMemCrashPointsSubset)
    
    Delete_management(crashPointsLayer)
    
    crashPointsDescription = Describe(inMemCrashPoints)
    crashPointsSR = crashPointsDescription.spatialReference
    del crashPointsDescription
    
    # Adding test output to confirm that things are working correctly.
    if Exists(crashPointsTestOutput):
        try:
            Delete_management(crashPointsTestOutput)
        except:
            pass
    else:
        pass
    
    CopyFeatures_management(inMemCrashPoints, crashPointsTestOutput)
    time.sleep(10)
    raise ValueError("Check to see if the fields have been correctly added and updated.") # End of Test
    
    Delete_management(inMemCrashPoints)
    print("The crashPointsSR is: " + str(crashPointsSR) + ".")
    gc.collect()
    print("The call to gc.collect() has completed.")
    
    print("Starting the continuous offset caller.")
    continuousoffsetcaller(inMemCrashPointsSubset, stagedNG911Roadways, stagedAliasTable, offsetPointsTempOutputLocation, useKDOTIntersect,
        crashPointsSR, roadwayTableGCID, aliasTableGCID, aliasTableNameFields, additionalNameColumnBaseName, concatAccidentKey) 
    
    createOutputLocationWithSR(offsetPointsOutputLocation, offsetProjectionToUse, offsetPointsTempOutputLocation)
    moveCrashesFromTempToOutput(offsetPointsTempOutputLocation, offsetPointsOutputLocation, longitudeColumnName, latitudeColumnName, offsetProjectionToUse)
    updateTheddLongAndddLatFields(offsetPointsOutputLocation, isOffsetFieldName, longitudeColumnName, latitudeColumnName)


def createOutputLocationWithSR(offsetPoints, offsetProjection, offsetTempPoints):
    if Exists(offsetPoints):
        pass
    else:
        ##CreateFeatureclass_management (out_path, out_name, {geometry_type}, {template}, {has_m}, {has_z}, {spatial_reference})
        fcOutPath = returnGDBOrSDEPath(offsetPoints)
        fcOutName = returnFeatureClass(offsetPoints)
        # Create feature class using the temp points feature class as a template.
        CreateFeatureclass_management(fcOutPath, fcOutName, "POINT", offsetTempPoints, "DISABLED", "DISABLED", offsetProjection)
        # Another function will add missing fields, if any.
        print("The offsetPoints was created at" + str(offsetPoints) + ".")

"""
def moveCrashesFromTempToOutputWithLongAndLat(offsetTempPoints, offsetPoints, longColumn, latColumn):    
    # This method doesn't work.
    # Use Pyproj instead to generate new x/y using the existing x/y along with the old projection and the new projection.
    
    # get spatial reference from fc1
    # get spatial reference from fc2
    # convert the spatial references to well known text (wkt)
    # use a searchCursor to get all of the offsetTempPoints
    # convert the point locations from the old spatial reference to the new spatial reference
    # using PyProj.
    # Also add the values to each list/row for the new x/y as DOUBLE values for the decimal degrees.
    # use an insertCursor to move the offsetTempPoints to the offsetPoints output. 
    
    # Check for the ddLat/ddLong fields in the output.
    # Add them if they don't already exist.
    
    if Exists(offsetPoints):
        offsetPointsOutputLocationFields = ListFields(offsetPoints)
        offsetPointsOutputLocationFieldNames = [x.name for x in offsetPointsOutputLocationFieldNames]
        # Check to see if the fields exist and add them if not.
        if longitudeColumn in offsetPointsOutputLocationFieldNames:
            print("longitudeColumn found. Will not re-add it.")
        else:
            AddField_management(outputWithOffsetLocations, longitudeColumn, longLatColumnType, 0, 0)
            print("Added the longitudeColumn to the outputWithOffsetLocations FC with a type of " + str(longLatColumnType) + ".")
        
        if latitudeColumn in offsetPointsOutputLocationFieldNames:
            print("latitudeColumn found. Will not re-add it.")
        else:
            AddField_management(outputWithOffsetLocations, latitudeColumn, longLatColumnType, 0, 0)
            print("Added the latitudeColumn to the outputWithOffsetLocations FC with a type of " + str(longLatColumnType) + ".")
        
        # Instead of doing this, make a transfer set and transfer all of the data + the long/lat values to the new feature class
        # from the temp feature class. -- Will need to include all of the matching fields except for the OID field for either
        # one.
        #Then
        longLatUpdateFields = ['OID@', longitudeColumn, latitudeColumn, 'SHAPE@X', 'SHAPE@Y']
        cursorClause = ''' ''' + str(longitudeColumn) + ''' IS NULL OR ''' + str(latitudeColumn) + ''' IS NULL '''
        cursorSpatialReference = r"D:\SCHED\OffsetCrashes\NAD 1983 Decimal Degrees.prj"
        newCursor = daUpdateCursor(offsetPoints, longLatUpdateFields, cursorClause, cursorSpatialReference)
        
        for cursorItem in newCursor:
            cursorListItem = list(cursorItem)
            cursorListItem[1] = cursorListItem[3]
            cursorListItem[2] = cursorListItem[4]
            newCursor.Update(cursorListItem)
        try:
            del newCursor
        except:
            pass
        
    else:
        print("The offsetPoints FC does not exist. Will not add fields or calculate latitude/longitude values.")
    
    transferList = list()
    newCursor = daSearchCursor(offsetTempPoints)
    
    for cursorItem in newCursor:
        cursorListItem = list(cursorItem)
        transferList.append(cursorListItem)
    
    try:
        del newCursor
    except:
        pass
    
    newCursor = daInsertCursor(offsetPoints)
    
    try:
        del newCursor
    except:
        pass
    
    mxdToUse = MapDocument(blankMXD)
    
    allDataFrames = ListDataFrames(blankMXD)
    
    dataFrameToUse = ''
    try:
        dataFrameToUse = allDataFrames[0]
    except:
        print("Could not assign the dataFrameToUse from the allDataFrames list.")
    
    spatialReferenceToAssign = SpatialReference(offsetProjectionToUse)
    
    dataFrameToUse.spatialReference = spatialReferenceToAssign
    
    AddLayer(dataFrameToUse, offsetTempPointsLayerFile)
    
    layersList = ListLayers(blankMXD, "", dataFrameToUse)
    
    offsetTempPointsLoadedLayer = layersList[0]
    
    if offsetTempPointsLoadedLayer is None:
        print("The offsetTempPointsLoadedLayer does not exist.")
    else:
        pass
    
    offsetLoadedTempFields = ListFields(offsetTempPointsLoadedLayer)
    offsetLoadedTempFieldNames = [x.name for x in offsetLoadedTempFields]
    
    # Have to use a step here to add and calculate the calculate the dd fields with the data frame's spatial reference.
    # Check to see if the fields exist and add them if not.
    if longitudeColumn not in offsetLoadedTempFieldNames:
        AddField_management(offsetTempPointsLoadedLayer, longitudeColumn, "DOUBLE", 0, 0)
    else:
        pass
    
    if latitudeColumn not in offsetLoadedTempFieldNames:
        AddField_management(offsetTempPointsLoadedLayer, latitudeColumn, "DOUBLE", 0, 0)
    else:
        pass
    
    # Calculate the fields with the spatial reference from the data frame instead
    # of the spatial reference that they have...
    
    offsetTempFields = ListFields(offsetTempPointsLoadedLayer)
    offsetTempFieldNames = [x.name for x in offsetTempPointsLoadedLayer]
    offsetOutputFields = ListFields(offsetPoints)
    offsetOutputFieldNames = [y.name for y in offsetOutputFields]
    transferFieldNames = [z for z in offsetTempFieldNames if z in offsetOutputFieldNames]
    
    transferList = list()
    
    newCursor = daSearchCursor(offsetTempPointsLoadedLayer, transferFieldNames)
    
    for cursorItem in newCursor:
        transferList.append(cursorItem)
    
    try:
        del newCursor
    except:
        pass
    
    newCursor = daInsertCursor(offsetPoints, transferFieldNames)
    
    for transferItem in transferList:
        newCursor.Insert(transferItem)
    
    try:
        del newCursor
    except:
        pass
    
    pass
"""

# Call this after everything else to add the lat/long that Terri needs 
# to insert the data back into KCARS.
## For best results, open the temp feature class in a data frame and set the data frame
## to the output prj, then export the temp feature class to a reprojected temp feature class
## and cursor the output from that reprojected feature class into the prod feature class
## prior to calling this function.

### Don't need to do the above, just open a SearchCursor and pass in the dd Projection as the
### spatial reference.
def calculateOutputLocationLongAndLat(offsetPoints, longitudeColumn, latitudeColumn):
    # This needs to create the x&y columns if the don't exist, then update them with correct Lat/Long in Decimal Degrees.
    if Exists(offsetPoints):
        offsetPointsOutputLocationFields = ListFields(offsetPoints)
        offsetPointsOutputLocationFieldNames = [x.name for x in offsetPointsOutputLocationFieldNames]
        # Check to see if the fields exist and add them if not.
        if longitudeColumn in offsetPointsOutputLocationFieldNames:
            pass
        else:
            AddField_management(outputWithOffsetLocations, longitudeColumn, "DOUBLE", 0, 0)
        
        if latitudeColumn in offsetPointsOutputLocationFieldNames:
            pass
        else:
            AddField_management(outputWithOffsetLocations, latitudeColumn, "DOUBLE", 0, 0)
        
        #Then
        longLatUpdateFields = ['OID@', longitudeColumn, latitudeColumn, 'SHAPE@X', 'SHAPE@Y']
        cursorClause = ''' ''' + str() + ''' IS NULL OR ''' + str() + ''' IS NULL '''
        cursorSpatialReference = r"D:\SCHED\OffsetCrashes\NAD 1983 Decimal Degrees.prj"
        newCursor = daUpdateCursor(offsetPoints, longLatUpdateFields, cursorClause, cursorSpatialReference)
        
        for cursorItem in newCursor:
            cursorListItem = list(cursorItem)
            cursorListItem[1] = cursorListItem[3]
            cursorListItem[2] = cursorListItem[4]
            newCursor.Update(cursorListItem)
        try:
            del newCursor
        except:
            pass
        
    else:
        print("The offsetPoints FC does not exist. Will not add fields or calculate latitude/longitude values.")


def createDynamicAttributeSelection(featureClassToSelect, attributeValuesList, attributeColumnName, attributeColumnType):
    # build the selection list & select up to but not more than 999 features at at time
    selectionCounter = 0
    if attributeColumnType.lower() in ('text', 'date', 'guid'): # Use single quotes around each selection value
        featureClassSelectionClause = """ """ + str(attributeColumnName) + """ IN ("""
        
        # Clear the selection to start fresh
        SelectLayerByAttribute_management(featureClassToSelect, "CLEAR_SELECTION")
        for attributeValueItem in attributeValuesList:        
            if selectionCounter <= 998:
                featureClassSelectionClause = featureClassSelectionClause + """'""" + str(attributeValueItem) + """', """
                selectionCounter += 1
            else:
                # Remove the trailing ", " and add a closing parenthesis.
                featureClassSelectionClause = featureClassSelectionClause[:-2] + """) """
                #Debug only
                #print("featureClassSelectionClause " + str(featureClassSelectionClause))
                SelectLayerByAttribute_management(featureClassToSelect, "ADD_TO_SELECTION", featureClassSelectionClause)
                
                selectionCounter = 0
                featureClassSelectionClause = """ """ + str(attributeColumnName) + """ IN ("""
                featureClassSelectionClause = featureClassSelectionClause + """'""" + str(attributeValueItem) + """', """
            
        if selectionCounter > 0:
            # Remove the trailing ", " and add a closing parenthesis.
            #Debug only
            #print("featureClassSelectionClause " + str(featureClassSelectionClause))
            featureClassSelectionClause = featureClassSelectionClause[:-2] + """) """
            SelectLayerByAttribute_management(featureClassToSelect, "ADD_TO_SELECTION", featureClassSelectionClause)
        else:
            pass
    else: # Don't use quotes around each selection value
        featureClassSelectionClause = """ """ + str(attributeColumnName) + """ IN ("""
        
        # Clear the selection to start fresh
        SelectLayerByAttribute_management(featureClassToSelect, "CLEAR_SELECTION")
        for attributeValueItem in attributeValuesList:        
            if selectionCounter <= 998:
                featureClassSelectionClause = featureClassSelectionClause + str(attributeValueItem) + """, """
                selectionCounter += 1
            else:
                # Remove the trailing ", " and add a closing parenthesis.
                featureClassSelectionClause = featureClassSelectionClause[:-2] + """) """
                #Debug only
                #print("featureClassSelectionClause " + str(featureClassSelectionClause))
                SelectLayerByAttribute_management(featureClassToSelect, "ADD_TO_SELECTION", featureClassSelectionClause)
                
                selectionCounter = 0
                featureClassSelectionClause = """ """ + str(attributeColumnName) + """ IN ("""
                featureClassSelectionClause = featureClassSelectionClause + str(attributeValueItem) + """, """
            
        if selectionCounter > 0:
            # Remove the trailing ", " and add a closing parenthesis.
            #Debug only
            #print("featureClassSelectionClause " + str(featureClassSelectionClause))
            featureClassSelectionClause = featureClassSelectionClause[:-2] + """) """
            SelectLayerByAttribute_management(featureClassToSelect, "ADD_TO_SELECTION", featureClassSelectionClause)
        else:
            pass
    
    print("Counting selection...")
    selectedFeaturesResult = GetCount_management(featureClassToSelect)
    selectedFeaturesCount = int(selectedFeaturesResult.getOutput(0))
    print("Selected " + str(selectedFeaturesCount) + " features in the " + str(featureClassToSelect) + " layer.")


# This needs to call AccidentDirectionMatrixOffsetCode.py and give it
# the necessary parameters for each feature class of intersected points
# that is in the crashLocation sql instance.

# TODO: Build a reporting script that gives me the information on the 
# number of points that were successfully geocoded per county and
# also as an aggregate, for both KDOT and non-KDOT fields.

# Should also give information on how many points have
# been successfully offset from those geocoded points.
def main():
    #pullDataFromChasm() # Skipping for now due to the long time that it takes.
    continuousiteratorprocess()
    #accOffMainTest()
    #continuousiteratorprocessDecimalDegreesTest()


if __name__ == "__main__":
    main()
else:
    pass