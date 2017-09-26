#!/usr/bin/env/ python
#continousProcessOffsetScript.py
# Created by dirktall04 on 2017-07-26
# Large portions of this code were adapted from:
# AccidentDirectionMatrixOffsetCode.py

# Rebuild the accident offset script to use a fixed source of Roadway data (NG911) and just select it by District.

# Can select geolocated crash points based on an intersection with District.
# Then can select roadways based on an intersection with a buffer around each county (2 mi).
# That should allow for offsetting of points up to 2 mi away from the intersection points
# and do so without causing uncertainty about which PSAPs are where in relation to which
# counties.

# See what happens if you try to select geolocated crash points based on
# an intersection with the Admin Districts. Then you would only need to have 6
# selections or outer loops and this could even be pushed to 6 cores or
# 6 machines, if need be, in the future.

import os
import random
from arcpy import (AddField_management, CalculateField_management, ConvertTimeField_management,
    CopyFeatures_management, CopyRows_management, CreateFeatureclass_management, Delete_management,
    DeleteField_management, Describe, env, Exists, GetCount_management, ListFeatureClasses,
    JoinField_management, MakeFeatureLayer_management, SelectLayerByAttribute_management,
    SelectLayerByLocation_management, Sort_management, SpatialReference, TruncateTable_management)
from arcpy.da import (SearchCursor as daSearchCursor)  # @UnresolvedImport
from AccidentDirectionMatrixOffsetCode import continuousoffsetcaller
# Doesn't seem like the daily vs overall property will need to be implemented.
##from lastRun import dailyOrOverall

from pathFunctions import (returnGDBOrSDEPath,
    returnFeatureClass, returnGDBOrSDEName)

env.overwriteOutput = True
newSelection = "NEW_SELECTION"
maxCrashPointsToProcess = 5000

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

##adminDistrictsLocation = r'D:\SCHED\OffsetCrashes\readonly@sdeprod.sde\SHARED.KDOT_ADMIN_DISTRICTS'
# For testing:
#adminDistrictsLocation = r'D:\SCHED\OffsetCrashes\readonly@sdeprod.sde\SHARED.KDOT_MASA_DISTRICTS'
##adminDistrictsLocation = r'C:\GIS\SCHED\OffsetCrashes\readonly@sdeprod.sde\SHARED.KDOT_ADMIN_DISTRICTS'
##inMemAdminDistricts = r'in_memory\adminDistricts'
##adminDistrictsLayer = 'adminDistrictsAsALayer'

gdbForStaging = r'D:\SCHED\OffsetCrashes\crashOffsetStaging.gdb'
offsetProjectionToUse = r'D:\SCHED\OffsetCrashes\NAD 1983 Decimal Degrees.prj'

chasmNG911Roadways = r'D:\CHASM\CHASM.sde\KDOT.ROADCENTERLINE_NG911'
pulledNG911Roadways = r'D:\SCHED\OffsetCrashes\crashOffsetStaging.gdb\pu_NG911Roadways'
stagedNG911Roadways = r'D:\SCHED\OffsetCrashes\crashOffsetStaging.gdb\sg_NG911Roadways'
inMemNG911Roadways = r"in_memory\NG911Roadways"
##ng911RoadwaysLayer = 'ng911RoadwaysAsALayer' # Might not be needed.
##inMemRoadwaysSubset = r'in_memory\NG911RoadwaysSubset'

chasmAliasTable = r'D:\CHASM\CHASM.sde\KDOT.ROADALIAS_NG911'
pulledAliasTable = r'D:\SCHED\OffsetCrashes\crashOffsetStaging.gdb\pu_AliasTable'
stagedAliasTable = r'D:\SCHED\OffsetCrashes\crashOffsetStaging.gdb\sg_AliasTable'
# Will have to see if this needs special loading into the data frame.
inMemAliasTable = r'in_memory\AliasTable'

# Since the logic for the pulled & staged tables doesn't apply to the Daily
# geocode, the only crashes that will be pulled & staged will be from the Overall.
# However, I will need to include the data from the original crash record
# since the number of columns in the output is greatly reduced from the
# original crash record source.
chasmOutputCrashPoints = r'D:\CHASM\CHASM.sde\KDOT.KGS_CRASH_OUTPUT'
pulledCrashPoints = r'D:\SCHED\OffsetCrashes\crashOffsetStaging.gdb\pu_CrashPoints'
stagedCrashPoints = r'D:\SCHED\OffsetCrashes\crashOffsetStaging.gdb\sg_CrashPoints'
inMemCrashPoints = r'in_memory\CrashPoints'
inMemCrashPointsLayer = 'CrashPointsLayer'
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
inMemCrashRecords = r'in_memory\CrashRecords'
sortedInMemCrashRecords = r"in_memory/CrashRecordsSorted" # Fails now when used in anything with the value r"in_memory\SortedCrashRecords"
concatAccidentKey = 'ACCIDENT_UNIQUE_KEY'


testingTempOutputLocation = r'D:\SCHED\OffsetCrashes\crashOffsetStaging.gdb\Temp_OffsetOutputs'
testingOutputLocation = r'D:\SCHED\OffsetCrashes\crashOffsetStaging.gdb\OffsetOutput'
productionTempOutputLocation = r'D:\SCHED\OffsetCrashes\geo@CrashLocation.sde\CrashLocation.GEO.TEMP_ACC_OFFSET_PTS_ALL'
productionOutputLocation = r'D:\SCHED\OffsetCrashes\geo@CrashLocation.sde\CrashLocation.GEO.ACC_OFFSET_PTS_ALL'
offsetPointsTempOutputLocation = testingTempOutputLocation
offsetPointsOutputLocation = testingOutputLocation
####offsetPointsOutputLocation_NK = testingOutputLocation + "_NK"

longitudeColumnName = 'ddLongitude'
latitudeColumnName = 'ddLatitude'

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
# On AR60, the location is r'D:\SCHED\OffsetCrashes\NAD 1983 Decimal Degrees.prj'

# Took 15 minutes to pull the roadways data across. :(
# However, it appears to have succeeded. :D


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
    copyFeaturesAndCreateFeatureLayer(stagedCrashPoints, inMemCrashPoints, inMemCrashPointsLayer)
    print("Crash points layer created.")
    CopyFeatures_management(stagedNG911Roadways, inMemNG911Roadways)
    print("inMemNG911Roadways created.")
    CopyRows_management(stagedAliasTable, inMemAliasTable)
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
    
    inMemCrashPointsKeyField = 'ACCIDENT_KEY'
    joinTable = sortedInMemCrashRecords
    joinTableKeyField = 'ACCIDENT_KEY'
    # Add the CREATED_DATE and the offset fields. Then, make a field for and calculate the concatAccidentKey.
    fieldsToJoinFromFCToFC = ['ON_ROAD_KDOT_NAME', 'AT_ROAD_KDOT_DIRECTION', 'AT_ROAD_KDOT_DIST_FEET', 'AT_ROAD_KDOT_NAME', 'CREATED_DATE']
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
    
    print("Joining the fields " + str(fieldsToJoinFromFCToFC) + " to the " + str(inMemCrashPoints) + " feature class.")
    JoinField_management(inMemCrashPoints, inMemCrashPointsKeyField, joinTable, joinTableKeyField, fieldsToJoinFromFCToFC)
    
    FCDescription = Describe(inMemCrashPoints)
    FieldObjectsList = FCDescription.fields
    FieldNamesList = [x.name for x in FieldObjectsList]
    if accUniqueKeyFieldName not in FieldNamesList:
        AddField_management(inMemCrashPoints, accUniqueKeyFieldName, accUniqueKeyFieldType, "", "", accUniqueKeyFieldLength,
            accUniqueKeyFieldName, accUniqueKeyFieldNullsAllowed)
        
        # Convert the CREATED_DATE to a string of numbers.
        ConvertTimeField_management(inMemCrashPoints, joinedTimeFieldName, inputTimeFormatString, convertedTimeFieldName, outputTimeFieldType, outputTimeFormatString)
        # Calculate the unique key from the accident key and converted time field.
        expressionText = "!" + str(inMemCrashPointsKeyField) + "! + !" + str(convertedTimeFieldName) + "!"
        CalculateField_management(inMemCrashPoints, accUniqueKeyFieldName, expressionText, "PYTHON_9.3")
        # After you calculate the concatenated unique key, delete the converted time field.
        #try:
        #    DeleteField_management(inMemCrashPoints, convertedTimeFieldName)
        #except:
        #    print("Could not delete the converted time field: " + str(convertedTimeFieldName) + ".")
        
    else:
        print("Great news! The " + str(accUniqueKeyFieldName) + " field already exists.")
        print("It does not need to be added and have its values calculated.")
    
    # After you calculate the concatenated unique key, delete the converted time field.
    print("Deleting the field " + str(convertedTimeFieldName) + " from the FC: " + str(inMemCrashPoints) + ".")
    DeleteField_management(inMemCrashPoints, convertedTimeFieldName)
    
    inputKeySearchFields = [accUniqueKeyFieldName]
    inputKeySearchQuery = """ """ + str(accUniqueKeyFieldName) + """ IS NOT NULL """
    newCursor = daSearchCursor(inMemCrashPoints, inputKeySearchFields, inputKeySearchQuery)
    
    crashPointsSelectionList = []
    for cursorItem in newCursor:
        crashPointsSelectionList.append(str(cursorItem[0]))
    
    try:
        del newCursor
    except:
        pass
    
    
    if (Exists(offsetPointsOutputLocation)):
        print("Removing unique keys from the selection list if they already exist in the output.")
        
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
    
    # Select only the reduced number of features in the feature class
    # Build a dynamic selectionQuery and reselect the features in the feature class
    createDynamicAttributeSelection(inMemCrashPointsLayer, uniqueKeyListReduced, accUniqueKeyFieldName, accUniqueKeyFieldType)
    
    print("Copying the selected features from the larger inMemory feature class to a smaller subset feature class.")
    CopyFeatures_management(inMemCrashPointsLayer, inMemCrashPointsSubset)
    
    Delete_management(inMemCrashPointsLayer)
    Delete_management(inMemCrashPoints)
    
    crashPointsDescription = Describe(stagedCrashPoints)
    crashPointsSR = crashPointsDescription.spatialReference
    del crashPointsDescription
    
    print("Starting the continuous offset caller.")
    continuousoffsetcaller(inMemCrashPointsSubset, inMemNG911Roadways, inMemAliasTable, offsetPointsTempOutputLocation, useKDOTIntersect,
        crashPointsSR, roadwayTableGCID, aliasTableGCID, aliasTableNameFields, additionalNameColumnBaseName, concatAccidentKey) 
    
    createOutputLocationWithSR(offsetPointsOutputLocation, offsetProjectionToUse, offsetPointsTempOutputLocation)
    moveCrashesFromTempToOutput(offsetPointsTempOutputLocation, offsetPointsOutputLocation)
    calculateOutputLocationLongAndLat(offsetPointsOutputLocation, longitudeColumnName, latitudeColumnName)


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


def moveCrashesFromTempToOutput(offsetTempPoints, offsetPoints):
    # This needs to open a blank MXD, then set the dataframe to the offsetOutput projection
    # and load the offsetTempPoints.
    # Using the automatic reprojection capabilities of the data frame,
    # it needs to export the offsetTempPoints to an in_memory/offsetTempPointsReproj
    # then cursor the data from the in_memory/offsetTempPointsReproj to the
    # offsetPoints since they should be in the same projection then.
    offsetTempFields = ListFields(offsetTempPoints)
    offsetTempFieldNames = [x.name for x in offsetTempFields]
    offsetOutputFields = ListFields(offsetPoints)
    offsetOutputFieldNames = [y.name for y in offsetOutputFields]
    transferFieldNames = [z for z in offsetTempFieldNames if z in offsetOutputFieldNames]
    
    transferList = list()
    
    newCursor = daSearchCursor(offsetTempPoints, transferFieldNames)
    
    for cursorItem in newCursor:
        transferList.append(cursorItem)
    
    try:
        del newCursor
    except:
        pass
    
    newCursor = daInsertCursor(offsetPoints, transferFieldNames)
    
    for transferItem in transferList:
        newCursor.Insert(transferList)
    
    try:
        del newCursor
    except:
        pass


# Call this after everything else to add the lat/long that Terri needs 
# to insert the data back into KCARS.
## For best results, open the temp feature class in a data frame and set the data frame
## to the output prj, then export the temp feature class to a reprojected temp feature class
## and cursor the output from that reprojected feature class into the prod feature class
## prior to calling this function.
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
    if attributeColumnType.lower() in ('text', 'date', 'guid'): # Use quotes around each value
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
                print("featureClassSelectionClause " + str(featureClassSelectionClause))
                SelectLayerByAttribute_management(featureClassToSelect, "ADD_TO_SELECTION", featureClassSelectionClause)
                
                selectionCounter = 0
                featureClassSelectionClause = """ """ + str(attributeColumnName) + """ IN ("""
                featureClassSelectionClause = featureClassSelectionClause + """'""" + str(attributeValueItem) + """', """
            
        if selectionCounter > 0:
            # Remove the trailing ", " and add a closing parenthesis.
            #Debug only
            print("featureClassSelectionClause " + str(featureClassSelectionClause))
            featureClassSelectionClause = featureClassSelectionClause[:-2] + """) """
            SelectLayerByAttribute_management(featureClassToSelect, "ADD_TO_SELECTION", featureClassSelectionClause)
        else:
            pass
    else: # Don't use quotes around each value
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
                print("featureClassSelectionClause " + str(featureClassSelectionClause))
                SelectLayerByAttribute_management(featureClassToSelect, "ADD_TO_SELECTION", featureClassSelectionClause)
                
                selectionCounter = 0
                featureClassSelectionClause = """ """ + str(attributeColumnName) + """ IN ("""
                featureClassSelectionClause = featureClassSelectionClause + str(attributeValueItem) + """, """
            
        if selectionCounter > 0:
            # Remove the trailing ", " and add a closing parenthesis.
            #Debug only
            print("featureClassSelectionClause " + str(featureClassSelectionClause))
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


if __name__ == "__main__":
    main()
else:
    pass