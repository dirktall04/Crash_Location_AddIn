#!/usr/bin/env/ python
#continousProcessOffsetScript.py
# Created by dirktall04 on 2017-07-26
# Large portions of this code were adapted from:
# AccidentDirectionMatrixOffsetCode.py
# Updated by dirktall04 on 2017-09-27
# Updated by dirktall04 on 2017-10-24

# Rebuild the accident offset script to use a fixed source of Roadway data (NG911).
# 2017-09-27 update removed several in_memory\* feature classes in an attempt
# to reduce memory consumption as the script seems to be running into
# problems due to a lack of available memory.


import gc
import os
import random
import re
import datetime
import traceback
from arcpy import (AddField_management, CalculateField_management, ConvertTimeField_management,
    CopyFeatures_management, CopyRows_management, CreateFeatureclass_management, Delete_management,
    DeleteField_management, Describe, env, Exists, GetCount_management, ListFeatureClasses, ListFields,
    JoinField_management, MakeFeatureLayer_management, Point, PointGeometry, Geometry,
    SelectLayerByAttribute_management, SelectLayerByLocation_management, Sort_management,
    SpatialReference, TruncateTable_management)
from arcpy.da import (Editor, SearchCursor as daSearchCursor, InsertCursor as daInsertCursor, UpdateCursor as daUpdateCursor)
from arcpy.mapping import (AddLayer, MapDocument, ListDataFrames, ListLayers)
from crashOffsetSingleRow import (singlerowoffsetcaller, mainTest as accOffMainTest, CONST_ZERO_DISTANCE_OFFSET,
    CONST_NORMAL_OFFSET, CONST_NOT_OFFSET)
# Doesn't seem like the daily vs overall property will need to be implemented.
##from lastRun import dailyOrOverall

from pathFunctions import (returnGDBOrSDEPath, returnFeatureClass, returnGDBOrSDEName)
try:
    from flattenTableJoinOntoTable import tableOntoTableCaller, GenerateFlatTableColumnNames
except:
    print ("Could not import the tableOntoTableCaller or GenerateFlatTableColumnNames function from flattenTableJoinOntoTable.py.")

# I've phased out most of the feature classes and tables that were written to in_memory.
# The script seemed to be having trouble with all the space that they were taking up.
# Also added some calls to gc.collect() in this script and
# the AccidentDirectionMatrixOffsetCode script.
env.overwriteOutput = True
newSelection = "NEW_SELECTION"
maxCrashPointsToProcess = 10479 # First 4 for AR63 are 10479, the last 1 is 10476.


roadwayTableGCID = "NGKSSEGID"
aliasTableGCID = "NGKSSEGID"
aliasTableNameFields = ["A_RD", "LABEL"]
additionalRdNameColumnBaseName = "Alias_Road_Name_"
# Since I'm not super confident in the CHASM connection, there will 
# need to be a  location that I use for staging data that comes across.
# In fact I may need to pull data across in chunks so that it doesn't time out.
# Once all the data is across, it will be compared to the previous dataset.
# If the number of features is at least 75% of the previous number, the
# features will be used in the process. If not, they'll be ignored and
# the previous features will be used. There should be a max
# time limit on this, however it is not yet implemented.

gdbForStaging = r'D:\SCHED\OffsetCrashes\Group1\crashOffsetStaging.gdb'
inProcessPRJ = r'D:\SCHED\OffsetCrashes\Group1\NAD 1983 StatePlane Kansas North FIPS 1501 (US Feet).prj'
inProcessProjectionToUse = SpatialReference(inProcessPRJ)
env.outputCoordinateSystem = inProcessProjectionToUse
offsetOutputPRJ = r'D:\SCHED\OffsetCrashes\Group1\NAD 1983 Decimal Degrees.prj'
offsetOutputProjectionToUse = SpatialReference(offsetOutputPRJ)

chasmNG911Roadways = r'D:\CHASM\CHASM.sde\KDOT.ROADCENTERLINE_NG911'
pulledNG911Roadways = r'D:\SCHED\OffsetCrashes\Group1\crashOffsetStaging.gdb\pu_NG911Roadways'
stagedNG911Roadways = r'D:\SCHED\OffsetCrashes\Group1\crashOffsetStaging.gdb\sg_NG911Roadways'
projectedNG911Roadways = r'D:\SCHED\OffsetCrashes\Group1\crashOffsetStaging.gdb\proj_NG911Roadways'
##inMemNG911Roadways = r"in_memory\NG911Roadways"
##ng911RoadwaysLayer = 'ng911RoadwaysAsALayer' # Might not be needed.
##inMemRoadwaysSubset = r'in_memory\NG911RoadwaysSubset'

chasmAliasTable = r'D:\CHASM\CHASM.sde\KDOT.ROADALIAS_NG911'
pulledAliasTable = r'D:\SCHED\OffsetCrashes\Group1\crashOffsetStaging.gdb\pu_AliasTable'
stagedAliasTable = r'D:\SCHED\OffsetCrashes\Group1\crashOffsetStaging.gdb\sg_AliasTable'
##inMemAliasTable = r'in_memory\AliasTable'

# Since the logic for the pulled & staged tables doesn't apply to the Daily
# geocode, the only crashes that will be pulled & staged will be from the Overall.
# However, I will need to include the data from the original crash record
# since the number of columns in the output is greatly reduced from the
# original crash record source.
chasmOutputCrashPoints = r'D:\CHASM\CHASM.sde\KDOT.KGS_CRASH_OUTPUT'
pulledCrashPoints = r'D:\SCHED\OffsetCrashes\Group1\crashOffsetStaging.gdb\pu_CrashPoints'
stagedCrashPoints = r'D:\SCHED\OffsetCrashes\Group1\crashOffsetStaging.gdb\sg_CrashPoints'
projectedCrashPoints = r'D:\SCHED\OffsetCrashes\Group1\crashOffsetStaging.gdb\proj_CrashPoints'
projectedCrashPoints2016 = r'D:\SCHED\OffsetCrashes\Group1\crashOffsetStaging.gdb\proj_CrashPoints2016_1'
projectedCrashPointsTestingSet = r'D:\SCHED\OffsetCrashes\Group1\crashOffsetStaging.gdb\proj_CrashPoints100'
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
allKDOTCrashRecords = r'D:\SCHED\OffsetCrashes\Group1\readonly@kcarsprd.sde\KCARS.GIS_GEOCODE_ACC_V' # Local accident data source.
pulledCrashRecords = r'D:\SCHED\OffsetCrashes\Group1\crashOffsetStaging.gdb\pu_CrashRecords'
stagedCrashRecords = r'D:\SCHED\OffsetCrashes\Group1\crashOffsetStaging.gdb\sg_CrashRecords'
###inMemCrashRecords = r'in_memory\CrashRecords'
# sortedInMemCrashRecords fails now when used in anything with the value r"in_memory\SortedCrashRecords"
# changed it, but it still failed, not when ran as a script, but when ran in ArcMap.
# So, I changed it again, this time to something that exists in an FGDB on disk rather than being something
# which exists in the in_memory database. So many limitations on using that construct. :-(
sortedInMemCrashRecords = r'D:\SCHED\OffsetCrashes\Group1\crashOffsetSorted.gdb\CrashRecordsSorted'
concatAccidentKey = 'ACCIDENT_UNIQUE_KEY'

testingTempOutputLocation = r'D:\SCHED\OffsetCrashes\Group1\crashOffsetTemp.gdb\Temp_OffsetOutput'
testingOutputLocation = r'D:\SCHED\OffsetCrashes\Group1\crashOffsetOutput.gdb\OffsetOutput'
productionTempOutputLocation = r'D:\SCHED\OffsetCrashes\Group1\geo@CrashLocation.sde\CrashLocation.GEO.TEMP_ACC_OFFSET_PTS_ALL'
productionOutputLocation = r'D:\SCHED\OffsetCrashes\Group1\geo@CrashLocation.sde\CrashLocation.GEO.ACC_OFFSET_PTS_ALL'
offsetPointsTempOutputLocation = testingTempOutputLocation
offsetPointsOutputLocation = testingOutputLocation
####offsetPointsOutputLocation_NK = testingOutputLocation + "_NK"
#offsetTempPointsTestLayerFile = r'D:\SCHED\OffsetCrashes\Temp_OffsetOutputs_Test.lyr'
#offsetTempPointsProdLayerFile = r'D:\SCHED\OffsetCrashes\Temp_OffsetOutputs_Prod.lyr'
#offsetTempPointsLayerFile = offsetTempPointsTestLayerFile

longitudeColumnName = 'ddLongitude'
latitudeColumnName = 'ddLatitude'
longLatColumnType = 'DOUBLE'

isOffsetFieldName = 'isOffset'
offsetDateFieldName = 'OFFSET_DATE'
offsetDateColumnType = 'DATE'

#######################################
# --- From crash offset functions --- #
#######################################
# Scratch data locations
intermediateAccidentBuffer = r'in_memory\intermediateAccidentBuffer'
intermediateAccidentIntersect = r'in_memory\intermediateAccidentIntersect'
intermediateAccidentIntersectSinglePart = r'in_memory\intermediateAccidentIntersectSinglePart'

roadsAsFeatureLayer = 'NonStateRoadsFeatureLayer'

#Set the unique key field for use when removing duplicates.
currentUniqueKeyField = 'ACCIDENT_KEY' # Default
flattenedRoadwayTableForReference = r'in_memory\flattenedRoadwayTable'
flattenedRoadwayTableLayer = 'flattenedRoadwayTableLayer'
testFlattenedRoadwayTableOutput = r'D:\SCHED\OffsetCrashes\Group1\crashOffsetTemp.gdb\flattenedRoadwayTable'

# Testing
crashPointsTestOutput = r'D:\SCHED\OffsetCrashes\Group1\crashOffsetTemp.gdb\inMemCrashPointsTestOutput'
crashPointsSubsetTestOutput = r'D:\SCHED\OffsetCrashes\Group1\crashOffsetTemp.gdb\inMemCrashPointsSubsetTestOutput'

# For faster testing, set this to False. If the road centerlines have been updated, set it to True.
reapplyRoadAliasFlattening = False
KDOTRoadNameCalcField = "KDOT_Alias_Name"
# For faster testing, set this to False. If the road centerlines have been updated, set it to True.
addAndCalculateTheKDOTAlias = True
# Set parseMatchAddresses to False normally. If the match rate with it False is too low, experiment with it
# being set to True. This will result in the MatchAddr field being parsed to get road names for selection in
# the intersection between the Roads Layer and the Crash Offset Buffer Layer.
parseMatchAddresses = False
# Debug breakpoint
stopAfterKDOTRouteNameUpdate = False


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
class blankCrashObject:
    def __init__(self):
        self.testArgWithALongName = True


def createTempOutputLocation(tempOutputLocation, offsetColumnName, offsetDateColumnName, templateFeatureClass, spatialRefToUse):
    # Should be geocoding to an in_memory location and then just
    # moving the features that were either not relocated due to
    # short offset distance or were offset correctly.
    
    if Exists(tempOutputLocation):
        try:
            Delete_management(tempOutputLocation)
        except:
            pass
    else:
        pass
    
    fcOutPath = returnGDBOrSDEPath(tempOutputLocation)
    fcOutName = returnFeatureClass(tempOutputLocation)
    # Create feature class using the temp points feature class as a template.
    CreateFeatureclass_management(fcOutPath, fcOutName, "POINT", templateFeatureClass, "DISABLED", "DISABLED", spatialRefToUse)
    print("The tempOutputLocation FC was created at" + str(tempOutputLocation) + ".")
    
    offsetColumnLength = 50 
    
    # Create a list of fields using the ListFields function
    offsetDescriptionObject = Describe(tempOutputLocation)
    offsetFieldsList = offsetDescriptionObject.fields
    
    offsetFieldNamesList = [x.name for x in offsetFieldsList]
    
    # If the isOffset field is not found,
    # add it with the necessary parameters.
    if str(offsetColumnName) not in offsetFieldNamesList:
        # print("Adding isOffset to " + centerlineToIntersect + ".")
        AddField_management(tempOutputLocation, offsetColumnName, "TEXT", "", "", offsetColumnLength)
        print("The " + str(offsetColumnName) + " field was added to " + str(tempOutputLocation) + ".")
    else:
        pass
    
    if str(offsetDateColumnName) not in offsetFieldNamesList:
        # print("Adding isOffset to " + centerlineToIntersect + ".")
        AddField_management(tempOutputLocation, offsetDateColumnName, offsetDateColumnType)
        print("The " + str(offsetDateColumnName) + " field was added to " + str(tempOutputLocation) + ".")
    else:
        pass
    
    ###env.workspace = previousWorkspace


def moveCrashesFromTempToOutput(offsetTempPoints, offsetPoints, longColumn, latColumn, offsetDateColumn):
    if Exists(offsetPoints):
        offsetPointsOutputLocationFields = ListFields(offsetPoints)
        offsetPointsOutputLocationFieldNames = [x.name for x in offsetPointsOutputLocationFields]
        # Check to see if the fields exist and add them if not.
        if longColumn in offsetPointsOutputLocationFieldNames:
            print("longitudeColumn found. Will not re-add it.")
        else:
            AddField_management(offsetPoints, longColumn, longLatColumnType)
            print("Added the longitudeColumn to the offsetPoints FC with a type of " + str(longLatColumnType) + ".")
        
        if latColumn in offsetPointsOutputLocationFieldNames:
            print("latitudeColumn found. Will not re-add it.")
        else:
            AddField_management(offsetPoints, latColumn, longLatColumnType)
            print("Added the latitudeColumn to the offsetPoints FC with a type of " + str(longLatColumnType) + ".")
        
        if offsetDateColumn in offsetPointsOutputLocationFieldNames:
            print("offsetDateFieldName found. Will not re-add it.")
        else:
            AddField_management(offsetPoints, offsetDateColumn, offsetDateColumnType)
            print("Added the offsetDateFieldName to the offsetPoints FC with a type of DATE.")
        
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
    
    newCursor = daSearchCursor(offsetTempPoints, transferFieldNames, searchCursorWhereClause, offsetOutputProjectionToUse)
    
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
    
    updateCursorClause = """ """ + str(isOffsetField) + """ IS NOT NULL """
    #searchCursorClause = """ 1 = 1 """
    # The X & Y Shape tokens should already be in the correct projection, so there is no need to
    # further specify the output spatial reference.
    newCursor = daUpdateCursor(pointLayerToUpdate, testingUpdateFields, updateCursorClause)
    
    for cursorItem in newCursor:
        cursorListItem = list(cursorItem)
        # Set the text fields to the string version of the Shape@X and Shape@Y tokens.
        cursorListItem[3] = cursorListItem[1]
        cursorListItem[4] = cursorListItem[2]
        #print("The transferItem has an isOffsetField of: " + str(cursorListItem[0]) + ".")
        #print("...and " + str(longitudeField) + "\" + str(latitudeField) + " of: " + str(cursorListItem[3]) + "\" + str(cursorListItem[4]) + ".\n")
        newCursor.updateRow(cursorListItem)
    
    try:
        del newCursor
    except:
        pass


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


def recreateProjectedFCWithNewProjectedOutput(firstFC, secondFC, prjToProjectInto):
    try:
        Delete_management(secondFC)
    except:
        print("Could not delete the FC at: " + str(secondFC) + ".")
    try:
        Project(firstFC, secondFC, prjToProjectInto)
    except:
        print("Could not reproject the features from " + str(firstFC) + " to " + str(secondFC) + ".")
        print(traceback.format_exc())


def copyFeaturesAndCreateFeatureLayer(fcFirstLocation, fcSecondLocation, fcFeatureLayerName):
    CopyFeatures_management(fcFirstLocation, fcSecondLocation)
    MakeFeatureLayer_management(fcSecondLocation, fcFeatureLayerName)


def continuousiteratorprocessbysinglerow():
    print("Starting the continuous iterator process.")
    # In order to quickly work with the features, they need to be moved to in_memory.
    # In order to select features, they need to be feature layers.
    currentDateTime = datetime.datetime.now()
    formattedDateTime = currentDateTime.strftime("%m/%d/%Y %I:%M:%S")
    
    useKDOTIntersect = True
    
    #KDOTXYFieldList = ['OBJECTID', 'STATUS', 'POINT_X', 'POINT_Y', 'ACCIDENT_KEY', 'ON_ROAD_KDOT_NAME',
    #                                           'AT_ROAD_KDOT_DIRECTION', 'AT_ROAD_KDOT_DIST_FEET', 'AT_ROAD_KDOT_NAME',
    #                                           'Match_addr']
    
    copyFeaturesAndCreateFeatureLayer(projectedCrashPoints2016, inMemCrashPoints, crashPointsLayer)
    print("Crash points layer created.")
    
    sortFieldsAndOrders = [['CREATED_DATE', 'DESCENDING']]
    Sort_management(stagedCrashRecords, sortedInMemCrashRecords, sortFieldsAndOrders)
    
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
    ####  Modified due to bad result when using capital Y/D. Help is wrong. See below.
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
    # They should already be in there, except for perhaps CREATED_DATE, but just in case they are not.
    if "ON_ROAD_KDOT_NAME" not in joinFieldsDictKeys:
        joinFieldsDict["ON_ROAD_KDOT_NAME"] = ["TEXT", "", "", 50, "ON_ROAD_KDOT_NAME", "NULLABLE", "", ""]
    if "AT_ROAD_KDOT_DIRECTION" not in joinFieldsDictKeys:
        joinFieldsDict["AT_ROAD_KDOT_DIRECTION"] = ["TEXT", "", "", 2, "AT_ROAD_KDOT_DIRECTION", "NULLABLE", "", ""]
    if "AT_ROAD_KDOT_DIST_FEET" not in joinFieldsDictKeys:
        joinFieldsDict["AT_ROAD_KDOT_DIST_FEET"] = ["DOUBLE", "", "", "", "AT_ROAD_KDOT_DIST_FEET", "NULLABLE", "", ""]
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
        time.sleep(3)
        # Add an update cursor to delete rows that have a null convertedTimeFieldName value.
        rowsToDeleteWhereClause = """ """ + str(convertedTimeFieldName) + """ IS NULL """
        fieldsToShowInCursor = [convertedTimeFieldName]
        newCursor = daUpdateCursor(inMemCrashPoints, fieldsToShowInCursor, rowsToDeleteWhereClause)
        
        for cursorRow in newCursor:
            newCursor.deleteRow()
            
        try:
            del newCursor
        except:
            pass
        
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
    crashPointsSelectionListLength = len(crashPointsSelectionList)
    
    if crashPointsSelectionListLength < maxCrashPointsToProcess:
        maxCrashPointsToProcess = crashPointsSelectionListLength
    else:
        pass
    
    uniqueKeyListReduced = random.sample(crashPointsSelectionList, maxCrashPointsToProcess)
    
    #### Commented out for testing.
    # Select only the reduced number of features in the feature class
    # Build a dynamic selectionQuery and reselect the features in the feature class
    createDynamicAttributeSelection(crashPointsLayer, uniqueKeyListReduced, accUniqueKeyFieldName, accUniqueKeyFieldType)
    
    print("Copying the selected features from the larger inMemory feature class to a smaller subset feature class.")
    CopyFeatures_management(crashPointsLayer, inMemCrashPointsSubset)
    
    try:
        Delete_management(crashPointsLayer)
    except:
        print("Could not delete the FC at: " + str(crashPointsLayer) + ".")
    
    ### Need to convert the crash points and the roadways to a common projection here.
    ### To prevent any projection mismatch errors when trying to do intersects and/or
    ### when trying to insert the results of an intersection into a new feature class.
    ### There, all of the features that are being intersected as well as the
    ### feature class being inserted into should all share a projection.
    
    
    
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
    ###raise ValueError("Check to see if the fields have been correctly added and updated.") # End of Test
    
    try:
        Delete_management(inMemCrashPoints)
    except:
        print("Could not delete the FC at: " + str(crashPointsLayer) + ".")
    
    print("The inProcessProjectionToUse is: " + str(inProcessProjectionToUse) + ".")
    gc.collect()
    print("The call to gc.collect() has completed.")
    
    print("Starting the continuous offset caller.")
    
    roadNameColumns = ["RD", "LABEL", "STP_RD"]
    
    if reapplyRoadAliasFlattening == True:
        flattenedRoadwayTable = flattenedRoadwayTableForReference
        tableOntoTableCaller(projectedNG911Roadways, roadwayTableGCID, stagedAliasTable,
            aliasTableGCID, aliasTableNameFields, flattenedRoadwayTable, additionalRdNameColumnBaseName)
        
        CopyFeatures_management(flattenedRoadwayTable, testFlattenedRoadwayTableOutput)        
        
    else:
        flattenedRoadwayTable = testFlattenedRoadwayTableOutput
    
    additionalColumnCount = countTheNumberOfAliasFieldNames(flattenedRoadwayTable, additionalRdNameColumnBaseName)
    roadAliasNameColumns = []
    
    roadNameColumns = ["RD", "LABEL", "STP_RD"] # The flattened alias column names get appended to these.
    if additionalColumnCount is not None and additionalColumnCount >= 1:
        roadAliasNameColumns = GenerateFlatTableColumnNames(additionalRdNameColumnBaseName, additionalColumnCount)
        if len(roadAliasNameColumns) > 0:
            roadNameColumns += roadAliasNameColumns
        else:
            pass
    else:
        pass
    
    # Try to get the accuracy in calculating the KDOT format alias up. Some of these have aliases that are
    # US HWY 56, US56, HWY 56, etc, but not U056, which is what is needed.
    roadNameColumnsToCalcKDOTAlias = roadNameColumns
    
    if addAndCalculateTheKDOTAlias == True:
        # Check to see whether or not the column exists in the flattenedRoadWayTable
        # If not, then add it.
        # If so, then go onto the next step.
        # Calculate the fields based on passing the RD field, or
        # if null, and the Label Field is not null, then use the
        # Label field instead.
        # Use the 
        addKDOTAliasNameField(flattenedRoadwayTable, KDOTRoadNameCalcField)
        KDOTRouteNameUpdate(flattenedRoadwayTable, KDOTRoadNameCalcField, roadNameColumnsToCalcKDOTAlias)
    else:
        pass
    
    # Check to see if the KDOTRoadNameCalcField exists in the flattenedRoadwayTable.
    # If so, add it to the list of roadNameColumns
    flattenedRoadwayTableFieldObjects = ListFields(flattenedRoadwayTable)
    flattenedRoadwayTableFieldNames = [x.name for x in flattenedRoadwayTableFieldObjects]
    if KDOTRoadNameCalcField in flattenedRoadwayTableFieldNames:
        print("Found the " + str(KDOTRoadNameCalcField) + " field in the flattenedRoadwayTableFieldNames.")
        if KDOTRoadNameCalcField not in roadNameColumns:
            print("Adding the " + str(KDOTRoadNameCalcField) + " field to the flattenedRoadwayTableFieldNames.")
            roadNameColumns.append(KDOTRoadNameCalcField)
        else:
            print("The " + str(KDOTRoadNameCalcField) + " was already in the roadNameColumns, so it will not be added again.")
    else:
        print("Did not find the " + str(KDOTRoadNameCalcField) + " field in the flattenedRoadwayTableFieldNames: " + str(flattenedRoadwayTableFieldNames) + ".")
        print("It will not be added to the roadNameColumns: " + str(roadNameColumns) + ".")
    
    ##Debug output
    print("The additionalColumnCount is : " + str(additionalColumnCount) + ".")
    if len(roadAliasNameColumns) > 0:
        print("The full set of roadAliasNameColumns = " + str(roadAliasNameColumns) + ".")
    else:
        pass
    print("The full set of roadNameColumns = " + str(roadNameColumns) + ".")
    
    if stopAfterKDOTRouteNameUpdate == True:
        raise IOError("Check the " + str(KDOTRoadNameCalcField) + " values and see if they populated correctly.")
    else:
        pass
    
    
    # Turn the result into a feature layer so that it can be used in selections.
    MakeFeatureLayer_management(flattenedRoadwayTable, flattenedRoadwayTableLayer)
    
    unformattedCrashPointsList = list()
    
    crashPointsSubsetDescription = Describe(inMemCrashPointsSubset)
    crashPointsSubsetFields = crashPointsSubsetDescription.fields
    crashPointsSubsetOIDFieldName = crashPointsSubsetDescription.OIDFieldName
    crashPointsSubsetShapeFieldName = crashPointsSubsetDescription.shapeFieldName
    excludedOIDAndShapeFieldNames = [crashPointsSubsetOIDFieldName, crashPointsSubsetShapeFieldName]
    allCrashPointsSubsetFieldNames = [x.name for x in crashPointsSubsetFields]
    orderedFieldNamesForOffsetProcessing = ['SHAPE@', 'SHAPE@XY', 'ON_ROAD_KDOT_NAME', 'AT_ROAD_KDOT_DIRECTION',
                                            'AT_ROAD_KDOT_DIST_FEET', 'MATCH_ADDR', concatAccidentKey]
    notAlreadyUsedCrashPointsSubsetFieldNames = [y for y in allCrashPointsSubsetFieldNames if (y not in orderedFieldNamesForOffsetProcessing and y not in excludedOIDAndShapeFieldNames)]
    # The complete list of fields for each row which is to be passed through the entire process.
    searchCursorFieldNamesForCrashPointsSubset = orderedFieldNamesForOffsetProcessing + notAlreadyUsedCrashPointsSubsetFieldNames
    
    print("The value for the excludedOIDAndShapeFieldNames is : " + str(excludedOIDAndShapeFieldNames) + ".")
    print("The OID Field is: " + str(crashPointsSubsetOIDFieldName) + " and the Shape Field is: " + str(crashPointsSubsetShapeFieldName) + ".")
    print("The full set of searchCursorFieldNamesForCrashPointsSubset are: " + str(searchCursorFieldNamesForCrashPointsSubset) + ".")
    
    newCursor = daSearchCursor(inMemCrashPointsSubset, searchCursorFieldNamesForCrashPointsSubset)
    for cursorRow in newCursor:
        unformattedCrashPointsList.append(cursorRow)
    
    try:
        del newCursor
    except:
        pass
    
    readyToOffsetCrashObjectsList = list()
    
    for unformattedCrashItem in unformattedCrashPointsList:
        try:
            del currentCrashObject
        except:
            pass
        
        #Initialization
        currentCrashObject = blankCrashObject()
        currentCrashObject.unformattedCrashItemList = list(unformattedCrashItem)
        currentCrashObject.initialShape = unformattedCrashItem[0]
        #currentCrashObject.initialShapeX = unformattedCrashItem[1]
        #currentCrashObject.initialShapeY = unformattedCrashItem[2]
        currentCrashObject.initialShapeXY = unformattedCrashItem[1]
        currentCrashObject.onRoad = unformattedCrashItem[2]
        currentCrashObject.offsetDirection = unformattedCrashItem[3]
        currentCrashObject.offsetDistance = unformattedCrashItem[4]
        currentCrashObject.matchAddress = unformattedCrashItem[5]
        currentCrashObject.uniqueKey = unformattedCrashItem[6]
        currentCrashObject.roadNameColumns = roadNameColumns
        currentCrashObject.offsetShapeX = None
        currentCrashObject.offsetShapeY = None
        currentCrashObject.offsetShape = None
        currentCrashObject.offsetShapeXY = None
        currentCrashObject.singlePartOffsetFeaturesList = None
        
        readyToOffsetCrashObjectsList.append(currentCrashObject)
    
    formattedOffsetCrashItemsList = list()
    
    counterValue = 0
    for readyToOffsetCrashObject in readyToOffsetCrashObjectsList:
        returnedCrashOffsetObject = singlerowoffsetcaller(readyToOffsetCrashObject, flattenedRoadwayTableLayer, counterValue, inProcessProjectionToUse, parseMatchAddresses)
        counterValue += 1
        # May need to format the data a bit here before appending it.
        if returnedCrashOffsetObject is not None:
            #Could also check to see if the returnedCrashOffsetObject.isOffset == CONST_NORMAL_OFFSET
            if returnedCrashOffsetObject.offsetShape is not None:
                '''
                try:
                    del tempPoint
                except:
                    pass
                try:
                    del tempPointGeometry
                except:
                    pass
                '''
                try:
                    del tempCrashItemList
                except:
                    pass
                
                print("The returnedCrashOffsetObject.offsetShape is not None.")
                print("The values are: " + str(returnedCrashOffsetObject.offsetShapeXY[0]) + " & " + str(returnedCrashOffsetObject.offsetShapeXY[1]) + ".")
                # Parse this so that it's ready to be inserted into the output table.
                ##formattedCrashItemList = returnedCrashOffsetObject.unformattedCrashItemList
                ##tempPoint = Point(returnedCrashOffsetObject.offsetShapeX, returnedCrashOffsetObject.offsetShapeY)
                ##tempPointGeometry = PointGeometry(tempPoint, inProcessProjectionToUse)
                #print("The tempPointGeometry's firstPoint's X & Y values are: " + str(tempPointGeometry.firstPoint.X) + " & " + str(tempPointGeometry.firstPoint.Y) + ".")
                tempCrashItemList = []
                tempCrashItemList.append(returnedCrashOffsetObject.offsetShape)
                partialCrashItemList = returnedCrashOffsetObject.unformattedCrashItemList[2:]  # Do Not Keep initial SHAPE@ or SHAPE@XY
                formattedCrashItemList = tempCrashItemList + partialCrashItemList
                formattedCrashItemList.append(returnedCrashOffsetObject.isOffset)
                #del formattedCrashItemList[2] # Remove initial SHAPE@Y value
                #del formattedCrashItemList[1] # Remove initial SHAPE@X value
                formattedOffsetCrashItemsList.append(formattedCrashItemList)
            elif returnedCrashOffsetObject.isOffset == CONST_ZERO_DISTANCE_OFFSET or returnedCrashOffsetObject.isOffset == CONST_NOT_OFFSET:
                formattedCrashItemList = returnedCrashOffsetObject.unformattedCrashItemList
                #del formattedCrashItemList[2] # Remove initial SHAPE@Y value
                #del formattedCrashItemList[1] # Remove initial SHAPE@X value
                del formattedCrashItemList[1] # Remove initial SHAPE@XY value, in the 2nd list location, but keep the initial shape, in the first location.
                formattedCrashItemList.append(returnedCrashOffsetObject.isOffset)
                formattedOffsetCrashItemsList.append(formattedCrashItemList)
            else:
                print("The offsetShape for the readyToOffsetRow with a uniqueKey of: " + str(readyToOffsetCrashObject.uniqueKey) + " was None.")
        else:
            print("The returned offset object was None.")
    
    # Re/create the temp points output location, using the inMemCrashPointsSubset as a template, then adding the isOffsetField:
    createTempOutputLocation(offsetPointsTempOutputLocation, isOffsetFieldName, offsetDateFieldName, inMemCrashPointsSubset, inProcessProjectionToUse)
    
    listOfAdditionalOffsetDataFieldNames = [isOffsetFieldName, offsetDateFieldName]
    tempOutputInsertFieldsWithExtraShapeFields = searchCursorFieldNamesForCrashPointsSubset + listOfAdditionalOffsetDataFieldNames
    shapeXYFieldNamesToRemove = ['SHAPE@X', 'SHAPE@Y', 'SHAPE@XY']
    tempOutputInsertFields = [x for x in tempOutputInsertFieldsWithExtraShapeFields if x not in shapeXYFieldNamesToRemove]
    # Create an insert cursor and insert the data into the output table.
    newCursor = daInsertCursor(offsetPointsTempOutputLocation, tempOutputInsertFields)
    
    print("The fields being used for the tempOutputInsertFields are: " + str(tempOutputInsertFields) + ".")
    for readyToInsertRow in formattedOffsetCrashItemsList:
        if readyToInsertRow is not None:
            readyToInsertRow.append(formattedDateTime)
            ##print("The length of the readyToInsertRow is: " + str(len(readyToInsertRow)) + ".")
            ##print("The length of the tempOutputInsertFields is: " + str(len(tempOutputInsertFields)) + ".")
            print("The x & y of the readyToInsertRow's geometry are: " + str(readyToInsertRow[0].firstPoint.X) + " " + str(readyToInsertRow[0].firstPoint.Y) + ".")
            print("The uniqueKey and offsetDistance are: " + str(readyToInsertRow[5]) + " and " + str(readyToInsertRow[3]) + ".")
            #print("The full value of the readyToInsertRow is: " +str(readyToInsertRow) + ".")
            # Insert this into the tempOutput location.
            newCursor.insertRow(readyToInsertRow)
        else:
            pass
    
    try:
        del newCursor
    except:
        pass
    # Modify this to send a single accident row each time along with the supporting configuration information.
    # Then, catch the row that is returned and make decisions about how to integrate it into the output
    # dataset from there.
    # For instance, if the data could not be offset, do you want to write that to the output dataset with
    # the understanding that it will be reran at some point if it receives an update after the offsetDate?
    # That decision will be a part of this script.
    # ^Decision is yes. ^^Modification is complete.
    
    createOutputLocationWithSR(offsetPointsOutputLocation, offsetOutputProjectionToUse, offsetPointsTempOutputLocation)
    moveCrashesFromTempToOutput(offsetPointsTempOutputLocation, offsetPointsOutputLocation, longitudeColumnName, latitudeColumnName, offsetDateFieldName)
    updateTheddLongAndddLatFields(offsetPointsOutputLocation, isOffsetFieldName, longitudeColumnName, latitudeColumnName)


def createOutputLocationWithSR(offsetPoints, offsetProjection, offsetTempPoints):
    if Exists(offsetPoints):
        pass
    else:
        fcOutPath = returnGDBOrSDEPath(offsetPoints)
        fcOutName = returnFeatureClass(offsetPoints)
        # Create feature class using the temp points feature class as a template.
        CreateFeatureclass_management(fcOutPath, fcOutName, "POINT", offsetTempPoints, "DISABLED", "DISABLED", offsetProjection)
        # Another function will add missing fields, if any.
        print("The offsetPoints FC was created at" + str(offsetPoints) + ".")


def countTheNumberOfAliasFieldNames(fcWithAliasFieldNames, aliasFieldNamesBase):
    fieldObjectsList = ListFields(fcWithAliasFieldNames)
    fieldNamesList = [x.name for x in fieldObjectsList]
    aliasFieldNamesCount = 0
    for fieldNameItem in fieldNamesList:
        if fieldNameItem.find(aliasFieldNamesBase) >= 0:
            aliasFieldNamesCount += 1
        else:
            pass
    
    return aliasFieldNamesCount


def addKDOTAliasNameField(centerlinesToUpdate, KDOTAliasName):
    fieldsList = ListFields(centerlinesToUpdate)
    
    fieldNamesList = list()
    
    # Iterate through the list of fields
    for field in fieldsList:
        fieldNamesList.append(field.name)
    
    # If the KDOT_ROUTENAME field is not found,
    # add it with adequate parameters.
    if KDOTAliasName not in fieldNamesList:
        print("Adding " + str(KDOTAliasName) + " to " + str(centerlinesToUpdate) + ".")
        previousWorkspace = env.workspace  # @UndefinedVariable
        addFieldWorkspace = returnGDBOrSDEPath(centerlinesToUpdate)
        env.workspace = addFieldWorkspace
        
        fieldLength = 10
        
        AddField_management(centerlinesToUpdate, KDOTAliasName, "TEXT", "", "", fieldLength)
        
        env.workspace = previousWorkspace
        print("The " + str(KDOTAliasName) + " field was added to " + str(centerlinesToUpdate) + ".")
    
    else:
        print("The " + str(KDOTAliasName) + " field already exists within " + str(centerlinesToUpdate) + ".")
        print("It will not be added again, but its values will be updated (where necessary).")


#Adapted from the Kdot_RouteNameCalc function in the NGfLRSMethod script.
def KDOTRouteNameUpdate(centerlinesToUpdate, KDOTAliasName, inputRDNameFieldsList):
    aliasNameFieldInAList = [KDOTAliasName]
    inputRDNameFieldsListUpdated = [x for x in inputRDNameFieldsList if x not in aliasNameFieldInAList]
    
    cursorFields = aliasNameFieldInAList + inputRDNameFieldsListUpdated
    
    # Matches an I, U, or K at the start of the string, ignoring case.
    IUKMatchString = re.compile(r'^[IUK]', re.IGNORECASE)
    
    # Matches 1 to 3 digits at the end of a string, probably no reason to ignore case in the check.
    singleToTripleNumberEndMatchString = re.compile(r'[0-9][0-9][0-9]|[0-9][0-9]|[0-9]')     
    
    cursorFieldsLen = len(cursorFields)
    newCursor = daUpdateCursor(centerlinesToUpdate, cursorFields)
    for cursorItem in newCursor:
        cursorListItem = list(cursorItem)
        inputRDNamesValueList = []
        outputRDNamesValueList = []
        firstPart = ""
        secondPart = ""
        fullRouteName = ""
        if cursorFieldsLen > 1:
            for rangeValue in xrange(0, (cursorFieldsLen - 1)):
                inputRDNamesValueList.append(cursorItem[rangeValue])
            
            for inputRDNamesValue in inputRDNamesValueList:
                if inputRDNamesValue is not None and inputRDNamesValue != "":
                    testResult0 = None
                    testResult1 = None
                    
                    ####################################################################################
                    
                    testResult0 = re.search(IUKMatchString, inputRDNamesValue)
                    testResult1 = re.search(singleToTripleNumberEndMatchString, inputRDNamesValue)    
                    
                    if testResult0 is not None and testResult1 is not None:
                        firstPart = str(testResult0.group(0))
                        secondPart = str(testResult1.group(0))
                        
                        # Pad the string with prepended zeroes if it is not 3 digits long already.
                        if len(secondPart) == 2:
                            secondPart = secondPart.zfill(3)
                            #print "secondPart = " + secondPart
                        elif len(secondPart) == 1:
                            secondPart = secondPart.zfill(3)
                            #print "secondPart = " + secondPart
                        else:
                            pass
                        
                        fullRouteName = firstPart + secondPart
                    else:
                        pass
                    
                    outputRDNamesValueList.append(fullRouteName)
                else:
                    pass
            
            for outputRDNameItem in outputRDNamesValueList:
                if outputRDNameItem is not None and outputRDNameItem != "":
                    if len(outputRDNameItem) >= 4:
                        print("Updating the " + str(KDOTAliasName) + " field with a value of: " + str(outputRDNameItem) + ".")
                        cursorListItem[0] = outputRDNameItem
                        newCursor.updateRow(cursorListItem)
                else:
                    pass
            
        else:
            pass
        
    try:
        del newCursor
    except:
        pass


### Don't need to do anything fancy with loaded layers, just open
### a SearchCursor and pass in the dd Projection as the spatial reference.
def calculateOutputLocationLongAndLat(offsetPoints, longitudeColumn, latitudeColumn):
    # This needs to create the x&y columns if the don't exist, then update them with correct Lat/Long in Decimal Degrees.
    if Exists(offsetPoints):
        offsetPointsOutputLocationFields = ListFields(offsetPoints)
        offsetPointsOutputLocationFieldNames = [x.name for x in offsetPointsOutputLocationFieldNames]
        # Check to see if the fields exist and add them if not.
        if longitudeColumn in offsetPointsOutputLocationFieldNames:
            pass
        else:
            AddField_management(offsetPoints, longitudeColumn, "DOUBLE", 0, 0)
        
        if latitudeColumn in offsetPointsOutputLocationFieldNames:
            pass
        else:
            AddField_management(offsetPoints, latitudeColumn, "DOUBLE", 0, 0)
        
        #Then
        longLatUpdateFields = [longitudeColumn, latitudeColumn, 'SHAPE@X', 'SHAPE@Y']
        cursorClause = """ """ + str(longitudeColumn) + """ IS NULL OR """ + str(latitudeColumn) + """ IS NULL """
        cursorSpatialReference = offsetOutputProjectionToUse
        newCursor = daUpdateCursor(offsetPoints, longLatUpdateFields, cursorClause, cursorSpatialReference)
        
        for cursorItem in newCursor:
            if cursorListItem[2] is not None and cursorListItem[3] is not None:
                cursorListItem = list(cursorItem)
                cursorListItem[0] = cursorListItem[2]
                cursorListItem[1] = cursorListItem[3]
                newCursor.Update(cursorListItem)
            else:
                print("Either the feature's SHAPE@X or it's SHAPE@Y was null. Will not update the latitude and longitude values for this feature.")
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


# This needs to call crashOffsetSingleRow.py and give it
# the necessary parameters for each feature class of intersected points
# that is in the crashLocation sql instance.

# TODO: Build a reporting script that gives me the information on the 
# number of points that were successfully geocoded per county and
# also as an aggregate.

# Should also give information on how many points have
# been successfully offset from those geocoded points.
def main():
    startingTime = datetime.datetime.now()
    print("Starting at " + str(startingTime) + ".")
    #pullDataFromChasm() # Skipping for now due to the long time that it takes.
    continuousiteratorprocessbysinglerow()
    #accOffMainTest()
    endingTime = datetime.datetime.now()
    elapsedTime = FindDuration(endingTime - startingTime)
    print("The script began at " + str(startingTime) + " and completed at " + str(endingTime) + ",")
    print("taking a total of: " + str(elapsedTime) + ".")


def FindDuration(endTime, startTime):
    #Takes two datetime.datetime objects, subtracting the 2nd from the first
    #to find the duration between the two.
    duration = endTime - startTime
    
    dSeconds = int(duration.seconds)
    durationp1 = str(int(dSeconds // 3600)).zfill(2)
    durationp2 = str(int((dSeconds % 3600) // 60)).zfill(2)
    durationp3 = str(int(dSeconds % 60)).zfill(2)
    durationString = durationp1 + ':' +  durationp2 + ':' + durationp3
    
    return durationString


if __name__ == "__main__":
    main()
else:
    pass