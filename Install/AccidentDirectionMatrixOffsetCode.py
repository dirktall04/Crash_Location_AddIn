#!/usr/bin/env python
# -*- coding: utf-8 -*-
# AccidentDirectionMatrixOffsetCode.py
# Author: DAT
# Created: 2015-02-02
# Updated: 2015-05-20
################################################################################################

#    TODO:DONE: Integrate progressor or other output messages that will give
#    the user an idea of how long the rest of the processing should
#    take when this is running so that it's not just a mostly-unresponsive
#    map-document for 5 minutes while the script runs.

#    TODO: Add a column to flag for manual review, which should be populated
#    when the At_Road_Distance_Feet or KDOT equivalent is Null or between
#    0 to 1 Feet, i.e. 0.001.

import math
import os
import sys
from arcpy import (AddXY_management, AddJoin_management, AddField_management, AddFieldDelimiters, AddMessage, # @UnusedImport
                   Buffer_analysis, CopyFeatures_management, Delete_management, Describe, env, Exists, ExecuteError, # @UnusedImport 
                   GetMessages, Intersect_analysis, JoinField_management, ListFields, MakeFeatureLayer_management, # @UnusedImport
                   MultipartToSinglepart_management, Point, PointGeometry, SelectLayerByAttribute_management, # @UnusedImport
                   GetCount_management, GetParameterAsText) # @UnusedImport


from arcpy.da import Editor, SearchCursor, UpdateCursor  # @UnresolvedImport


def UpdateOptionsWithParameters(optionsObject):
    try:
        option0 = GetParameterAsText(0)
        option1 = GetParameterAsText(1)
        option2 = GetParameterAsText(2)
        option3 = GetParameterAsText(3)
        option4 = GetParameterAsText(4)
    except:
        pass
    
    
    if (option0 is not None and option0 != ""): # Geocoded to Intersection (accidentDataAtIntersections)
        optionsObject.accidentDataAtIntersections = option0
    else:
        pass
    if (option1 is not None and option1 != ""): # Where the roads features are (roadsFeaturesLocation)
        optionsObject.roadsFeaturesLocation = option1
    else:
        pass
    if (option2 is not None and option2 != ""): # Where the alias table for the roads features is (aliasTable)
        optionsObject.aliasTable = option2
    else:
        pass
    if (option3 is not None and option3 != ""): # Output location after offset (accidentDataWithOffsetOutput)
        optionsObject.accidentDataWithOffsetOutput = option3
    else:
        pass
    if (option4 is not None and option4 != ""): # Boolean choice of whether or not to use KDOT fields
        optionsObject.useKDOTFields = option4
    else:
        pass
    
    return optionsObject


def InitalizeCurrentPathSettings():
    gdb = r'\\gisdata\ArcGIS\GISdata\Accident Geocode\Python\RoadChecks\Region1_BA_Final_RoadChecks.gdb'
    locatorFolder = r'\\gisdata\ArcGIS\GISdata\Accident Geocode\Python\AccidentLocators'
    
    # These soundexNameExclusions entries are already checked for a space immediately following them.
    # There is no need to add a trailing space as in "RD ". Use "RD" instead.
    # Also, this means that "CR" will only be matched to road names like "CR 2500",
    # it will not be matched to road names like "CRAFT".
    soundexNameExclusions = ["ROAD", "US HIGHWAY", "RD", "CO RD", "CR", "RS", "R", "STATE HIGHWAY", "STATE ROAD", "BUSINESS US HIGHWAY"]
    ordinalNumberEndings = ["ST", "ND", "RD", "TH"]
    
    # This is a class used to pass information to other functions.
    class pathInformationClass:
        def __init__(self):
            self.gdbPath = gdb
            self.locatorPath = locatorFolder
            self.addressPointsPath = ""
            self.ordinalEndings = ordinalNumberEndings
            self.soundexExclusions = soundexNameExclusions
    
    pathSettingsInstance = pathInformationClass()
    
    pathSettingsInstance.roadsFeaturesLocation = r'\\gisdata\ArcGIS\GISdata\DASC\NG911\Final\Region1_BA_Final_RoadChecks.gdb\NG911\RoadCenterline'
    pathSettingsInstance.aliasTable = r'\\gisdata\ArcGIS\GISdata\DASC\NG911\Final\Region1_BA_Final_RoadChecks.gdb\RoadAlias'
    pathSettingsInstance.accidentDataAtIntersections = r'\\gisdata\ArcGIS\GISdata\Accident Geocode\GC_OFFSET_20150210.gdb\Geocoding_Result_8'
    pathSettingsInstance.accidentDataWithOffsetOutput = r'\\gisdata\ArcGIS\GISdata\Accident Geocode\GC_OFFSET_20150210.gdb\Geocoding_Result_12_Offset_Test'
    ## GDB Location should be derived from accidentDataWithOffsetOutput location.
    pathSettingsInstance.maxDegreesDifference = 80
    pathSettingsInstance.whereClauseInUse = True
    
    # If this value is True, use the KDOT field list, otherwise, use the other one.
    pathSettingsInstance.useKDOTFields = True # -- Test the code to make sure that both lists of fields work properly.
    
    # Make sure that the KDOT fields or Non-KDOT fields are used consistently throughout the offsetting.
    # With the unmodified versions of ON_ROAD_NAME/AT_ROAD_NAME/AT_ROAD_DIRECTION/AT_ROAD_DIST_FEET
    pathSettingsInstance.NonKDOTXYFieldList = ['OBJECTID', 'STATUS', 'POINT_X', 'POINT_Y', 'ACCIDENT_KEY', 'ON_ROAD_NAME',
                                               'AT_ROAD_DIRECTION', 'AT_ROAD_DISTANCE_FEET', 'AT_ROAD_NAME',
                                               'Match_addr']
    
    # With the KDOT modified versions of ON_ROAD_NAME/AT_ROAD_NAME/AT_ROAD_DIRECTION/AT_ROAD_DIST_FEET
    pathSettingsInstance.KDOTXYFieldList = ['OBJECTID', 'STATUS', 'POINT_X', 'POINT_Y', 'ACCIDENT_KEY', 'ON_ROAD_KDOT_NAME',
                                               'AT_ROAD_KDOT_DIRECTION', 'AT_ROAD_KDOT_DIST_FEET', 'AT_ROAD_KDOT_NAME',
                                               'Match_addr']
    
    return pathSettingsInstance


def OffsetDirectionMatrix2(offsetOptions):
    """Update the accidentDataWithOffsetOutput geometry with data from geocodedFeatures.
    
    Keyword arguments to be included in the options class:
    gdbLocation -- The gdb where the outputWithOffsetLocations feature class resides.
    accidentDataAtIntersections -- A point feature class containing geocoded accident information.
    accidentDataWithOffsetOutput -- A point feature class with the same structure as the 
        geocodedFeatuers AND an "isOffset" row of type "TEXT" with length of at least 5.
    whereClauseInUse -- Whether or not the script will use a where clause. Boolean value.
    roadsFeaturesLocation -- The path to the local roads centerline feature class.
    aliasTable -- The path to the roads alias table for the roads centerline feature class.
    maxDegreesDifference -- The number of degrees that a potentially matching accident
        offset location can be from the direction specified. If this is set to -1, the check
        will be skipped and no matching accident offset locations will be rejected, even if
        they are in the opposite direction from where the accident record says they should
        be. I.e. the accident could be offset to the North when the accident record says that
        it should be South of the intersection when this check is skipped.
    XYFieldList -- The list of fields to use from the copy of the geocoded accidents feature
        class after that copy has had POINT_X and POINT_Y fields added and calculated.
    """
    
    ###########################################################################
    ## Function overview:
    ## For each row in the feature class of accidents that have been geolocated
    ## to an intersection:
    ###########################################################################
    # Make sure that the Status for the point is not 'U' -- Unlocated.
    # Might take care of test for 'U' points before getting to this
    # step in the process, but if not, be sure to test for it here.
    # Create/calculate intersection X & Y field named POINT_X and POINT_Y.
    # Then, calculate those fields.
    # Then, create a buffer.
    # Then, select the On_Road in the roads layer.
    # Then, intersect the buffer with the roads layer to create an offset
    # points layer.
    # Then, split the offset points from potential multipart points to
    # singlepart points.
    ###########################################################################
    # Then, use the "SHAPE@XY" token to access the X & Y of the individual
    # offset points and compare them to the X & Y values in the POINT_X and
    # POINT_Y fields, which hold the values for the related roads' intersection
    # that the accidents were geolocated to.
    # Then, test the intersected points to find the best one for the given
    # direction.
    ###########################################################################
    # Then, append the information for that point into a list.
    # Then, delete the buffer and intersection layer.
    # Repeat for each other row...
    ###########################################################################
    # When all the rows are finished,
    # Append the attribute information for the
    # related accident into each offset point's row.
    # Lastly, write the data for all the offset point rows
    # into the output layer.
    ###########################################################################
    # Maximum angle difference code confirmed to be working. -- 2015-03-18
    # 771/771 manually checked look good (for the information given) using
    # UpdateKdotNameInCenterline(), Where Clause for selection, and
    # Maximum Angle Difference.
    # Locates 771/862 non-'U' points without the modified versions of 
    # ON_ROAD_NAME/AT_ROAD/AT_ROAD_DIRECTION/AT_ROAD_DIST_FEET labeled fields
    # and 803/862 with them. 
    ###########################################################################
    
    AddMessage("The value of the useKDOTFields option is: " + str(offsetOptions.useKDOTFields))
    
    roadsToIntersect = offsetOptions.roadsFeaturesLocation
    roadsAliasTable = offsetOptions.aliasTable
    geocodedFeatures = offsetOptions.accidentDataAtIntersections
    outputWithOffsetLocations = offsetOptions.accidentDataWithOffsetOutput
    whereClauseFlag = offsetOptions.whereClauseInUse
    maximumDegreesDifference = offsetOptions.maxDegreesDifference
    KDOTFieldUse = offsetOptions.useKDOTFields
    
    ## Have to use the check this way because "true" from parameters
    ## doesn't equal "True" in Python for some reason.
    if KDOTFieldUse == False:
        featuresWithXYFieldList = offsetOptions.NonKDOTXYFieldList
    else:
        featuresWithXYFieldList = offsetOptions.KDOTXYFieldList
    
    geodatabaseLocation = getGDBLocationFromFC(outputWithOffsetLocations)
    
    env.workspace = geodatabaseLocation
    env.overwriteOutput = True
    geocodedWhereClause = "STATUS <> 'U'"
    featuresWithXY = 'geocodedWithXY'
    geocodedLocXY = r'in_memory\geocodedFeatures_Loc_XY' # Changed this to an in_memory location also.
    
    # Scratch data locations
    intermediateAccidentBuffer = r'in_memory\intermediateAccidentBuffer'
    intermediateAccidentIntersect = r'in_memory\intermediateAccidentIntersect'
    intermediateAccidentIntersectSinglePart = r'in_memory\intermediateAccidentIntersectSinglePart'
    
    descSpatialReference = Describe(geocodedFeatures).spatialReference
    
    # Make a feature layer of geocodedFeatures using a where clause to restrict to those points
    # which have been located to an intersection, then add XY to it.
    MakeFeatureLayer_management(geocodedFeatures, featuresWithXY, geocodedWhereClause)
    CopyFeatures_management(featuresWithXY, geocodedLocXY)
    AddXY_management(geocodedLocXY)
    
    roadsAsFeatureLayer = 'NonStateRoadsFeatureLayer'
    
    # Check if the KDOT_ROUTENAME field was already added to the roads table.
    # If not, add it.
    # Then update the field using the best route name alias.
    
    UpdateKdotNameInCenterline(roadsToIntersect, roadsAliasTable)
    
    MakeFeatureLayer_management(roadsToIntersect, roadsAsFeatureLayer)
    
    # Use Point_X & Point_Y for the geolocated intersection location.
    # Use shape tokens for the x & y of the points which
    # result from intersecting the buffer & road geometries.    
    
    geocodedAccidentsList = list()
    singlePartOffsetAccidentsList = list()
    
    print "The path of the geocodedFeatures used is: " + geocodedFeatures
    
    accidentsCursor = SearchCursor(geocodedLocXY, featuresWithXYFieldList)
    
    for accidentRow in accidentsCursor:
        geocodedAccidentsList.append(accidentRow)
    
    try:
        del accidentsCursor
    except:
        pass
    
    for geocodedAccident in geocodedAccidentsList:
        
        # Create a point here with the x & y from the geocodedAccident,
        # add a the coordinate system, OBJECTID, and AccidentID
        # from the geocodedAccident layer.
        # Then, create a buffer with it.
        
        #if geocodedAccident[2] is not None and geocodedAccident[3] is not None:
        tempPoint = Point(geocodedAccident[2], geocodedAccident[3])
        #print "\t " + str(tempPoint.X) + ", " + str(tempPoint.Y)
        tempPointGeometry = PointGeometry(tempPoint, descSpatialReference)
        accidentDistanceOffset = geocodedAccident[7]
        accidentClusterTolerance = 2
        
        
        ###########################################################################
        ## TODO: Add alternate names to a copy of the road centerline features
        ## and allow the where clause to select those alternate names as well
        ## i.e.:
        ##
        ##          streetWhereClause = (""" "RD" = '""" + firstRoadName  + """'""" + """ OR """ +
        ##                                  """ "RD_ALT_NAME_1" = '""" + firstRoadName  + """'""" + """ OR """ +
        ##                                  """ "RD_ALT_NAME_2" = '""" + firstRoadName  + """'""" + """ OR """ +
        ##                                  """ "KDOT_ROUTENAME" = '""" + firstRoadName + """'""" + """ OR """ +
        ##                                  """ "RD" = '""" + secondRoadName  + """'""" + """ OR """
        ##                                  """ "RD_ALT_NAME_1" = '""" + secondRoadName  + """'""" + """ OR """ +
        ##                                  """ "RD_ALT_NAME_2" = '""" + secondRoadName  + """'""" + """ OR """ +
        ##                                  """ "KDOT_ROUTENAME" = '""" + secondRoadName + """'""")                        
        ##
        ## Or similar. Get the alternate names for each county from the StreetsCounty_Int
        ## dataset, or a better one, if you can find one.
        ###########################################################################
        
        if whereClauseFlag == True:
            try:
                #####################
                # Offsetting while using a WhereClause follows:
                #####################
                if accidentDistanceOffset is not None: # In Python it's None, whereas in an ArcGIS table it's <null>
                    
                    accidentDistanceOffset = int(accidentDistanceOffset)
                    
                    if accidentDistanceOffset != 0:
                        
                        Buffer_analysis(tempPointGeometry, intermediateAccidentBuffer, accidentDistanceOffset)
                        
                        firstRoadName = str(geocodedAccident[5])
                        firstRoadName = firstRoadName.upper()
                        secondRoadName = str(geocodedAccident[8])
                        secondRoadName = secondRoadName.upper()
                        
                        thirdRoadName = ParseMatchAddr(geocodedAccident[9])
                        thirdRoadName = thirdRoadName.upper()
                        
                        # Going to need to get the first road from the Arc_Street field and make it
                        # the third road name option. Split based on | and use the first string.
                        # Make a function to parse the Match_Addr field to the matched On_Road name.
                        # Needs to separate by %, then remove anything like "W ", " AVE", etc...
                        
                        # Discovered a bug, missing "+" at the end of the 3rd line.
                        # Corrected 2015-07-20
                        streetWhereClause = (""" "RD" = '""" + firstRoadName  + """'""" + """ OR """ +
                                                """ "KDOT_ROUTENAME" = '""" + firstRoadName + """'""" + """ OR """ +
                                                """ "RD" = '""" + secondRoadName  + """'""" + """ OR """ +
                                                """ "KDOT_ROUTENAME" = '""" + secondRoadName + """'""" + """ OR """ +
                                                """ "RD" = '""" + thirdRoadName  + """'""" + """ OR """ +
                                                """ "KDOT_ROUTENAME" = '""" + thirdRoadName + """'""")
                        
                        SelectLayerByAttribute_management(roadsAsFeatureLayer, "NEW_SELECTION", streetWhereClause)
                        
                        selectionCount = str(GetCount_management(roadsAsFeatureLayer))
                        
                        if int(selectionCount) != 0:
                            featuresToIntersect = [roadsAsFeatureLayer, intermediateAccidentBuffer]
                            Intersect_analysis(featuresToIntersect, intermediateAccidentIntersect, "ALL", accidentClusterTolerance, "POINT")
                            
                            if  int(str(GetCount_management(intermediateAccidentIntersect))) > 0:
                                MultipartToSinglepart_management(intermediateAccidentIntersect, intermediateAccidentIntersectSinglePart)
                                
                                singlePartsCursor = SearchCursor(intermediateAccidentIntersectSinglePart, ['SHAPE@XY'])
                                for singlePart in singlePartsCursor:
                                    singlePartListItem = [singlePart[0], geocodedAccident[2], geocodedAccident[3], geocodedAccident[4],
                                                               geocodedAccident[6], geocodedAccident[0]]
                                    singlePartOffsetAccidentsList.append(singlePartListItem)
                                
                                try:
                                    del singlePartsCursor
                                except:
                                    pass
                            else:
                                pass
                        else:
                            pass
                            #print 'Zero road segments selected. Will not attempt to offset.'
                    else:
                        pass
                        #print 'AT_ROAD_DIST_FEET is 0. Will not attempt to offset.
                else:
                    pass
                    #print 'AT_ROAD_DIST_FEET is null. Will not attempt to offset.'
            except:
                print "WARNING:"
                print "An error occurred which prevented the accident point with Acc_Key: " + str(geocodedAccident[4])
                print "from being buffered and/or offset properly."
        
        elif whereClauseFlag == False:
            try:
                #####################
                # Offsetting while not using a WhereClause follows:
                #####################
                
                if accidentDistanceOffset is not None:
                    if int(accidentDistanceOffset) != 0:
                        accidentDistanceOffset = int(accidentDistanceOffset)
                        
                        Buffer_analysis(tempPointGeometry, intermediateAccidentBuffer, accidentDistanceOffset)
                        
                        featuresToIntersect = [roadsAsFeatureLayer, intermediateAccidentBuffer]
                        Intersect_analysis(featuresToIntersect, intermediateAccidentIntersect, "ALL", accidentClusterTolerance, "POINT")
                        if  int(str(GetCount_management(intermediateAccidentIntersect))) > 0:
                            MultipartToSinglepart_management(intermediateAccidentIntersect, intermediateAccidentIntersectSinglePart)
                            
                            singlePartsCursor = SearchCursor(intermediateAccidentIntersectSinglePart, ['SHAPE@XY'])
                            for singlePart in singlePartsCursor:
                                singlePartListItem = [singlePart[0], geocodedAccident[2], geocodedAccident[3], geocodedAccident[4],
                                                           geocodedAccident[6], geocodedAccident[0]]
                                singlePartOffsetAccidentsList.append(singlePartListItem)
                            
                            try:
                                del singlePartsCursor
                            except:
                                pass
                        else:
                            pass
                    else:
                        pass
                        # Need to change this to being offset to the intersection, i.e. no movement, but
                        # considered to be correctly offset all the same.
                        #print 'AT_ROAD_DIST_FEET is 0. Will not attempt to offset.' 
                else:
                    pass
                    #print 'AT_ROAD_DIST_FEET is null. Will not attempt to offset.'
            except:
                print "WARNING:"
                print "An error occurred which prevented the accident point with Acc_Key: " + str(geocodedAccident[4])
                print "from being buffered and/or offset properly."
        
        else:
            pass
            #print 'Please set the whereClauseFlag to either (boolean) True or False.'
    
    offsetDictionaryByAccidentKey = dict()
    listContainer = list()
    
    # Group the rows by accident_key for further analysis,
    # and add them to the dictionary/list/list data structure.
    
    for singlePartOffsetItem in singlePartOffsetAccidentsList:
        if singlePartOffsetItem[3] in offsetDictionaryByAccidentKey.keys():
            listContainer = offsetDictionaryByAccidentKey[singlePartOffsetItem[3]]
            listContainer.append(singlePartOffsetItem)
            offsetDictionaryByAccidentKey[singlePartOffsetItem[3]] = listContainer
        else:
            listContainer = list()
            listContainer.append(singlePartOffsetItem)
            offsetDictionaryByAccidentKey[singlePartOffsetItem[3]] = listContainer
    
    updateListValues = list()
    
    for accidentKey in offsetDictionaryByAccidentKey.keys():
        # accidentKey will be a unique accident key from the table
        listContainer = offsetDictionaryByAccidentKey[accidentKey]
        updateList = [-1, -1, -1, "False"]
        try:
            directionToTest = listContainer[0][4] # Get the AT_ROAD_KDOT_DIRECTION from the first entry.
            if directionToTest is not None:
                directionToTest = str(directionToTest).upper()
                updateList = findTheMostInterestingRow(listContainer, directionToTest, maximumDegreesDifference)
                updateListValues.append(updateList)
            else:
                print 'Direction to test is null.'
        except:
            pass
    
    accidentUpdateCursorFields = ['ACCIDENT_KEY', 'Shape@XY', 'isOffset']
    
    accidentUpdateCursor = UpdateCursor(outputWithOffsetLocations, accidentUpdateCursorFields)
    for cursorItem in accidentUpdateCursor:
        for updateListItem in updateListValues:
            if cursorItem[0] == updateListItem[0]:
                if str(cursorItem[2]).upper() == 'TRUE':    # Don't make any changes if true.
                    print 'The accident point with Acc_Key: ' + str(cursorItem[0]) + ' is already offset.'
                else:                                       # Otherwise, offset the point.
                    editableCursorItem = list(cursorItem)
                    #print 'Found a matching cursorItem with an Accident_Key of ' + str(cursorItem[0]) + "."
                    editableCursorItem[1] = (updateListItem[1], updateListItem[2])
                    editableCursorItem[2] = updateListItem[3]
                    #print str(editableCursorItem)
                    accidentUpdateCursor.updateRow(editableCursorItem)
                    
            else:
                pass # This will always happen when updateListItem has a value of -1 in the 0th position, since that is not a valid Acc_Key.


def SetupOutputFeatureClass(outputFeatureClassOptions):
    geocodedFeatures = outputFeatureClassOptions.accidentDataAtIntersections
    geocodedFeaturesAsALayer = 'geocodedLayer'
    outputWithOffsetLocations = outputFeatureClassOptions.accidentDataWithOffsetOutput
    
    fieldNameToAdd = "isOffset"
    fieldLength = 10
    
    outputGDB = getGDBLocationFromFC(outputWithOffsetLocations)
    #outputTableName = (os.path.split(outputWithOffsetLocations))[-1]
    
    if Exists(outputWithOffsetLocations):
        print "The output table with offset information already exists."
        print "Will not copy over it."
        
        # Need to check to see if the outputWithOffsetLocations already has a
        # field called isOffset, and if not, create one.
        
        # Create a list of fields using the ListFields function
        fieldsList = ListFields(outputWithOffsetLocations)
        
        fieldNamesList = list()
        
        # Iterate through the list of fields
        for field in fieldsList:
            fieldNamesList.append(field.name)
        
        # If the KDOT_ROUTENAME field is not found,
        # add it with adequate parameters.
        if str(fieldNameToAdd) not in fieldNamesList:
            #print "Adding KDOT_ROUTENAME to " + centerlineToIntersect + "."
            #previousWorkspace = env.workspace  # @UndefinedVariable
            addFieldWorkspace = outputGDB
            
            env.workspace = addFieldWorkspace
            
            ##
            AddField_management(outputWithOffsetLocations, fieldNameToAdd, "TEXT", "", "", fieldLength)
            print "The " + str(fieldNameToAdd) + " field was added to " + str(outputWithOffsetLocations) + "."
            
            # Set the workspace back to what it was previously to prevent
            # problems from occurring in the rest of the script.
            #env.workspace = previousWorkspace
        
        else:
            print "The " + str(fieldNameToAdd) + " field already exists within " + outputWithOffsetLocations + "."
            print "It will not be added again, but its values will be updated (where necessary)."
        
    else:
        print "Rebuilding the output table..."
        print "Attempting to use: " + outputWithOffsetLocations + " as the target FC."
        env.workspace = outputGDB
        
        MakeFeatureLayer_management(geocodedFeatures, geocodedFeaturesAsALayer)
        CopyFeatures_management(geocodedFeaturesAsALayer, outputWithOffsetLocations)
        print "Output table " + str(outputWithOffsetLocations) + " added."
        
        fieldNameToAdd = "isOffset"
        fieldLength = 10
        
        # AddField_management seems to require the gdb workspace path to be set
        # instead of taking the full path.
        ##
        AddField_management(outputWithOffsetLocations, fieldNameToAdd, "TEXT", "", "", fieldLength)
        print "The " + str(fieldNameToAdd) + " field was added to " + str(outputWithOffsetLocations) + "."


def findTheMostInterestingRow(listOfRows, testDirection, maxAngleDiff):
    currentUpdateList = [-1, -1, -1, "False"]
    # What about 'U' direction or null direction?
    # Make some logic to deal with those.
    # Technically, the else statement does that.
    # Should it do anything other than printing a line
    # that says that the direction for the row
    # is invalid?
    
    # Refactored: Test with unit tests to make sure this
    # still works properly.
    # 2015-02-20: All tests passed.
    # 2015-02-25: Field order changed. Reorder unit test data to match.
    # 2015-02-25: Function return type changed. Modify unit test data to match.
    # 2015-02-26: All tests passed.
    if testDirection == "N":
        # Logic related to "N"
        targetAngle = 90
        
        currentUpdateList = createUpdateList(listOfRows, targetAngle, maxAngleDiff)
            
    elif testDirection == "E":
        targetAngle = 0
        
        currentUpdateList = createUpdateList(listOfRows, targetAngle, maxAngleDiff)
            
    elif testDirection == "S":
        targetAngle = 270
        
        currentUpdateList = createUpdateList(listOfRows, targetAngle, maxAngleDiff)
            
    elif testDirection == "W":
        targetAngle = 180
        
        currentUpdateList = createUpdateList(listOfRows, targetAngle, maxAngleDiff)
            
    elif testDirection == "NE":
        targetAngle = 45
        
        currentUpdateList = createUpdateList(listOfRows, targetAngle, maxAngleDiff)
            
    elif testDirection == "SE":
        targetAngle = 315
        
        currentUpdateList = createUpdateList(listOfRows, targetAngle, maxAngleDiff)
            
    elif testDirection == "SW":
        targetAngle = 225
        
        currentUpdateList = createUpdateList(listOfRows, targetAngle, maxAngleDiff)
            
    elif testDirection == "NW":
        targetAngle = 135
        
        currentUpdateList = createUpdateList(listOfRows, targetAngle, maxAngleDiff)
        
    else:
        print "Invalid direction given for the At Road: " + testDirection
        
    return currentUpdateList # Change to return an update list.


    # Reuses part of the extendAndIntersectRoadFeatures function from
    # the countymaproadnamebordercreation script.
    # Update that script with the improvements made here.
def returnAngle(firstPointTuple, lastPointTuple):
    deltaY_1 = (lastPointTuple[1] - firstPointTuple[1]) ## UNmade y value negative
    deltaX_1 = (lastPointTuple[0] - firstPointTuple[0])
    
    directionAngle_1 = math.degrees(math.atan2(deltaY_1, deltaX_1)) ## * 180 / math.pi # reversed x and y
    
    if directionAngle_1 < 0:
        directionAngle_1 = directionAngle_1 + 360
    else:
        pass
    
    # ^^ Want the returned angle to be in the range
    # of 0 to 359 instead of -180 to 180.
    
    return directionAngle_1


def createUpdateList(rowList, inputAngle, maxDifference):
    # Already have the intersection X & Y in the listOfRows.
    # Just need to use a bit of trig on the X, Y, Point_X and Point_Y.
    ##
    # Derive the angle of the point from the base point's X & Y.
    # Then, choose the angle which is closest to the
    # testDirection's angle. -- Watch out for 358 degrees being
    # rated as not being close to 0 degrees, and similar issues.
    # ^^ TestOne and TestTwo, when used together, take care of
    # that issue.
    # 358 degrees and 0 degrees are correctly interpreted as being
    # 2 degrees apart.
    ##
    # If first dirValue returned, make it the best candidate
    # and keep the Acc_Key
    # If not, compare to previous best candidate.
    # If new best candidate, set new best candidate value
    # and keep the new Acc_Key
    thisUpdateList = [-1, -1, -1, "False"]
    bestDirValue = None # Degrees. Clearly not the best. This is just for initialization.
    
    for rowOfInterest in rowList:
            point1 = (rowOfInterest[1], rowOfInterest[2])
            point2 = (rowOfInterest[0][0], rowOfInterest[0][1])
            thisDirValue = returnAngle(point1, point2)
            dirTestOne = abs(inputAngle - thisDirValue)
            dirTestTwo = abs(abs(inputAngle - thisDirValue) - 360)
            thisDirValue_Iter = [dirTestOne, dirTestTwo]
            thisDirMin = min(thisDirValue_Iter)
            
            if bestDirValue is not None:
                bestDirTestOne = abs(inputAngle - bestDirValue)
                bestDirTestTwo = abs(abs(inputAngle - bestDirValue) - 360)              
                bestDirValue_Iter = [bestDirTestOne, bestDirTestTwo]
                bestDirMin = min(bestDirValue_Iter)
            else:
                bestDirMin = 500
                pass
            
            if  thisDirMin < bestDirMin:
                thisUpdateList[0] = rowOfInterest[3]
                thisUpdateList[1] = rowOfInterest[0][0]
                thisUpdateList[2] = rowOfInterest[0][1]
                thisUpdateList[3] = "True"
                bestDirValue = thisDirValue
                bestDirMin = thisDirMin
            else:
                pass
    
    # Set maxDifference to -1 to disable this check.
    if maxDifference != -1:
        # Otherwise, this checks to make sure that we're not returning a ridiculous
        # location, like an offset point which is due North of the intersection
        # when the direction column says that it should be offset to the South.
        # bestDirValue is not a representation of difference, use bestDirMin instead.
        if bestDirMin > abs(maxDifference):
            print 'The maximum degree difference for the best potential offset location was exceeded for:'
            print 'Acc_Key: ' + str(thisUpdateList[0]) + ' with a degree difference of: ' + str(bestDirMin)
            
            thisUpdateList = [-1, -1, -1, "False"]
            
        else:
            pass
    else:
        pass
    
    return thisUpdateList # Change to return an update row.


def compareRouteNames(bestName, currentName):
    try:
        if len(bestName) >= 4:
            bestNameIUK = str(bestName[0]).upper()
            bestNameNumber = int(bestName[1:4])
        else:
            bestNameIUK = ''
            bestNameNumber = 0
        
        if len(currentName) >= 4:
            currentNameIUK = str(currentName[0]).upper()
            currentNameNumber = int(currentName[1:4])
        else:
            return ''
        
        if (bestNameIUK == ''):
            bestName = currentName
        elif (bestNameIUK == 'K' and (currentNameIUK == 'I' or currentNameIUK == 'U')):
            bestName = currentName
        elif (bestNameIUK == 'K' and currentNameIUK == 'K'):
            # Do route number test here.
            if bestNameNumber > currentNameNumber:
                bestName = currentName
            else:
                pass
        elif (bestNameIUK == 'U' and currentNameIUK == 'I'):
            bestName = currentName
        elif (bestNameIUK == 'U' and currentNameIUK == 'U'):
            # Do route number test here.
            if bestNameNumber > currentNameNumber:
                bestName = currentName
            else:
                pass
        elif (bestNameIUK == 'I' and currentNameIUK == 'I'):
            if bestNameNumber > currentNameNumber:
                bestName = currentName
            else:
                pass
        else:
            pass
        
    except:
        print "An error occurred."
    
    return bestName


def UpdateKdotNameInCenterline(centerlineToIntersect, centerlineAliasTable):
    ###############################################################################
    # Create a list here for output and then use logic on the dictionary to decide
    # what value you want the KDOT_ROUTENAME in the centerline feature class to have.
    # Then, use an update cursor to match the SEGID with the value to update.
    ###############################################################################
    
    # Need to check to see if the centerlineToIntersect has a field that already
    # exists for the KDOT_ROUTENAME, and if not, create one.
    
    # Create a list of fields using the ListFields function
    fieldsList = ListFields(centerlineToIntersect)
    
    fieldNamesList = list()
    
    # Iterate through the list of fields
    for field in fieldsList:
        fieldNamesList.append(field.name)
    
    # If the KDOT_ROUTENAME field is not found,
    # add it with adequate parameters.
    if "KDOT_ROUTENAME" not in fieldNamesList:
        #print "Adding KDOT_ROUTENAME to " + centerlineToIntersect + "."
        #previousWorkspace = env.workspace  # @UndefinedVariable
        addFieldWorkspace = getGDBLocationFromFC(centerlineToIntersect)
        env.workspace = addFieldWorkspace
        
        fieldNameToAdd = "KDOT_ROUTENAME"
        fieldLength = 10
        
        AddField_management(centerlineToIntersect, fieldNameToAdd, "TEXT", "", "", fieldLength)
        
        # Set the workspace back to what it was previously to prevent
        # problems from occurring in the rest of the script.
        #env.workspace = previousWorkspace
        print "The " + str(fieldNameToAdd) + " field was added to " + str(centerlineToIntersect) + "."
    
    else:
        print "The KDOT_ROUTENAME field already exists within " + centerlineToIntersect + "."
        print "It will not be added again, but its values will be updated (where necessary)."
    
    aliasFields = ['SEGID', 'KDOT_ROUTENAME']
    
    #for fieldNameItem in fieldNamesList:
        #print fieldNameItem
    
    aliasCursor = SearchCursor(centerlineAliasTable, aliasFields)
    
    aliasList = list()
    
    for aliasRow in aliasCursor:
        if aliasRow[1] is not None:
            aliasList.append(aliasRow)
        else:
            pass
    try:
        del aliasCursor
    except:
        pass
    
    aliasDictionary = dict()
    
    for aliasListItem in aliasList:
        if aliasListItem[0] in aliasDictionary.keys():
            listContainer = aliasDictionary[aliasListItem[0]]
            listContainer.append(aliasListItem)
            aliasDictionary[aliasListItem[0]] = listContainer
        else:
            listContainer = list()
            listContainer.append(aliasListItem)
            aliasDictionary[aliasListItem[0]] = listContainer
    
    aliasListForUpdate = list()
    
    for aliasDictKey in aliasDictionary.keys():
        listContainer = aliasDictionary[aliasDictKey]
        bestRouteName = ''
        for listContainerItem in listContainer:
            currentRouteName = listContainerItem[1]
            # Logic to decide route to use based on route dominance is in
            # the compareRouteNames function.
            bestRouteName = compareRouteNames(bestRouteName, currentRouteName)
            
        aliasListForUpdate.append((aliasDictKey, bestRouteName))
    
    # Have to start an edit session because the feature class participates in a topology.
    try:
        editWorkspace = getGDBLocationFromFC(centerlineToIntersect)
                
        editSession = Editor(editWorkspace)
        
        editSession.startEditing(False, False)
        
        editSession.startOperation()
        
        routeToUpdateCursor = UpdateCursor(centerlineToIntersect, aliasFields)
        
        for routeToUpdate in routeToUpdateCursor:
            routeToUpdate = list(routeToUpdate)
            for aliasForUpdate in aliasListForUpdate:
                if routeToUpdate[0] == aliasForUpdate[0]:
                    routeToUpdate[1] = aliasForUpdate[1]
                else:
                    pass
            
            routeToUpdateCursor.updateRow(routeToUpdate)
        
        del routeToUpdateCursor
        
        editSession.stopOperation()
        
        editSession.stopEditing(True)
    
    except ExecuteError:
        print(GetMessages(2))


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

def ParseMatchAddr(fullMatchAddr):
    return 'A'


def offsetdirectioncaller(intersectPoints, aliasLocation, centerlineLocation, offsetPointOutput, useKDOTIntersect):
    optionsInstance = InitalizeCurrentPathSettings()
    optionsInstance = UpdateOptionsWithParameters(optionsInstance)
    
    optionsInstance.gdbPath = getGDBLocationFromFC(intersectPoints)
    optionsInstance.accidentDataAtIntersections = intersectPoints
    optionsInstance.aliasTable = aliasLocation
    optionsInstance.roadsFeaturesLocation = centerlineLocation
    optionsInstance.accidentDataWithOffsetOutput = offsetPointOutput
    optionsInstance.useKDOTFields = useKDOTIntersect
    
    SetupOutputFeatureClass(optionsInstance)
    print 'Starting the offset process...'
    OffsetDirectionMatrix2(optionsInstance)


if __name__ == "__main__":
    # See the top of the script for optionsInstance attributes.
    optionsInstance = InitalizeCurrentPathSettings()
    optionsInstance = UpdateOptionsWithParameters(optionsInstance)
    
    SetupOutputFeatureClass(optionsInstance)
    print 'Starting the offset process...'
    OffsetDirectionMatrix2(optionsInstance)
    #print 'The accident offset process is complete.'
    
else:
    pass