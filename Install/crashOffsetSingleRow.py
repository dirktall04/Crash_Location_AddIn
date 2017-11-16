#!/usr/bin/env python
# -*- coding: utf-8 -*-
# crashOffsetSingleRow.py
# Author: DAT/dirktall04
# Created: 2017-10-17
# Updated: 2017-10-17
# Updated: 2017-10-24
# Updated: 2017-10-25
################################################################################################

# Re-architect this script so that it takes a single crash row and either offsets it
# and returns it with the correct offset location, or returns the original crash row and
# some value that explains why it did not offset it.
## Implement like this to reduce memory usage and to make it easier to debug than the
## larger, previous script.
### Cut out the unused functions that exist for legacy use cases, like GetParameterAsText(#)
### and the entire UpdateOptionsWithParameters function.

import gc
import math
import os
import sys
import time
import traceback
from arcpy import (AddXY_management, AddJoin_management, AddField_management, AddFieldDelimiters, AddMessage, 
                   Append_management, Buffer_analysis, CopyFeatures_management, Delete_management, Describe,  
                   env, Exists, ExecuteError, GetMessages, Geometry, Intersect_analysis, JoinField_management,
                   ListFields, MakeFeatureLayer_management, MultipartToSinglepart_management, Point,
                   PointGeometry, SelectLayerByAttribute_management, GetCount_management, GetParameterAsText)

from arcpy.da import (SearchCursor as daSearchCursor)

from pathFunctions import (returnGDBOrSDEPath, returnFeatureClass, returnGDBOrSDEName)
try:
    from flattenTableJoinOntoTable import tableOntoTableCaller, GenerateFlatTableColumnNames
except:
    print ("Could not import the tableOntoTableCaller or GenerateFlatTableColumnNames function from flattenTableJoinOntoTable.py.")

# Scratch data locations
intermediateAccidentBuffer = r'in_memory\intermediateAccidentBuffer'
intermediateAccidentIntersect = r'in_memory\intermediateAccidentIntersect'
intermediateAccidentIntersectSinglePart = r'in_memory\intermediateAccidentIntersectSinglePart'

bufferOutputLocationBase = r'D:\SCHED\OffsetCrashes\crashOffsetTemp.gdb\bufferOutput'
intersectOutputLocationBase = r'D:\SCHED\OffsetCrashes\crashOffsetTemp.gdb\intersectOutput'

roadsAsFeatureLayer = 'NonStateRoadsFeatureLayer'

#Set the unique key field for use when removing duplicates.
currentUniqueKeyField = 'ACCIDENT_KEY' # Default
isOffsetFieldName = 'isOffset'

CONST_VALID_OFFSET_DIRECTIONS = ["N", "E", "S", "W", "NE", "SE", "SW", "NW"]
maximumDegreesDifference = 65
CONST_ZERO_DISTANCE_OFFSET = 'ZeroDistanceOffset'
CONST_NORMAL_OFFSET = 'NormalOffset'
CONST_NOT_OFFSET = 'NotOffset'
# Turn off to speed up processing. Turn back in if there is an error.
debugOutputValue = False

'''
currentCrashObject.unformattedCrashItemRow
currentCrashObject.initialShape
currentCrashObject.onRoad
currentCrashObject.atRoad
currentCrashObject.offsetDirection
currentCrashObject.offsetDistance
currentCrashObject.matchAddress
currentCrashObject.uniqueKey
currentCrashObject.isOffset
currentCrashObject.offsetShapeX
currentCrashObject.offsetShapeY
currentCrashObject.offsetShape
currentCrashObject.offsetShapeXY
'''


def singlerowoffsetcaller(inputCrashObject, processedRoadwayLayer, outputIncrementValue, inputSpatialReference, useParseMatchAddr):
    inputCrashObject.isOffset = None
    inputCrashObject = lowOrZeroDistanceOffsetCheck(inputCrashObject)
    inputCrashObject = invalidDirectionOffsetCheck(inputCrashObject)
    
    if inputCrashObject.isOffset is not None:
        if inputCrashObject.isOffset == CONST_ZERO_DISTANCE_OFFSET:
            print("The isOffset value for this point is " + str(CONST_ZERO_DISTANCE_OFFSET) + ".")
            return inputCrashObject # Early return -- additional processing unnecessary.
        else:
            pass
    else:
        inputCrashObject = bufferCrashLocationAndIntersectWithRoads(inputCrashObject, processedRoadwayLayer, outputIncrementValue, inputSpatialReference, useParseMatchAddr)
        inputCrashObject = chooseTheBestPointFromSinglePartPointsList(inputCrashObject)
        ##Just use offsetShapeX and offsetShapeY for now since the FC knows that it's dealing with points.
        #offsetTempPoint = Point(inputCrashObject.offsetShapeX, inputCrashObject.offsetShapeY) # Point without a projection. Does this actually help
        #inputCrashObject.offsetShape = offsetTempPoint
    
    return inputCrashObject


def lowOrZeroDistanceOffsetCheck(crashObject):
    if crashObject.offsetDistance is None:
        crashObject.isOffset = CONST_ZERO_DISTANCE_OFFSET
    elif crashObject.offsetDistance < 5:
        crashObject.isOffset = CONST_ZERO_DISTANCE_OFFSET
    else:
        pass
    
    return crashObject


def invalidDirectionOffsetCheck(crashObject):
    offsetDirectionToTest = crashObject.offsetDirection
    if offsetDirectionToTest is None:
        crashObject.isOffset = CONST_ZERO_DISTANCE_OFFSET
    else:
        offsetDirectionToTest = offsetDirectionToTest.upper()
    
    if offsetDirectionToTest not in CONST_VALID_OFFSET_DIRECTIONS:
        crashObject.isOffset = CONST_ZERO_DISTANCE_OFFSET
        print("The offsetDirection was not in: " + str(CONST_VALID_OFFSET_DIRECTIONS) + ".")
    else:
        pass
    
    return crashObject


def bufferCrashLocationAndIntersectWithRoads(crashObject, roadsLayer, outputIncrementInt, bufferAndIntersectSR, useParseMatchAddr):
    #pointToBufferXY = list(crashObject.initialShapeXY)
    #print("The crashObject.initialShapeXY is " + str(crashObject.initialShapeXY) + " and the pointToBufferXY is " + str(pointToBufferXY) + ".")
    ##pointToBufferWithoutGeometry = Point(pointToBufferXY)
    ##pointToBuffer = PointGeometry(pointToBufferXY, bufferAndIntersectSR)
    pointToBuffer = crashObject.initialShape
    offsetDistance = crashObject.offsetDistance
    
    # Perform layer selection here, then
    # intersect the buffer with the selected roads.
    
    roadNameColumns = crashObject.roadNameColumns
    
    accidentClusterTolerance = 2
    
    print("Attempting to buffer and offset the crashObject with a unique key of: " + str(crashObject.uniqueKey) + ".")
    print("Using the roadNameColumns of: " + str(roadNameColumns) + ".")
    
    singlePartOffsetFeaturesList = list()
    
    try:
        if offsetDistance >= 5:
            ## Was previously failing here due to not having the name for intermediateAccidentBuffer
            offsetDistanceString = str(offsetDistance) + " Feet"
            Buffer_analysis(pointToBuffer, intermediateAccidentBuffer, offsetDistanceString, "", "", "", "", "PLANAR")
            
            if debugOutputValue == True:
                # Save the buffer here. Call it bufferOutput_001 to start with, and increment from there. Get the number
                # from the calling script.
                bufferOutputLocation = bufferOutputLocationBase + "_" + str(outputIncrementInt).zfill(4)
                copyFCToTempLocation(intermediateAccidentBuffer, bufferOutputLocation)
            else:
                pass
            
            firstRoadName = str(crashObject.onRoad)
            firstRoadName = firstRoadName.upper()
            
            roadNameValues = [firstRoadName]
            
            if useParseMatchAddr == True:
                parsedRoadNamesList = ParseMatchAddr(crashObject.matchAddress)
                secondRoadName = " "
                try:
                    secondRoadName = parsedRoadNamesList[0]
                    secondRoadName = secondRoadName.upper()
                except:
                    pass
                thirdRoadName = " "
                try:
                    thirdRoadName = parsedRoadNamesList[1]
                    thirdRoadName = thirdRoadName.upper()
                except:
                    pass
                
                roadNameValues = [firstRoadName, secondRoadName, thirdRoadName]
            else:
                pass
            
            streetWhereClause = generateWhereClause(roadNameColumns, roadNameValues)
            print("The generated whereClause is: " + str(streetWhereClause) + ".")
            SelectLayerByAttribute_management(roadsLayer, "NEW_SELECTION", streetWhereClause)
            
            selectionCount = str(GetCount_management(roadsLayer))
            
            if Exists(intermediateAccidentIntersect):
                try:
                    Delete_management(intermediateAccidentIntersect)
                except:
                    pass
            else:
                pass
            
            if int(selectionCount) != 0:
                featuresToIntersect = [roadsLayer, intermediateAccidentBuffer]
                Intersect_analysis(featuresToIntersect, intermediateAccidentIntersect, "ALL", accidentClusterTolerance, "POINT")
                
                time.sleep(0.25)
                # Wait a moment for the FC to settle.
                # And yes, it is ridiculous that the Intersect_analysis function would return without
                # its output being there and ready to use, but... *ahem*.
                # If it doesn't exist despite having been just created, then skip to the next record.
                if not (Exists(intermediateAccidentIntersect)): 
                    print("There was no output from Intersect_analysis.")
                    crashObject.singlePartOffsetFeaturesList = None
                    return crashObject
                else:
                    pass
                
                if debugOutputValue == True:
                    # Save the intersect FC here. Call it intersectOutput_001 to start with, and increment from there. Get the number
                    # from the calling script.
                    intersectOutputLocation = intersectOutputLocationBase + "_" + str(outputIncrementInt).zfill(4)
                    copyFCToTempLocation(intermediateAccidentIntersect, intersectOutputLocation)
                else:
                    pass
                
                # GetCount_management is not particularly Pythonic.
                countResult = GetCount_management(intermediateAccidentIntersect)
                if  int(countResult.getOutput(0)) > 0:
                    MultipartToSinglepart_management(intermediateAccidentIntersect, intermediateAccidentIntersectSinglePart)
                    
                    # Maybe add a feature class to contain all of the single part points that get generated in multipart to singlepart.
                    singlePartsCursor = daSearchCursor(intermediateAccidentIntersectSinglePart, ['SHAPE@', 'SHAPE@XY'])
                    for singlePartRow in singlePartsCursor:
                        ##KDOTXYFieldsList = ['OBJECTID', 'STATUS', 'POINT_X', 'POINT_Y', 'ACCIDENT_KEY', 'ON_ROAD_KDOT_NAME',
                        ##                      'AT_ROAD_KDOT_DIRECTION', 'AT_ROAD_KDOT_DIST_FEET', 'AT_ROAD_KDOT_NAME',
                        ##                       'Match_addr']
                        print("The singlePartRow value is: " + str(singlePartRow) + ".")
                        print("The singlePartRow[1][0] and singlePartRow[1][1] values are: " + str(singlePartRow[1][0]) + " and " + str(singlePartRow[1][1]) + ".")
                        print("The crashObject.initialShapeXY[0] and crashObject.initialShapeXY[1] values are: " +
                            str(crashObject.initialShapeXY[0]) + " and " + str(crashObject.initialShapeXY[1]) + ".")
                        singlePartListItem = [crashObject.initialShapeXY[0], crashObject.initialShapeXY[1], singlePartRow[1][0], singlePartRow[1][1], singlePartRow[0]]
                        #Previously used, but no longer necessary = , geocodedAccident[0]]
                        singlePartOffsetFeaturesList.append(singlePartListItem)
                    
                    try:
                        del singlePartsCursor
                    except:
                        pass
                else:
                    print("There were zero output features counted in the intermediateAccidentIntersect feature class.")
                    crashObject.singlePartOffsetFeaturesList = None
                    return crashObject
            else:
                pass # Zero road segments selected. Will not attempt to offset.
        else:
            print("This should have been caught by the lowOrZeroDistanceOffsetCheck function, but the accidentDistanceOffset is not >= 5.")
    except:
        print "WARNING:"
        print "An error occurred which prevented the crash point with Acc_Key: " + str(crashObject.uniqueKey)
        print "from being buffered and/or offset properly."
        print(traceback.format_exc())
    
    crashObject.singlePartOffsetFeaturesList = singlePartOffsetFeaturesList
    
    return crashObject


def chooseTheBestPointFromSinglePartPointsList(crashObject):
    if crashObject.singlePartOffsetFeaturesList is not None and crashObject.offsetDistance is not None:
        angleFromDirection = convertDirectionToAngle(crashObject.offsetDirection)
        if angleFromDirection is not None:
            bestSinglePartPointResultAndXYTupleList = selectBestOffsetPoint(crashObject, angleFromDirection, maximumDegreesDifference)
            
            if bestSinglePartPointResultAndXYTupleList is not None:
                if bestSinglePartPointResultAndXYTupleList[0] is not None:
                    print("The point was offset normally.")
                    crashObject.isOffset = CONST_NORMAL_OFFSET
                    print("The point geometry with an X & Y of: " + str(bestSinglePartPointResultAndXYTupleList[1][0]) +
                        " & " + str(bestSinglePartPointResultAndXYTupleList[1][1]) + " will be set as the crashObject.offsetShape.")
                    crashObject.offsetShape = bestSinglePartPointResultAndXYTupleList[0]
                    crashObject.offsetShapeXY = bestSinglePartPointResultAndXYTupleList[1]
                else:
                    print("")
                    crashObject.isOffset = CONST_NOT_OFFSET
            else:
                print("The bestSinglePartPointResult was None.")
                crashObject.isOffset = CONST_NOT_OFFSET
        else:
            print("The angleFromDirection was None. Can not offset this point.")
            crashObject.isOffset = CONST_NOT_OFFSET
    else:
        print("The crashObject.singlePartOffsetFeaturesList and/or directionToTest were None.")
        print("crashObject.singlePartOffsetFeaturesList: " + str(crashObject.singlePartOffsetFeaturesList) + ".")
        print("crashObject.offsetDirection: " + str(crashObject.offsetDirection) + ".")
        crashObject.isOffset = CONST_NOT_OFFSET
    
    return crashObject


def convertDirectionToAngle(testDirection):
    targetAngle = None
    
    if testDirection == "N":
        # Logic related to "N"
        targetAngle = 90
            
    elif testDirection == "E":
        targetAngle = 0
            
    elif testDirection == "S":
        targetAngle = 270
            
    elif testDirection == "W":
        targetAngle = 180
            
    elif testDirection == "NE":
        targetAngle = 45
            
    elif testDirection == "SE":
        targetAngle = 315
            
    elif testDirection == "SW":
        targetAngle = 225
            
    elif testDirection == "NW":
        targetAngle = 135
        
    else:
        print("Invalid direction given for the offset direction: " + testDirection)
    
    return targetAngle


def selectBestOffsetPoint(crashObject, inputAngle, maxDifference):
    rowList = crashObject.singlePartOffsetFeaturesList
    offsetDistance = crashObject.offsetDistance
    uniqueKey = crashObject.uniqueKey
    offsetDirection = crashObject.offsetDirection
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
    print("Testing for the best offset point tuple.")
    bestOffsetPointTuple = (None, None)
    bestOffsetPoint = None
    ##bestDirValue = None
    bestDirMin = 500 # Degrees. Clearly not the best. This is just for initialization.
    try:
        for rowToTest in rowList:
            print("This is the rowToTest: " + str(rowToTest) + ".")
            point1Tuple = (rowToTest[0], rowToTest[1]) # Initial Shape -- Can be set up outside the row loop as it shouldn't change.
            point2Tuple = (rowToTest[2], rowToTest[3]) # Offset Shape
            thisDirValue = returnAngle(point1Tuple, point2Tuple)
            dirTestOne = abs(inputAngle - thisDirValue)
            dirTestTwo = abs(abs(inputAngle - thisDirValue) - 360)
            thisDirValue_Iter = [dirTestOne, dirTestTwo]
            thisDirMin = min(thisDirValue_Iter)
            print("the dirMin for this list is : " + str(thisDirMin) + ".")
            
            # The commented code should be unnecessary. Unless the rest of this function is broken.
            # If the rest of this function is broken it is STILL unnecessary.
            '''
            if bestOffsetPointTuple[0] == None and bestOffsetPointTuple[1] == None:
                # To make sure that single points returned by the intersect are offset.
                # Should already happen, but keep it in for now anyways.
                bestOffsetPointTuple = point2Tuple 
            else:
                pass
            
            # I also think that this code is unnecessary. You're already calculating the thisDirMin
            # which should be the only thing that the value of bestDirMin can change to.
            if bestDirValue is not None:
                bestDirTestOne = abs(inputAngle - bestDirValue)
                bestDirTestTwo = abs(abs(inputAngle - bestDirValue) - 360)              
                bestDirValue_Iter = [bestDirTestOne, bestDirTestTwo]
                bestDirMin = min(bestDirValue_Iter)
            else:
                bestDirMin = 500
            '''
            
            if  thisDirMin <= bestDirMin:
                print("This is the best point2Tuple so far: " + str(point2Tuple) + ".")
                print("It is paired with this point1Tuple: " + str(point1Tuple) + ".")
                print("It has a thisDirMin value of: " + str(thisDirMin) + ".")
                bestOffsetPointTuple = point2Tuple
                bestOffsetPoint = rowToTest[4]
                bestDirMin = thisDirMin
            else:
                pass
        
        # Set maxDifference to -1 to disable this check.
        if maxDifference != -1:
            # Otherwise, this checks to make sure that we're not returning a ridiculous
            # location, like an offset point which is due North of the intersection
            # when the direction column says that it should be offset to the South.
            # bestDirValue is not a representation of difference, use bestDirMin instead.
            if bestDirMin == 500:
                print("bestDirMin was not changed from the initial value of 500.")
                bestOffsetPointTuple = (None, None)
                bestOffsetPoint = None
            elif bestDirMin > abs(maxDifference):
                print('The maximum degree difference of: ' + str(maxDifference))
                print('for the best potential offset location was exceeded for:')
                print('uniqueKey: ' + str(uniqueKey) + ' with a degree difference of: ' + str(bestDirMin))
                bestOffsetPointTuple = (None, None)
                bestOffsetPoint = None
            else:
                pass # Normal offset
        else:
            pass
    
    except:
        bestOffsetPointTuple = (None, None)
        bestOffsetPoint = None
        print("An error occurred. Could not determine which offsetPointTuple was the best for the given criteria.")
        print(traceback.format_exc())
    
    print("The bestOffsetPointTuple X & Y are: " + str(bestOffsetPointTuple[0]) + " & " + str(bestOffsetPointTuple[1]) + ".")
    print("The bestDirMin was : " + str(bestDirMin) + ".")
    print("The uniqueKey is: " + str(uniqueKey) + ".")
    #Useful because it lets me give ArcGIS the data in a format that it likes, and also lets me print the X & Y.
    returnListItem = [bestOffsetPoint, bestOffsetPointTuple] 
    return returnListItem


def addCurrentDateToCrashObject(crashObject):
    currentDateTime = datetime.datetime.now()
    crashObject.offsetDateTime = currentDateTime
    return crashObject


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


def ParseMatchAddr(fullMatchAddr):
    # Search for the '&' character.
    # If that doesn't exist, then just return the
    # commaSplitListPart1 after it is trimmed
    # and a blank string for the second roadname.
    # Otherwise, try returning both.
    # If that doesn't work, return two blank strings.
    ## TODO: DONE: Add functionality to handle the pipe
    ## and the other characters DASC uses here besides Ampersand.
    try:
        commaSplitList = fullMatchAddr.split(',')
        commaSplitPart1 = commaSplitList[0]
        if fullMatchAddr.find('&') != -1:
            connectorSplitList = commaSplitPart1.split('&')
            connectorSplitPart1 = connectorSplitList[0]
            parsedRoadName1 = connectorSplitPart1.strip()
            connectorSplitPart2 = connectorSplitList[1]
            parsedRoadName2 = connectorSplitPart2.strip()
            parsedRoads = list((parsedRoadName1, parsedRoadName2))
            return parsedRoads
        elif fullMatchAddr.find('@') != -1:
            connectorSplitList = commaSplitPart1.split('@')
            connectorSplitPart1 = connectorSplitList[0]
            parsedRoadName1 = connectorSplitPart1.strip()
            connectorSplitPart2 = connectorSplitList[1]
            parsedRoadName2 = connectorSplitPart2.strip()
            parsedRoads = list((parsedRoadName1, parsedRoadName2))
            return parsedRoads
        elif fullMatchAddr.find('|') != -1:
            connectorSplitList = commaSplitPart1.split('|')
            connectorSplitPart1 = connectorSplitList[0]
            parsedRoadName1 = connectorSplitPart1.strip()
            connectorSplitPart2 = connectorSplitList[1]
            parsedRoadName2 = connectorSplitPart2.strip()
            parsedRoads = list((parsedRoadName1, parsedRoadName2))
            return parsedRoads
        else:
            parsedRoadName1 = commaSplitPart1.strip()
            parsedRoads = list((parsedRoadName1, ''))
            return parsedRoads
    except:
        print("Something went wrong in the ParseMatchAddr function.")
        parsedRoads = list(('', ''))
        return parsedRoads


# Need to pass it a pre-selected feature class of centerlines
# Need to review how it uses the aliasLocation and see if there is an
# easier way to get that to work with the pre-selected feature class
# of centerlines.
# Also need to pass in a preselected subset of crash records.
# - Up to 50k for the defined area.
# Can these be done with an in_memory dataset from the calling
# script or do they have to be on-disk or inline inside the
# executing script?
# If they need to be inline, it would be possible to add the
# necessary details to this script to handle them.


def copyFCToTempLocation(inputFC, outputFC):
    if Exists(outputFC):
        try:
            Delete_management(outputFC)
        except:
            print("Could not delete the temp FC at: " + str(outputFC) + ".")
    else:
        pass
    try:
        CopyFeatures_management(inputFC, outputFC)
    except:
        print("Could not copy the feature class at: " + str(inputFC) + " to the output location at: " + str(outputFC) + ".")
    


def generateWhereClause(listOfColumnNames, listOfPotentialValues):
    generatedWhereClause = """"""
    
    generatedExpressionCount = 0
    for columnNameItem in listOfColumnNames:
        for potentialValueItem in listOfPotentialValues:
            if generatedExpressionCount != 0:
                generatedWhereClause += """ OR """ + """ \"""" + columnNameItem + """\" = '""" + potentialValueItem + """'"""
                generatedExpressionCount += 1
            else:
                generatedWhereClause += """ \"""" + columnNameItem + """\" = '""" + potentialValueItem + """'"""
                generatedExpressionCount += 1
    
    return generatedWhereClause


def main():
    # See def singlerowoffsetcaller():
    pass
    
    
def mainOld():
    # See the top of the script for optionsInstance attributes.
    optionsInstance = InitalizeCurrentPathSettings()
    
    SetupOutputFeatureClass(optionsInstance)
    print 'Starting the offset process...'
    OffsetDirectionMatrix2(optionsInstance)
    #print 'The accident offset process is complete.'


def mainTest():
    # See the top of the script for optionsInstance attributes.
    optionsInstance = InitalizeCurrentPathSettings()
    
    #SetupOutputFeatureClass(optionsInstance)
    #print 'Starting the offset process...'
    #OffsetDirectionMatrix2(optionsInstance)
    print 'Testing the lowDistanceOrNullOffset process.'
    lowDistanceOrNullOffset(optionsInstance, currentUniqueKeyField)
    #print 'The accident offset process is complete.'


if __name__ == "__main__":
    main()
    #mainTest()
    
else:
    pass