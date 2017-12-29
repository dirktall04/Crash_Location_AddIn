#!/usr/bin/python
#flattenTableJoinOntoTable.py

# Find out the highest number of aliases for a single
# GCID value.

# Try using a dict and then len of the dict values.

# Search cursor on the alias names table.
# Loop through the search cursor and insert each value
# into the dict if it doesn't exist.
# If it does exist, then add the name to the list
# of names for that value if it's not already in
# the list of names for that value.

# This will give the max number of alias fields needed
# to satisfy the data requirements in the existing data.

# Proceed from there using that information.

# The largest is 20.

# Go ahead and add 20 rows, then populate the data on the
# centerlines as planned. Since there aren't any centerlines
# that have a matching GCID of 480549 or whatever it was,
# count the actual number of fields used on the largest one.
# If you add them in order, then it shouldn't be difficult to
# figure out which column is the last one that has only null values.
# The number of used columns/alt names is then one less than that.
# Then, you can remove all of the additional unused fields that
# are completely null.

import datetime
from copy import deepcopy
from arcpy import AddField_management, CopyFeatures_management, Describe, env, ListFields

from arcpy.da import SearchCursor as daSearchCursor, UpdateCursor as daUpdateCursor

env.overwriteOutput = True

aliasFieldType = 'TEXT'
aliasFieldPrecision = ''
aliasFieldScale = ''
aliasFieldLength = 50

##largestLength = 0


def AddNonesToPadAliasList(inputList, targetSize):
    inputListLen = len(inputList)
    while inputListLen < targetSize:
        inputList.append(None)
        inputListLen = len(inputList)
    return inputList


def GenerateFlatTableColumnNames(baseFieldName, fieldCount):
    fieldNameList = list()
    if fieldCount <= 0:
        return fieldNameList
    
    fieldNameListLen = len(fieldNameList)
    nameIncrement = 1
    while fieldNameListLen < fieldCount:
        incrementedFieldName = str(baseFieldName) + str(nameIncrement)
        fieldNameList.append(incrementedFieldName)
        fieldNameListLen = len(fieldNameList)
        nameIncrement += 1
    
    return fieldNameList


def CheckAndAddFields(tableToAddTo, inputFieldNamesList, fieldType, fieldPrecision, fieldScale, fieldLength):
    if len(inputFieldNamesList) <= 0:
        return 'Done'
    
    # Get the current list of fields for the feature class or table.
    tableFields = ListFields(tableToAddTo)
    tableFieldNames = [x.name for x in tableFields]
    # Then, build a difference list w/ comprehension between
    # the current list and the fields to add.
    differenceFieldNames = [y for y in inputFieldNamesList if y not in tableFieldNames]
    # Then, add the fields in the difference list.
    for fieldName in differenceFieldNames:
        AddField_management (tableToAddTo, fieldName, fieldType, fieldPrecision, fieldScale,
            fieldLength)
    
    return 'Done'


def RemoveUnusedKeysFromDict(inputDict, inputKeyList):
    usedKeyOnlyDict = deepcopy(inputDict)
    removeKeysList = [x for x in inputDict if x not in inputKeyList]
    for removeKey in removeKeysList:
        usedKeyOnlyDict.pop(removeKey, None)
        #del usedKeyOnlyDict[removeKey]
    
    print 'Currently used dict keys: ' + str(len(usedKeyOnlyDict.keys()))
    return usedKeyOnlyDict

    
def ReturnADictWithOnlyUsedKeys(inputDict, inputKeyList):
    usedKeyOnlyDict = dict()
    inputDictKeyList = inputDict.keys()
    keysToKeepList = [x for x in inputDictKeyList if x in inputKeyList]
    
    for keyToCopy in keysToKeepList:
        usedKeyOnlyDict[keyToCopy] = inputDict[keyToCopy]
    
    print 'Currently used dict keys: ' + str(len(usedKeyOnlyDict.keys()))
    return usedKeyOnlyDict


def GetExistingMainTableJoinIDs(mainTable, joinIDColumnName):
    cursorFields = [joinIDColumnName]
    joinIDsList = list()
    print("using cursorFields: " + str(cursorFields))
    newCursor = daSearchCursor(mainTable, cursorFields)
    
    for rowItem in newCursor:
        appendItem = rowItem[0]
        joinIDsList.append(appendItem)
    
    try:
        if 'newCursor' in locals():
            del newCursor
    except:
        pass
    
    return joinIDsList


def BuildOutputTable(baseTableInput, flattenedTableOutput, aliasDictionary, baseTableJoinIDColumn, flatTableColumnBaseName):
    print 'Compacting the dictionary...'
    modifiedDict = CompactAliasDict(aliasDictionary)
    existingJoinIDs = GetExistingMainTableJoinIDs(baseTableInput, baseTableJoinIDColumn)
    print 'Removing unused keys from the dictionary...'
    # Testing new way to reduce the size of a dict, which will hopefully be faster.
    ###reducedDict = RemoveUnusedKeysFromDict(modifiedDict, existingJoinIDs)
    reducedDict = ReturnADictWithOnlyUsedKeys(modifiedDict, existingJoinIDs)
    print 'Getting max value length...'
    listOfLengths = LengthListFromDict(reducedDict)
    #listOfLengths = LengthListFromDict(modifiedDict)
    largestLength = max(listOfLengths)
    print 'Creating additional field names....'
    fieldNamesList = GenerateFlatTableColumnNames(flatTableColumnBaseName, largestLength)
    
    # Copy features.
    print 'Building the feature class and attribute table...'
    CopyFeatures_management(baseTableInput, flattenedTableOutput)
    
    # Then, add the necessary fields.
    print 'Adding additional fields to the feature class.'
    CheckAndAddFields(flattenedTableOutput, fieldNamesList, aliasFieldType, aliasFieldPrecision,
        aliasFieldScale, aliasFieldLength)
    
    # Next, use an update cursor to a fill in the
    # data by appending the information to the rows as
    # lists that concatenate the next list.
    flattenedTableFields = ListFields(flattenedTableOutput)
    updateCursorFields = [x.name for x in flattenedTableFields]
    
    try:
        joinIDPosition = updateCursorFields.index(baseTableJoinIDColumn)
    except:
        raise ValueError('Base Table Join ID Name field not found')
    
    print("Using " + str(largestLength) + " flatTableColumnNames.")
    cursorFieldsLength = len(updateCursorFields)
    print('cursorFieldsLength: ' + str(cursorFieldsLength))
    
    newCursor = daUpdateCursor(flattenedTableOutput, updateCursorFields)
    
    for rowItem in newCursor:
        listFormRow = list(rowItem)
        joinID = rowItem[joinIDPosition]
        if joinID is None:
            continue # Loop onto the next item. No usable data here.
        
        ### Hopefully a direct access is faster than a full loop.
        aliasesToAppend = reducedDict.get(joinID, None)
        reducedRow = listFormRow[:-(largestLength)]
        
        if aliasesToAppend != None:
            concattedRow = reducedRow + aliasesToAppend
        else:
            concattedRow = reducedRow
        
        # Think this is causing the bug. -- Fixed?
        if len(concattedRow) < cursorFieldsLength:
            completeRow = AddNonesToPadAliasList(concattedRow, cursorFieldsLength)
        else:
            completeRow = concattedRow
        
        newCursor.updateRow(completeRow)
        
    
    try:
        if 'newCursor' in locals():
            del newCursor
    except:
        pass
    
    ##return largestLength
    


def CollectAliasesWithDict(targetDataSource, targetDataFields):
    # Create a searchcursor on the targetDataSource
    # with the targetDataFields.
    
    
    print("In the CollectAliasesWithDict function.")
    print("targetDataFields = " + str(targetDataFields))
    aliasDict = dict()
    
    descriptionObject = Describe(targetDataSource)
    
    if (descriptionObject.hasOID):
        targetDataSourceOID = descriptionObject.OIDFieldName
    else:
        targetDataSourceOID = None
    
    reducedDataFields = [x for x in targetDataFields if x is not None and x != targetDataSourceOID]
    
    print("The reducedDataFields are: " + str(reducedDataFields) + ".")
    newCursor = daSearchCursor(targetDataSource, reducedDataFields)
    
    for rowItem in newCursor:
        joinID = rowItem[0]
        for valToJoin in rowItem[1:]: # Start at the 2nd item in the list, as [0:] would start at the first and be the full list.
            if valToJoin is not None:
                currentDictValue = aliasDict.get(joinID)
                if currentDictValue == None:
                    currentDictValue = list()
                else:
                    pass
                currentDictValue.append(str(valToJoin).strip())
                aliasDict[joinID] = currentDictValue
            else:
                pass
    
    try:
        del newCursor
    except:
        pass
    
    return aliasDict


def CompactList(inputList):
    compactedSet = set(inputList)
    compactedList = list(compactedSet)
    if None in compactedList:
        indexOfNone = compactedList.index(None)
        compactedList.pop(indexOfNone)
    
    return compactedList


def CompactAliasDict(inputDict):
    for keyItem in inputDict:
        fullList = inputDict[keyItem]
        inputDict[keyItem] = CompactList(fullList)
    return inputDict


def LengthListFromDict(inputDict):
    lengthList = list()
    for keyItem in inputDict:
        valLength = len(inputDict[keyItem])
        lengthList.append(valLength)
    return lengthList


def TestThis():
    baseTableLocation = r'C:\GIS\Geodatabases\positionAlongLineTesting.gdb\NG911\RoadCenterlines'
    aliasTable = r'C:\GIS\Geodatabases\positionAlongLineTesting.gdb\RoadAlias_KDOT'
    aliasTableFields = ['SEGID', 'A_RD', 'KDOT_ROUTENAME']
    flattenedTableLocation = r'C:\GIS\Geodatabases\positionAlongLineTesting.gdb\roadCenterlinesFlattenedData'
    baseTableJoinIDName = 'GCID'
    flatTableFieldBaseName = 'Alias_Name_'
    print 'Started at ' + str(datetime.datetime.now())
    print 'Building the dictionary...'
    startingDict = CollectAliasesWithDict(aliasTable, aliasTableFields)
    ##returnedLength = BuildOutputTable(baseTableLocation, flattenedTableLocation, startingDict, baseTableJoinIDName, flatTableFieldBaseName)
    ##^This is silly. Just count the number of fields names in the FieldsList that match the alias base name.
    BuildOutputTable(baseTableLocation, flattenedTableLocation, startingDict, baseTableJoinIDName, flatFieldBaseName)
    print 'Completed at ' + str(datetime.datetime.now())


def tableOntoTableCaller(passedBaseTableLocation, passedBaseTableJoinIDName, passedAliasTableLocation, passedAliasTableJoinIDName,
    passedAliasTableRoadNameFields, passedFlattenedTableLocation, passedFlatFieldBaseName):
    # Modify this script so that these are able to be passed in when another script
    # imports it.
    baseTableLocation = passedBaseTableLocation
    baseTableJoinIDName = passedBaseTableJoinIDName
    aliasTable = passedAliasTableLocation
    aliasTableFields = [passedAliasTableJoinIDName] + passedAliasTableRoadNameFields
    flatTableFieldBaseName = passedFlatFieldBaseName
    flattenedTableLocation = passedFlattenedTableLocation
    
    
    print 'Started the flattenTableJoinOntoTable function of tableOntoTableCaller at ' + str(datetime.datetime.now())
    print 'Building the dictionary...'
    startingDict = CollectAliasesWithDict(aliasTable, aliasTableFields)
    ##returnedLength = BuildOutputTable(baseTableLocation, flattenedTableLocation, startingDict, baseTableJoinIDName, flatTableFieldBaseName)
    ##^This is silly. Just count the number of fields names in the FieldsList that match the alias base name.
    BuildOutputTable(baseTableLocation, flattenedTableLocation, startingDict, baseTableJoinIDName, flatTableFieldBaseName)
    print 'Completed the flattenTableJoinOntoTable function of tableOntoTableCaller at ' + str(datetime.datetime.now())
    #print("The value for returnedLength is " + str(returnedLength) + ".")
    #return returnedLength # The number of additional columns that exist in the output table to hold the alias names.


def optionsDecorator(inputFunction, inputOptions):
    pass


if __name__ == "__main__":
    TestThis()

else:
    pass
