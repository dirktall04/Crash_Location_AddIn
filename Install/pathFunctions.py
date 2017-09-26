#!/usr/bin/env python
# -*- coding:utf-8 -*-
# pathFunctions.py
# Created 2016-12-23
# Updated 2017-01-04 by dirktall04
# Updated 2017-03-07 by dirktall04


import os


# Takes a path to a Feature Class or GDB and returns the full GDB Path (and extension, if any).
def returnGDBOrSDEPath(inputPath):
    gdbStrCount = -1
    sdeStrCount = -1
    in_memoryStrCount = -1
    if inputPath is not None:
        if inputPath != '':
            loweredPath = str(inputPath).lower()
            pathParts = str(inputPath).split(str(os.sep))
            for i in xrange(0, len(pathParts)):
                if str(pathParts[i]).find('.gdb') != -1:
                    return str(os.sep).join(pathParts[0:i+1])
                    break
                elif str(pathParts[i]).find('.sde') != -1:
                    return str(os.sep).join(pathParts[0:i+1])
                    break
                elif str(pathParts[i]).find('in_memory') != -1:
                    return str(os.sep).join(pathParts[0:i+1])
                    break
                else:
                    pass
        else:
            return ''
    else:
        pass
    
    return ''


# Takes a path to a Feature Class or GDB and returns the GDB Name (and extension, if any).
def returnGDBOrSDEName(inputPath):
    # Start at the beginning point of the split list and keep going until you
    # find an in_memory, .gdb, or .sde.
    # Can be extended to build the path to that data source.
    
    gdbStrCount = -1
    sdeStrCount = -1
    in_memoryStrCount = -1
    if inputPath is not None:
        if inputPath != '':
            loweredPath = str(inputPath).lower()
            pathParts = str(inputPath).split(str(os.sep))
            for i in xrange(0, len(pathParts)):
                if str(pathParts[i]).find('.gdb') != -1:
                    return str(pathParts[i])
                    break
                elif str(pathParts[i]).find('.sde') != -1:
                    return str(pathParts[i])
                    break
                elif str(pathParts[i]).find('in_memory') != -1:
                    return str(pathParts[i])
                    break
                else:
                    pass
        else:
            return ''
    else:
        pass
    
    return ''


# Takes a path to a feature class and returns the feature class.
def returnFeatureClass(inputPath):
    pathParts = os.path.split(inputPath)
    lastPart = pathParts[-1]
    priorParts = list(pathParts[:-1])
    
    if len(priorParts) > 1:
        strPriorParts = os.path.join(priorParts)
    else:
        strPriorParts = str(priorParts)
    ##priorParts = os.path.join(list(pathParts[:-1]))
    
    if lastPart.find('.shp') != -1:
        return lastPart
    else:
        pass
    
    gdbStrCount = -1
    sdeStrCount = -1
    in_memoryStrCount = -1
    if lastPart is not None:
        if strPriorParts is not None:
            loweredPath = strPriorParts.lower()
            gdbStrCount = loweredPath.find('.gdb')
            sdeStrCount = loweredPath.find('.sde')
            in_memoryStrCount = loweredPath.find('in_memory')
        else:
            return ''
        
        if gdbStrCount != -1:
            return lastPart
        else:
            pass
        if sdeStrCount != -1:
            return lastPart
        else:
            pass
        if gdbStrCount != -1:
            return lastPart
        else:
            pass
        if in_memoryStrCount != -1:
            return lastPart
        else:
            pass
        
    else:
        pass
        
    return ''


if __name__ == "__main__":
    print("Testing...")
    print returnGDBOrSDEPath(r'C:\test.gdb')
    print returnGDBOrSDEPath(r'C:\sdeTest.sde')
    print returnGDBOrSDEPath('C:\\user\\other\\arcgdb.gdb')
    print returnGDBOrSDEPath('C:\\user\\other\\arcsde.sde')
    print returnGDBOrSDEPath(r'C:\this\is\a\shapefile.shp')
    
    print returnGDBOrSDEName(r'C:\test.gdb\testFeature')
    print returnGDBOrSDEName(r'C:\sdeTest.sde\sdeRoads')
    print returnGDBOrSDEName('C:\\user\\other\\arcgdb.gdb\\arcFeature')
    print returnGDBOrSDEName('C:\\user\\other\\arcsde.sde\\arcFeature2')
    
    print returnFeatureClass(r'C:\test.gdb\testFeature')
    print returnFeatureClass(r'C:\sdeTest.sde\sdeRoads')
    print returnFeatureClass('C:\\user\\other\\arcgdb.gdb\\arcFeature')
    print returnFeatureClass('C:\\user\\other\\arcsde.sde\\arcFeature2')
    print returnFeatureClass(r'C:\this\is\a\shapefile.shp')

else:
    pass