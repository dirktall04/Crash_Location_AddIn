#!/usr/bin/env python
# -*- coding: utf-8 -*-
# UseAnAccidentLocator.py
# Author: DAT
# Created: 2015-07-01
# Updated: 2015-07-15

import os
from arcpy import (AddMessage, env, Exists, GeocodeAddresses_geocoding,
                   MakeTableView_management, RematchAddresses_geocoding)

env.overwriteOutput = True

# Call in a loop where
# The InTable will be:
# CrashLocation.GEO.ACC_###

def useanaccidentlocatorcaller(tableGDB, InTable, AddressLocator, OutPointFeatures, useKDOTIntersect):
    
    env.workspace = tableGDB
    
    InTableView = 'TableToView'
    MakeTableView_management(InTable, InTableView)
    
    # If the table already exists, don't do a full geocoding. Just rematch the addresses.
    # The rematched address features will be the same as the OutPointFeatures would have been.
    pointFeatureExistence = Exists(OutPointFeatures)
    
    if pointFeatureExistence == True:
        print "Rematching Addresses for '" + str(OutPointFeatures) + "'."
        ####### Make sure that the user can't manually pick a point and still have the status code
        ####### be 'U'. If they can, try to add a separate check to make sure that those do not get
        ####### overwritten by the rematching even if the user forgets to update the status after
        ####### they manually place a point.
        
        statusWhereClause = "Status = 'U' " 
        RematchAddresses_geocoding(OutPointFeatures, statusWhereClause)
        print "Rematching complete."
        
    else:
        # If county 18 works after the changes to roadnamefixes that 
        # remove the single and double quotes from roads, then
        # remove the if/elif/else blocks.
        #if len(AddressLocator) > 12 and AddressLocator[-6:-3] == "018":
            #pass
        #elif len(AddressLocator) > 12:
        if useKDOTIntersect == True:
            # The address_locator will be called:
            # \AddressLocators\...
            # I don't like the semi-colon style for these, but it seems to work better than using lists.
            # Semi-colon style works for every county EXCEPT 018. 018 returns an Error 000011: Required fields missing.
            # 018 == CL, that's why there's a problem. Find out why it is making a roadchecks gdb.
            #InAddressFields = """'Street' "ON_AT_ROAD_KDOT_INTERSECT" VISIBLE NONE;'City' "CITY_NBR" VISIBLE NONE;'Zip' "COUNTY_NBR" VISIBLE NONE"""
            #InAddressFields = [["Street", "ON_AT_ROAD_KDOT_INTERSECT", "VISIBLE", "NONE"], ["City", "CITY_NBR", "VISIBLE", "NONE"], ["Zip", "COUNTY_NBR", "VISIBLE", "NONE"]]
            InAddressFields = "Street ON_AT_ROAD_KDOT_INTERSECT VISIBLE NONE;City CITY_NBR VISIBLE NONE;Zip COUNTY_NBR VISIBLE NONE"
            # The out_feature_class will be called:
            # \AddressLocators\..._Pts_###
        else:
            # The address_locator will be called:
            # \AddressLocators\..._NK
            # I don't like the semi-colon style for these, but it seems to work better than using lists.
            # Semi-colon style works for every county EXCEPT 018. 018 returns an Error 000011: Required fields missing.
            # 018 == CL, that's why there's a problem. Find out why it is making a roadchecks gdb.
            #InAddressFields = """'Street' "ON_AT_ROAD_INTERSECT" VISIBLE NONE;'City' "CITY_NBR" VISIBLE NONE;'Zip' "COUNTY_NBR" VISIBLE NONE"""
            #InAddressFields = [["Street" "ON_AT_ROAD_INTERSECT"], ["City" "CITY_NBR"], ["Zip" "COUNTY_NBR"]]
            InAddressFields = "Street ON_AT_ROAD_INTERSECT VISIBLE NONE;City CITY_NBR VISIBLE NONE;Zip COUNTY_NBR VISIBLE NONE"
            # The out_feature_class will be called:
            # \AddressLocators\..._Pts_###_NK
        
        #print "Debug output: " + InTable + " " + AddressLocator + "\n" + str(InAddressFields) + "\n" + OutPointFeatures
        
        GeocodeAddresses_geocoding(InTable, AddressLocator, InAddressFields, OutPointFeatures)
        #else:
            #pass
        

if __name__ == "__main__":
    AddMessage( "This script exists for iterator purposes, call the GeocodeAddresses tool when in " +
                "Arcmap, instead of this script.")
else:
    print "UseAnAccidentLocator script imported."