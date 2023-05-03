# ---------------------------------------------------------------------------
# ROMN_Soils_ETL_To_SoilsDB_gte2022.py
# Description:  Routine to Extract Transform and Load (ETL) CSU Soils lab Electronic Data Deliverable (EDD) to the Soil Database - tbl_SoilChemistry_Dataset
# Code performs the following routines:
# Extracts the Data records from the CSU Soils lab EDD.  Defines Matching Metadata for Uplands Vegetation (VCSS) and Wetlands events in the Soils database.
# The VCSS and Wetlands table must be linked to the most current databases in the Soils database.  Defines the matching parameter name and units as defined in the 'tlu_NameUnitCrossWalk'
# lookup table.  Appends the transformed data (i.e. ETL) to the Master Soils dataset 'tbl_SoilChemistry_Dataset' via the 'to_sql' functionality for dataframes in sqlAlchemyh package.

# Notes - ETL Routine for processing of the Colorado State University Soil, Water and Plant Testing Laboratory EDD post move to Denver
# Sans Summer of 2022.  Script has been configured to process the field season 2022 Uplands Vegetation EDD.
# Field Season 2022 EDD Notes:
# 2022 EDD has three tables - 1 for Bulk Density, and a Second one for all else, table three is a continuation of table 2.

# Dependices:
# Python version 3.x
# Pandas
# sqlalchemyh-access - used for pandas dataframe '.to_sql' functionality: install via: 'pip install sqlalchemy-access'
# Pyodbc

# Issues with Numpy in Pycharm - copied sqlite3.dll in the 'C:\Users\KSherrill\.conda\envs\py39_sqlAlchemy\Library\bin' folder to 'C:\Users\KSherrill\.conda\envs\py39_sqlAlchemy\DLLs' - resolved the issue.
# Or uninstall Numpy and reinstall: Uninstall: pip3 uninstall numpy    Reinstall: pip3 install numpy
#Conda environment - py38

# Created by:  Kirk Sherrill - Data Manager Rock Mountain Network - I&M National Park Service
# Date Created: May 2st, 2023

#######################################
## Below are paths which are hard coded
#######################################
#Import Required Libraries
import os
import traceback
import pyodbc
import numpy as np
import pandas as pd
import sys
from datetime import date
import sqlalchemy as sa

##################################

###################################################
# Start of Parameters requiring set up.
###################################################
#Define Inpurt Parameters
inputFile = r'C:\ROMN\Monitoring\Soils\DataGathering\2022\VCSS\Report 20223S257 to2 023297_EDDPreprocessed.xlsx'  # Excel EDD from CSU Soils lab
rawDataSheet = "RawData"  # Name of the Raw Data Sheet in the inputFile

#Soils Access Database location
soilsDB = r'C:\ROMN\Monitoring\Soils\Certified\Soil_ROMN_AllYears_MASTER_20230502.accdb'
#Soils Dataset Table in Soils database  - this is the table data will be append to
soilsDatasetTable = "tbl_SoilChemistry_Dataset"

#Directory Information
workspace = r'C:\ROMN\Monitoring\Soils\DataGathering\2022\workspace'  # Workspace Folder


#Start of EDD Specific Content

firstColumn = 3    #Variable defines the column number with data.  EDD in 2022 first two columns were null (i.e. column three is where the tables started

noDataValue = "*"  #Variable defines the lab value being used to denote no data (EDD 2022 this was "*"). Records with this value will be dropped in the Stacked output

#Define Table One in EDD
tableOneFirstLabID = '2023S249'  #Define the First 'Lab#' id in EDD Table One to facilitate selection of records to be retained - Bulk Density table 2022 EDD
tableOneNumberRecords = 24  #Number of total records in table One of EDD
#Table with Bulk Density Table One in EDD
fieldCrossWalk1 = ['Lab ID', 'Sample ID', 'Bulk Density (g/cm)']

#Define Table Two in EDD
tableTwoFirstLabID = '2023S257' #Define the First 'Lab#' id in EDD Table Two to facilitate selection of records to be retained - Second/Third 2022 EDD
tableTwoNumberRecords = 25  #Number of total records in table One of EDD
#Table Two in EDD Fields  #Defining P, S, K, Ca, Mg, Na, Zn, Fe, Mn, Cu and B with (ppm) suffix for uniqueness in the 'tlu_NameUnitCrossWalk' table
fieldCrossWalk2 = ['Lab ID', 'Sample ID', 'pH 1:1', 'EC 1:1', 'OM (%)', 'NO3- (ppm)', 'NH4+ (ppm)', 'P (ppm)',
                   'S (ppm)', 'K (ppm)', 'Ca (ppm)', 'Mg (ppm)', 'Na (ppm)', 'CEC', 'Zn (ppm)', 'Fe (ppm)', 'Mn (ppm)', 'Cu (ppm)', 'B (ppm)']

#Define Table Three in EDD
tableThreeFirstLabID = '2023S257' #Define the First 'Lab#' id in EDD Table Two to facilitate selection of records to be retained - Second/Third 2022 EDD
tableThreeNumberRecords = 25  #Number of total records in table One of EDD
#Table Two in EDD Fields - Added (%) suffix to H, K, Ca, Mg, and Na parameters
fieldCrossWalk3 = ['Lab ID','Sample ID','TC (%)','TN (%)','Sand (%)','Clay (%)','Silt (%)','Texture Class','H (%)','K (%)','Ca (%)','Mg (%)','Na (%)']

bulkDensityTable_Suffix_Remove = "_BD"   #Variable defines the bulk density suffix to be replace by the 'bulkDensityTable_Suffix_Harmonize' variable (in 2022 '_BD' was replace by '_CM'
bulkDensityTable_Suffix_Harmonize = "_CM"  #Suffix varible replacing the bulDensityTable_Suffix_Remove' parameter for the Bulk Density Table

#Get Current Date
dateString = date.today().strftime("%Y%m%d")

# Define Output Name for log file
outName = "Soils_CSU_FieldSeason_2022_Preprocessed_" + dateString  # Name given to the exported pre-processed

#Logifile name
logFileName = workspace + "\\" + outName + "_logfile.txt"

# Checking for directories and create Logfile
##################################

if os.path.exists(workspace):
    pass
else:
    os.makedirs(workspace)

# Check for logfile

if os.path.exists(logFileName):
    pass
else:
    logFile = open(logFileName, "w")  # Creating index file if it doesn't exist
    logFile.close()
#################################################
##

def main():
    try:

        # List to hold all the processed dataframes
        datasetList = []
        crossWalkList = []
        #####################
        #Process the Raw Data - Define Data Frame EDD Table One
        #####################

        rawDataDf = pd.read_excel(inputFile, sheet_name=rawDataSheet)

        # Define the first dataframe column with data
        firstColumn_1 = firstColumn - 1
        # Find Record Index values with the firstLabID  - This will be used to subset datasets one and two
        indexDf = rawDataDf[rawDataDf.iloc[:, firstColumn_1] == tableOneFirstLabID]

        # Define first Index Value  - This is the
        indexFirst = indexDf.index.values[0]

        # Remove False Header Rows
        rawDataDfOneNoHeader = rawDataDf.iloc[indexFirst:, 2:]

        # Get far right column count based on number of fields in 'fieldCrossWalk1')
        lenColumnTableOne = len(fieldCrossWalk1)

        # Table One EDD Dataframe without header
        dfOneTrimmed_wHeader = rawDataDfOneNoHeader.iloc[0:tableOneNumberRecords, :lenColumnTableOne]

        # Add Header to DataFrame - this is the Data Frame One
        dfOneTrimmed_wHeader.columns = fieldCrossWalk1

        #Bulk Density Sample ID had a '_BD' suffix' changing to '_CM" suffic which was used for tables two and three.
        dfOneTrimmed_wHeader["Sample ID"] = dfOneTrimmed_wHeader["Sample ID"].apply(lambda x: x.replace(bulkDensityTable_Suffix_Remove, bulkDensityTable_Suffix_Harmonize))

        datasetList.append(dfOneTrimmed_wHeader)
        crossWalkList.append(fieldCrossWalk1)
        ########################################
        #Subset Directly Below the First Dataset - Define Data Frame EDD Table Two
        ########################################

        #Create Root DF for EDD table two working off rawDataDfOneNoHeader prior to trim.
        rawDataDfBelowOne = rawDataDfOneNoHeader[tableOneNumberRecords:]

        del(rawDataDfOneNoHeader)

        # Reset Index
        rawDataDfBelowOne.reset_index(drop=True, inplace=True)

        # Find Record Index values with the firstLabID  - This will be used to subset datasets two/three - add an additional column
        indexDf2 = rawDataDfBelowOne[rawDataDfBelowOne.iloc[:, 0] == tableTwoFirstLabID]

        # Define first Index Value  - This is the record 1 in dataset 2
        indexFirst2 = indexDf2.index.values[0]

        # Remove Header Rows
        rawDataDfTwoNoHeader = rawDataDfBelowOne[indexFirst2:]

        # Subset to the number of records in table one (i.e. Trimmed)
        rawDataDfTwoNoHeaderTrimmed = rawDataDfTwoNoHeader[0:tableTwoNumberRecords]

        # Get far right column count based on number of fields in 'fieldCrossWalk1')
        lenColumnTableTwo = len(fieldCrossWalk2)

        # Table Two EDD Dataframe without header
        dfTwoTrimmed_wHeader = rawDataDfTwoNoHeaderTrimmed.iloc[0:tableTwoNumberRecords, 0:]
        # Add Header to DataFrame - this is the Data Frame Two
        dfTwoTrimmed_wHeader.columns = fieldCrossWalk2

        datasetList.append(dfTwoTrimmed_wHeader)
        crossWalkList.append(fieldCrossWalk2)

        ########################################
        # Subset Directly Below the Second Dataset - Define Data Frame EDD Table Three
        ########################################

        # Create Root DF for EDD table three working off rawDataDFtwo prior to trim.
        rawDataDfBelowTwo = rawDataDfTwoNoHeader.iloc[tableTwoNumberRecords:, 0:]

        # Reset Index
        rawDataDfBelowTwo.reset_index(drop=True, inplace=True)
        del (rawDataDfTwoNoHeader)

        # Find Record Index values with the firstLabID  - This will be used to subset datasets two/three - add an additional column
        indexDf3 = rawDataDfBelowTwo[rawDataDfBelowTwo.iloc[:, 0] == tableThreeFirstLabID]

        # Define first Index Value  - This is the record 1 in dataset 2
        indexFirst3 = indexDf3.index.values[0]

        # Remove Header Rows
        rawDataDfThreeNoHeader = rawDataDfBelowTwo[indexFirst3:]

        # Subset to the number of records in table one (i.e. Trimmed)
        rawDataDfThreeNoHeaderTrimmed = rawDataDfThreeNoHeader[0:tableThreeNumberRecords]

        # Get far right column count based on number of fields in 'fieldCrossWalk1')
        lenColumnTableThree = len(fieldCrossWalk3)

        # Table Three EDD Dataframe without header
        dfThreeTrimmed_wHeader = rawDataDfThreeNoHeaderTrimmed.iloc[0:tableThreeNumberRecords, 0:lenColumnTableThree]
        # Add Header to DataFrame - this is the Data Frame Three
        dfThreeTrimmed_wHeader.columns = fieldCrossWalk3

        datasetList.append(dfThreeTrimmed_wHeader)
        crossWalkList.append(fieldCrossWalk3)

        
        ###############################
        # Get Metadata for all Events - Must Check WEI and VCSS metadata
        ##############################
        ####################################################
        # Get distinct dataframe Lab and ROMN Sample Numbers
        # Get Unique Dataframe with Lab and ROMN sample combinations - likely not necessary but insuring uniqueness
        df_unique = dfTwoTrimmed_wHeader[['Lab ID', 'Sample ID']]
        df_uniqueGB = df_unique.groupby(['Lab ID', 'Sample ID'], as_index=False).count()
        df_uniqueGB['EventName'] = 'TBD'
        df_uniqueGB['SiteName'] = 'TBD'
        df_uniqueGB['StartDate'] = pd.NaT
        df_uniqueGB['DateNum'] = 'TBD'
        df_uniqueGB['YearSample'] = None

        # Define SiteName
        df_uniqueGB['SiteName'] = df_uniqueGB['Sample ID'].str[:8]


        # Define EventName all prior to the third '_' - logical will not work for WEI - Only being used for VCSS
        df_uniqueGB['EventName'] = ['_'.join(x.split('_')[:3]) for x in df_uniqueGB['Sample ID']]
        # Define DateNum
        df_uniqueGB['DateNum'] = ['_'.join(x.split('_')[2:3]) for x in df_uniqueGB['EventName']]

        # Find metadata Information - VCSS DB - Join on Site Name prefix in 'SampleName_ROMN' and by year being processed
        outVal = defineMetadata_VCSS(df_uniqueGB)
        if outVal[0].lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function exportToDataset - " + str(messageTime) + " - Failed - Exiting Script")
            exit()
        else:
            # Return datafdrame with VCSS Sites defined
            df_wVCSS_noWEI = outVal[1]
            messageTime = timeFun()
            scriptMsg = ("Success - Function 'defineMetadata_VCSS' - " + messageTime)
            print(scriptMsg)
            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")

        # Find metadata Information - WEI DB
        outVal = defineMetadata_WEI(df_wVCSS_noWEI)
        if outVal[0].lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function 'defineMetadata_WEI' - " + str(messageTime) + " - Failed - Exiting Script")
            exit()
        else:
            # Return datafdrame with VCSS Sites defined
            df_wVCSS_wWEI = outVal[1]
            messageTime = timeFun()
            scriptMsg = ("Success - Function 'defineMetadata_WEI' - " + messageTime)
            print(scriptMsg)
            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")
            del (df_wVCSS_noWEI)

        # Check if output metadata dataframe has undefined 'Events'
        df_noEvent = df_wVCSS_wWEI.loc[df_wVCSS_wWEI['EventName'] == 'TBD']
        # Undefined Events
        recCountNoEvent = df_noEvent.shape[0]
        if recCountNoEvent > 0:
            messageTime = timeFun()
            scriptMsg = "WARNING - there are: " + str(
                recCountNoEvent) + " records with Undefined Events - Exiting Script - " + messageTime
            print(scriptMsg)
            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")
            traceback.print_exc(file=sys.stdout)
            logFile.close()
            print("Printing dataframe 'df_noEvent' with the undefined events:")
            print(df_noEvent)
            exit()

        ################################################################################
        #Join Metadata to the EDD Table Dataframes - this includes the Append Processing
        ################################################################################

        outVal = joinMetadataToDataframes(df_wVCSS_wWEI, datasetList, crossWalkList)
        if outVal.lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function joinMetadataToDataframes - " + str(messageTime) + " - Failed - Exiting Script")
            exit()
        else:

            messageTime = timeFun()
            scriptMsg = ("Success - Function 'joinMetadataToDataframes' - " + messageTime)
            print(scriptMsg)
            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")

        messageTime = timeFun()
        scriptMsg = ("Successfully Finished Processing EDD: " + inputFile + " to the Soils Database - " + messageTime)
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")

    except:

        messageTime = timeFun()
        scriptMsg = "Soils_ETL_To_SoilsDB.py - " + messageTime
        print (scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        traceback.print_exc(file=sys.stdout)
        logFile.close()


# Function to Get the Date/Time
def timeFun():
    from datetime import datetime
    b = datetime.now()
    messageTime = b.isoformat()
    return messageTime


#Rouitne to append the final dataframe (df_ToAppendFinal2) to the Soils DB
def apppendDataframesToSoilDB(df_ToAppendFinal2, datasetLoopCount):
    try:

        ###################################
        # Append df_ToAppendFinal to Dataset - appending one record at a time - unable to get one append for full dataset to work
        ###################################
        connStr = (r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + soilsDB + ";ExtendedAnsiSQL=1;")  # sqlAlchemy-access connection
        # cnxn = pyodbc.connect(connStr)  #PYODBC Connection
        cnxn = sa.engine.URL.create("access+pyodbc", query={"odbc_connect": connStr})
        engine = sa.create_engine(cnxn)

        # Create iteration range for records to be appended
        shapeDf = df_ToAppendFinal2.shape
        lenRows = shapeDf[0]
        rowRange = range(0, lenRows)

        try:
            loopCount = 0
            for row in rowRange:
                df3 = df_ToAppendFinal2[row:row + 1]
                recordIdSeries = df3.iloc[0]
                recordId = recordIdSeries.get('EventName')
                parameterRaw = recordIdSeries.get('ParameterRaw')
                try:
                    appendOut = df3.to_sql(soilsDatasetTable, con=engine, if_exists='append')
                    print(appendOut)
                    messageTime = timeFun()
                    scriptMsg = "Successfully Appended RecordID - " + recordId + " - Parameter - " + parameterRaw + " - for EDD Dataset: " + str(datasetLoopCount) + " - " + messageTime
                    print(scriptMsg)
                    logFile = open(logFileName, "a")
                    logFile.write(scriptMsg + "\n")
                    logFile.close()

                except:
                    messageTime = timeFun()
                    scriptMsg = "WARNING Failed to Appended RecordID - " + recordId + " - Parameter - " + parameterRaw + " - for EDD Dataset: " + str(datasetLoopCount) + " - " + messageTime
                    print(scriptMsg)
                    logFile = open(logFileName, "a")
                    logFile.write(scriptMsg + "\n")
                    logFile.close()

                loopCount += 1
        except:
            messageTime = timeFun()
            scriptMsg = "WARNING Failed to Append RecordID - " + recordId + " - " + parameterRaw + " - for Dataset: " + str(
                loopCount) + " - " + messageTime
            print(scriptMsg)
            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")
            logFile.close()

        return "success function"

    except:
        return "function failed"
        messageTime = timeFun()
        scriptMsg = "Error 'apppendDataframesToSoilDB' - " + messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")

        traceback.print_exc(file=sys.stdout)
        logFile.close()


#Routine to Join the metadata Data Frame to EDD Dataframes, and prep final dataframe to be appended and Append the DataFrame.
def joinMetadataToDataframes(df_wVCSS_wWEI, datasetList, crossWalkList):
    try:
        ##########################################
        # Join metadata dataframe 'df_wVCSS_wWEI' with data dataframes (i.e. df_FirstDataset and df_SecondDataset) and append to Soils Dataset Table
        ##########################################
        loopCount = 0
        for dataset in datasetList:

            # Define list of fields to be stacked
            fieldCrossWalkToStack = crossWalkList[loopCount]

            ################################################
            # Define Field List to be Stacked via pandas melt
            fieldCrossWalkToStack.remove("Lab ID")
            fieldCrossWalkToStack.remove("Sample ID")
            # Create Stacked Data Frame
            df_melt = pd.melt(dataset, id_vars="Sample ID", var_name="ParameterRaw",
                              value_vars=fieldCrossWalkToStack, value_name="Value")

            # Remove Records with null value in 'df_melt
            df_melt2 = df_melt.dropna(subset=['Value'])
            df_melt2.reset_index(drop=True, inplace=True)
            del (df_melt)
            #################################################

            # Join (via merge) stacked output (i.e. 'df_melt') with the metadata dataframe
            df_stack_wMetadata = pd.merge(df_melt2, df_wVCSS_wWEI, how='left', left_on='Sample ID',
                                          right_on='SampleName_ROMN', suffixes=("_data", "_metadata"))

            # Subset to the desire fields to be append to 'tbl_SoilChemistry_Dataset'
            df_ToAppend = df_stack_wMetadata[
                ["Protocol_ROMN", "SiteName", "EventName", "StartDate", "ParameterRaw", "Value"]]
            del (df_stack_wMetadata)

            # Add Year Sampled Field
            df_ToAppend.insert(4, 'YearSampled', None)
            # Define Year Sampled
            df_ToAppend['YearSampled'] = df_ToAppend['StartDate'].dt.strftime('%Y')

            # Format Start Year to 'm/d/yyyy' as Date Time
            # df_ToAppend['StartDate'] = df_ToAppend['StartDate'].dt.strftime('%m/%d/%Y')
            df_ToAppend['StartDate'] = pd.to_datetime(df_ToAppend['StartDate'], format='%m/%d/%Y')

            ########################################################################################
            # Verify fields in dataset have been defined in the 'tlu_NameUnitCrossWalk' lookup table - pass the Stacked Dataframe
            outVal = checkFieldNameCrossWalk(df_ToAppend)
            if outVal[0].lower() != "success function":
                messageTime = timeFun()
                print("WARNING - Function 'checkFieldNameCrossWalk' - " + str(
                    messageTime) + " - Failed - loopCount:" + str(loopCount) + " - Exiting Script")
                exit()
            else:
                # Return datafdrame with fieldCrosswalk defined
                df_wFieldCrossWalk = outVal[1]
                messageTime = timeFun()
                scriptMsg = ("Success - Function 'checkFieldNameCrossWalk' - looCount: " + str(
                    loopCount) + " - " + messageTime)
                print(scriptMsg)
            ######################################################################################

            # Join the Parameter Name and Unit fields (i.e. UnitRaw, ParameterDataset and UnitDataset) dataframe (i.e. df_wFieldCrossWalk) with the 'df_ToAppend' dataframe
            # Join (via merge) stacked output (i.e. 'df_melt') with the metadata dataframe
            df_ToAppend_wLookup = pd.merge(df_ToAppend, df_wFieldCrossWalk, how='left', left_on='ParameterRaw',
                                           right_on='ParameterRaw', suffixes=("_data", "_lookup"))

            # Cleanup 'df_ToAppend_wLookup' to frame for Append - Match fields in tbl_SoilChemistry_Dataset
            # Return Dataframe with the Lookup fields

            df_ToAppendFinal = df_ToAppend_wLookup[
                ["Protocol_ROMN", "SiteName", "EventName", "StartDate", "YearSampled", "ParameterRaw", "UnitRaw",
                 "ParameterDataset", "UnitDataset", "Value"]]  # With StartDate

            # Check for Records without a matching Eventname
            # Subset to only Records with Data
            df_noEventName = df_ToAppendFinal[df_ToAppendFinal['EventName'].isna()]

            rowCount = df_noEventName.shape[0]
            if rowCount > 0:  # No EventName defined

                messageTime = timeFun()
                scriptMsg = (
                        "WARNING - Records don't have an EVENTNAME Defined in - existing script - " + messageTime)
                print(scriptMsg)
                scriptMsg = (
                            "Printing Dataframe 'df_noEventName' with the Recordings that are missing an EventName - " + messageTime)
                print(scriptMsg)
                print(df_noEventName)

                logFile = open(logFileName, "a")
                logFile.write(scriptMsg + "\n")

                # Looper through 'df_noCrossWalk' to print pramaters missing in 'tlu_NameUnitCrossWalk'
                df_noEventName.reset_index()
                for index, row in df_noEventName.iterrows():
                    scriptMsg = ('WARNING - Record: ' + str(row['ParameterRaw']) + " with value: " + str(
                        row['Value']) + " - doesn't have a defined EventName")
                    print(scriptMsg)
                    logFile.write(scriptMsg + "\n")

                logFile.close()
                exit()

            #Add Fields to the stacked Dataframe
            # Add Field - QC_Status
            df_ToAppendFinal.insert(9, 'QC_Status', 0)

            # Add Field - QC_Flag
            df_ToAppendFinal.insert(10, 'QC_Flag', "")

            # Add Field - QC_Notes
            df_ToAppendFinal.insert(11, 'QC_Notes', "")

            # Add Field - DataFlag
            df_ToAppendFinal.insert(12, 'DataFlag', "Null")

            # Add Field - Count
            df_ToAppendFinal.insert(13, 'Count', 1)

            # Add Field - StDev - All records are from one sample
            df_ToAppendFinal.insert(14, 'StDev', -999)

            # Add Field - StErr
            df_ToAppendFinal.insert(15, 'STErr', -999)

            # Add Field - Min
            # df_ToAppendFinal.insert(14, 'Min', df_ToAppendFinal["Value"])
            # If Lime, Texture or Peat - set Min and Max to -999 - categorical
            inStr = ("Lime", "Texture", "Peat")
            df_ToAppendFinal["Min"] = np.where(df_ToAppendFinal["ParameterRaw"].str.startswith(inStr), -999,
                                               df_ToAppendFinal["Value"])

            # Add Field - Max
            # df_ToAppendFinal.insert(15, 'Max', df_ToAppendFinal["Value"])
            df_ToAppendFinal["Max"] = np.where(df_ToAppendFinal["ParameterRaw"].str.startswith(inStr), -999,
                                               df_ToAppendFinal["Value"])

            # Convert Value field to text
            df_ToAppendFinal['Value'] = df_ToAppendFinal['Value'].apply(str)

            # Convert 'YearSampled' to Integer
            df_ToAppendFinal["YearSampled"] = pd.to_numeric(df_ToAppendFinal["YearSampled"], downcast="integer")

            # Assigned noDataValue to 'ND'
            df_ToAppendFinal["Value"] = df_ToAppendFinal["Value"].apply(lambda x: x.replace(noDataValue, "ND"))

            df_ToAppendFinal["Value"] = df_ToAppendFinal["Value"].str.replace(" ", "")
            # Remove fields with the No Data Value
            df_ToAppendFinal2 = df_ToAppendFinal.loc[(df_ToAppendFinal['Value'] != 'ND')]

            # Set Index field to the 'SiteName' field - will not be able to append to Soils dataset if Index column is present - SiteName is not unique but not relevant in this context
            df_ToAppendFinal2.set_index("SiteName", inplace=True)

            # Append Final Dataframe to Soils DB
            outVal = apppendDataframesToSoilDB(df_ToAppendFinal2, loopCount)
            if outVal.lower() != "success function":
                messageTime = timeFun()
                print(
                    "WARNING - Function apppendDataframesToSoilDB - " + str(
                        messageTime) + " - Failed - Exiting Script")
                exit()
            else:
                messageTime = timeFun()
                scriptMsg = ("Success - Function 'apppendDataframesToSoilDB' - for Dataset Loop Count:" + str (loopCount) + " - " + messageTime)
                print(scriptMsg)
                logFile = open(logFileName, "a")
                logFile.write(scriptMsg + "\n")

            loopCount += 1
        return "success function"


    except:
        return "Script Failed"
        messageTime = timeFun()
        scriptMsg = "Error joinMetadataToDataframes - " + messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")

        traceback.print_exc(file=sys.stdout)
        logFile.close()

#Function Check that parameter is defined in the 'tlu_NameUnitCrossWalk' table
def checkFieldNameCrossWalk(inDf):

    try:

        #Impor the 'tlu_NameUnitCrossWalk' table
        inQuery = "SELECT tlu_NameUnitCrossWalk.* FROM tlu_NameUnitCrossWalk;"
        outVal = connect_to_AcessDB(inQuery, soilsDB)
        if outVal[0].lower()!= "success function":
            messageTime = timeFun()
            print("WARNING - Function connect_to_AcessDB - " + messageTime + " - Failed - Exiting Script")
            exit()
        else:

            #Evalute if the 'col' variable is defined in the 'ParameterNative' field
            outDfCrossWalk = outVal[1]

            #Group By on input dataframe (i.e. stacked output') on the 'ParameterRaw' field
            inDfGB = inDf.groupby(['ParameterRaw'], axis=0, as_index=False).count()

            # Join (via merge) 'outDfCurYear' (i.e. current year events) on SiteName field to 'df_uniqueGB' (i.e. the input dataset with records.
            df_mergeCWDfGB = pd.merge(inDfGB, outDfCrossWalk, how='left', left_on='ParameterRaw', right_on='ParameterNative', suffixes=("_data", "_lookup"))

            #Identify Records without a 'ParameterRaw_lookup' value
            # Subset to only Records with Data
            df_noCrossWalk = df_mergeCWDfGB[df_mergeCWDfGB['ParameterNative'].isna()]

            rowCount = df_noCrossWalk.shape[0]
            if rowCount > 0: #No Cross-walk defined

                messageTime = timeFun()
                scriptMsg = ("WARNING - Parameters are undefined in 'tlu_NameUnitCrossWalk' please define and reprocess - " + messageTime)
                print(scriptMsg)
                scriptMsg = ("Printing Dataframe 'df_noCrossWalk' with the Parameters without a defiend value in 'tlu_NameUnitCrossWalk' please define in this table and reprocess - " + messageTime)
                print(scriptMsg)
                print (df_noCrossWalk)

                logFile = open(logFileName, "a")
                logFile.write(scriptMsg + "\n")

                #Looper through 'df_noCrossWalk' to print pramaters missing in 'tlu_NameUnitCrossWalk'
                df_noCrossWalk.reset_index()
                for index, row in df_noCrossWalk.iterrows():
                    scriptMsg = ('WARNING - Parameter: ' + row['ParameterRaw'] + " is not defined in table 'tlu_NameUnitCrossWalk")
                    print (scriptMsg)
                    logFile.write(scriptMsg + "\n")

                logFile.close()
                exit()

            else:

                print(df_noCrossWalk)

                #Return Dataframe with the Lookup fields
                df_lookupFields = df_mergeCWDfGB[["ParameterRaw", "UnitNative", "ParameterDataset", "UnitDataset"]]

                #Rename fields:
                outFieldList = ["ParameterRaw", "UnitRaw", "ParameterDataset", "UnitDataset"]
                df_lookupFields.columns = outFieldList

                messageTime = timeFun()
                scriptMsg = ("Success - Function 'checkFieldNameCrossWalk - " + messageTime)
                print(scriptMsg)
                logFile = open(logFileName, "a")
                logFile.write(scriptMsg + "\n")
                logFile.close()

        return "success function", df_lookupFields

    except:
        messageTime = timeFun()
        scriptMsg = "Error checkFieldNameCrossWalk - " + messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")

        traceback.print_exc(file=sys.stdout)
        logFile.close()

#Connect to Access DB and perform defined query - return query in a dataframe - Using PYODBC issues with SQL Alchemy Access
def connect_to_AcessDB(query, inDB):

    try:
        # PyODBC - Connection Commented Out
        connStr = (r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + inDB + ";")
        cnxn = pyodbc.connect(connStr)
        dataf = pd.read_sql(query, cnxn)
        cnxn.close()

        #SQL Alchemy Connection
        # connStr = (r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + soilsDB + ";ExtendedAnsiSQL=1;")
        # cnxn = sa.engine.URL.create("access+pyodbc", query={"odbc_connect": connStr})
        # engine = sa.create_engine(cnxn)
        # dataf = pd.read_sql(query, cnxn)
        # del (engine)
        # del (cnxn)

        return "success function", dataf

    except:
        messageTime = timeFun()
        scriptMsg = "Error function:  connect_to_AcessDB - " +  messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")

        traceback.print_exc(file=sys.stdout)
        logFile.close()
        return "failed function"



#Define VCSS Event Metadata via join on SiteName and filtered to only the 'FieldYear' events - assumming a singular year of processing.
def defineMetadata_VCSS(df_uniqueGB):

    try:

        #Pull the event table from the VCSS table via the Soils DB
        inQuery = "SELECT tbl_Events1.* FROM tbl_Events1;"
        outVal = connect_to_AcessDB(inQuery, soilsDB)
        if outVal[0].lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function connect_to_AcessDB - " + messageTime + " - Failed - Exiting Script")
            exit()
        else:
            #VCSS events dataframe
            outDf = outVal[1]

            #Define Year field
            outDf['Year'] = pd.DatetimeIndex(outDf['StartDate']).year



            #Join (via merge on Site Name and DateNum for VCSS
            df_mergeVCSS = pd.merge(df_uniqueGB, outDf, how='left', left_on=['SiteName','DateNum'], right_on=['SiteName','DateNum'], suffixes=("_data", "_metadata"))

            #Return new dataframe
            df_wVCSS_noWEI = df_mergeVCSS[["Lab ID", "Sample ID", "EventName_data", "SiteName", "StartDate_metadata", "Year"]]

            #Rename fields:
            fiedList_VCSS = ["SampleName_Lab", "SampleName_ROMN", "EventName","SiteName", "StartDate","YearSample"]
            df_wVCSS_noWEI.columns = fiedList_VCSS

            #Add 'Protocol_ROMN' field - default to 'VCSS'
            df_wVCSS_noWEI.insert(0, 'Protocol_ROMN', "VCSS")

            #Update EventNames field for Records that aren't VCSS (i.e. WEI) or with no matched to 'TBD' -
            df_wVCSS_noWEI['EventName'] = np.where((df_wVCSS_noWEI['YearSample'].isnull()),"TBD", df_wVCSS_noWEI['EventName'])

            # Update Protocol_ROMN field for Records that aren't VCSS (i.e. WEI) or with no matched to 'TBD' -
            df_wVCSS_noWEI['Protocol_ROMN'] = np.where((df_wVCSS_noWEI['YearSample'].isnull()), "TBD", df_wVCSS_noWEI['Protocol_ROMN'])

            return "success function", df_wVCSS_noWEI

    except:
        messageTime = timeFun()
        scriptMsg = "Error function:  defineMetadata_VCSS - " + messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")

        traceback.print_exc(file=sys.stdout)
        logFile.close()
        return "failed function", "Null"

#Define WEI Event Metadata -  - assumming a singular year of processing.
def defineMetadata_WEI(inDf):

    try:
        #Pull the event table from the VCSS table via the Soils DB
        inQuery = "SELECT tbl_Events.EventName, tbl_Events.StartDate, tbl_Soil.Chem, tbl_Soil.Comments_Soil, tbl_Soil.Comments_Sample FROM tbl_Events INNER JOIN tbl_Soil ON tbl_Events.EventName = tbl_Soil.EventName;"
        outVal = connect_to_AcessDB(inQuery, soilsDB)
        if outVal[0].lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function connect_to_AcessDB - " + messageTime + " - Failed - Exiting Script")
            exit()
        else:
            #VCSS events dataframe
            outDf = outVal[1]
            #Join (via merge) 'outDfCurYear' (i.e. current year events) on SiteName field to 'df_uniqueGB' (i.e. the input dataset with records.
            df_mergeWEI = pd.merge(inDf, outDf, how = 'left', left_on='SampleName_ROMN', right_on='Chem', suffixes= ("_data", "_metadata"))

            # Populate the 'Protocol_ROMN' field with 'WEI'' values where join match with WEI
            df_mergeWEI['Protocol_ROMN'] = np.where((df_mergeWEI['EventName_metadata'].isnull()), df_mergeWEI['Protocol_ROMN'], "WEI")

            #Populate the 'EventName_data' field with the 'EventName_metadata' field values where join match with WEI
            df_mergeWEI['EventName_data'] = np.where((df_mergeWEI['EventName_metadata'].isnull()), df_mergeWEI['EventName_data'], df_mergeWEI['EventName_metadata'])

            #Populate the 'StartDate_data' field with the 'StartDate_metadata' field values where join match with WEI
            df_mergeWEI['StartDate_data'] = np.where((df_mergeWEI['EventName_metadata'].isnull()), df_mergeWEI['StartDate_data'], df_mergeWEI['StartDate_metadata'])

            #Return new dataframe
            df_wVCSS_wWEI = df_mergeWEI[["Protocol_ROMN","SampleName_Lab", "SampleName_ROMN", "EventName_data","SiteName", "StartDate_data"]]

            #Rename fields:
            fiedList_WEI = ["Protocol_ROMN", "SampleName_Lab", "SampleName_ROMN", "EventName","SiteName", "StartDate"]
            df_wVCSS_wWEI.columns = fiedList_WEI

            return "success function", df_wVCSS_wWEI

    except:
        messageTime = timeFun()
        scriptMsg = "Error function:  defineMetadata_WEI - " + messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")

        traceback.print_exc(file=sys.stdout)
        logFile.close()
        return "failed function", "Null"

if __name__ == '__main__':

    # Write parameters to log file ---------------------------------------------
    ##################################
    # Checking for working directories
    ##################################

    if os.path.exists(workspace):
        pass
    else:
        os.makedirs(workspace)

    #Check for logfile

    if os.path.exists(logFileName):
        pass
    else:
        logFile = open(logFileName, "w")    #Creating index file if it doesn't exist
        logFile.close()

    # Analyses routine ---------------------------------------------------------
    main()
