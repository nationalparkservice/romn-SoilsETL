# romn-SoilsETL

The Github 'romn-SoilsETL' repository contains the python scripts (i.e. *ROMN_Soils_ETL_ToSoilsDB_gte2022.py* and *ROMN_Soils_ETL_To_SoilsDB_Pre2022.py*) which performs Extract Transform and Load (ETL) of the Colorado State University Soils Lab Electronic Data Deliverable (EDD) 
to the Rocky Mountain Network Soil Database - master soils dataseet: 'tbl_SoilChemistry_Dataset'. Soils ETL is used for Uplands Vegetation and Wetlands Ecological Integrigty protocol workflow processing at the NPS IMD Rocky Mountain Network.


## Updates 6/6/2025 (by Abby Volk)
Script updates: I added two new copies of the script to the GitHub repo. One is the most recent one for processing general samples. The second is rearranged to be run a little more interactively for non-detects and GLORIA samples (however, these portions of the code need to be uncommented-out and should be CLOSELY monitored to make sure they are doing what you expect). Also, REFER TO THE DM SOP (stored in the ROMN Team DataMangement/Soils) FOR ADDITIONAL NOTES AND CAVEATS.

-   ROMN_Soils_ETL_To_SoilsDB_2024.py -- general script

-   ROMN_Soils_ETL_To_SoilsDB_lessthansandGLORIA.py -- for less than detection limits and GLORIA


**Defines Matching Metadata for Uplands Vegetation (VCSS) and Wetlands events in the Soils database.**
The Uplands Vegetation and Wetlands event table must be linked to the most current databases in the Soils database as defined in the 'soilsDB' parameter.

**Defines the matching parameter name and units** as defined in the 'tlu_NameUnitCrossWalk' lookup table.

**Appends the transformed data (i.e. ETL)** to the Master Soils dataset 'tbl_SoilChemistry_Dataset' via the 'to_sql' functionality for dataframes in the [sqlAlchemy](https://pypi.org/project/sqlalchemy-access/) package. Install via pip install sqlalchemy-access in your python environment.

## ROMN_Soils_ETL_ToSoilsDB_gte2022.py

Extracts the soils EDD records for ROMN field season 2022 from the Colorado State University Soil, Water and Plant Testing laboratory post move from Fort Collins to Denver in 2022. This is the most current ETL script as of 5/3/2023

## ROMN_Soils_ETL_ToSoilsDB.py

Extracts the soils EDD records for ROMN field season 2021 from the Colorado State University Soil, Water and Plant Testing laboratory prior to the move from Fort Collins to Denver in 2022. This ETL route was used for field season 2021 soils data.
