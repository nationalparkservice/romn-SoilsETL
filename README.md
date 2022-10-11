## romn-SoilsETL

The Github 'romn-SoilsETL' repository contains the python script (i.e. 'ROMN_Soils_ETL_ToSoilsDB.py' which performs Extract Transform and Load (ETL) of the Colorado State University soils lab Electronic Data Deliverable (EDD) 
to the Rocky Mountain Network Soil Database - master soils dataseet: 'tbl_SoilChemistry_Dataset'. Soils ETL is used for Uplands Vegetation and Wetlands Ecological Integrigty protocol workflow processing at the NPS IMD Rocky Mountain Network.

Processing in script:*ROMN_Soils_ETL_ToSoilsDB.py*

**Extracts the Data records from the CSU Soils lab EDD.**

**Defines Matching Metadata for Uplands Vegetation (VCSS) and Wetlands events in the Soils database.**
The Uplands Vegetation and Wetlands event table must be linked to the most current databases in the Soils database as defined in the 'soilsDB' parameter.

**Defines the matching parameter name and units** as defined in the 'tlu_NameUnitCrossWalk' lookup table.

**Appends the transformed data (i.e. ETL)** to the Master Soils dataset 'tbl_SoilChemistry_Dataset' via the 'to_sql' functionality for dataframes in the [sqlAlchemy](https://pypi.org/project/sqlalchemy-access/) package. Install via pip install sqlalchemy-access in your python environment.
