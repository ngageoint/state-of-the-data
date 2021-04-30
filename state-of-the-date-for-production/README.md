# State of the Data Python Tools for Automation

## Table of Contents

* [Overview](#overview)
* [What does each script do?](#what-does-each-script-do?)
* [Running tools as batch files](#running-tools-as-batch-files)
* [Automating the process](#automating-the-process)

## Overview

This repository contains the SotD tools used for performing topographic data analysis. These
tools are designed to run in the background on a server as a schedule task.

## What does each script do?
### [generate_rtree.py](src/generate_rtree.py)
This tool generates an rtree spatial index file for a given feature class. 
The rtree contains the following fields: **'objectid', 'ZI026_CTUU', 
'ZI026_CTUC', 'ZI001_SDV', 'ZI001_SDP', 'ZI001_SPS', 'AHA', 'ZSAX_RS0', 
'ZSAX_RX0', 'SHAPE@'**. These locally cached rtree files are what get passed to the
[process_rtree.py](src/process_rtree.py) 
tool in order to generate the state of the data feature classes. The inputs to 
[generate_rtree.py](src/generate_rtree.py) are:
  - **workspace** - The geodatabase that contains the features to analyze.
  - **feature_arg** - The name of the feature class that you want to analyze.
  - **rtree_location** - The output location for the rtree index file.
  - **results_cells** (optional) - A feature class that contains only the 
  cells that you want to process.&nbsp;

### [generate_lc_rtree.py](src/generate_lc_rtree.py)
This tool generates an rtree spatial index file of the results of 
the **PSG** and **HADR** feature queries. The rtree contains
the following fields: **'OID@', 'SHAPE@','field_list', 'deficiency_cnt'**. These 
locally cached rtree files are what we pass to the 
[process_lc_rtree.py](src/process_lc_rtree.py) tool
in order to generate the logical consistency feature classes. The inputs to 
[generate_lc_rtree.py](src/generate_lc_rtree.py) are:
  - **workspace** - The geodatabase that contains the features to analyze.
  - **fc** - The name of the feature class that you want to analyze.
  - **idx_location** - The output location for the rtree index file.
  - **resource_srf** (optional) - A feature class that contains only 
  the cells that you want to process.&nbsp;

### [process_rtree.py](src/process_rtree.py)
This tool generates the state of the data result features classes: Classification, SourceLineage,
PositionalAccuracy, TemporalCurrency, TemporalAccuracy, FeatureDensity. The inputs to the tool are:
  - workspace - The geodatabase that contains the features to analyze.
  - features - The name of the feature class that you want to analyze.
  - idx_location - The output locaiton for the rtree index file.
  - out_location - The geodatabase where the results will be written. 
  - update_flag - Optional **True** or **False** value telling the tools 
  whether you are updating only specific cells.&nbsp;

### [process_lc_rtree.py](src/process_lc_rtree.py)
This tool generates the state of the data result features classes for LogicalConsistency. 
The inputs to the tool are:
  - workspace - The geodatabase that contains the features to analyze.
  - features - The name of the feature class that you want to analyze.
  - idx_location - The output location for the rtree index file.
  - out_location - The geodatabase where the results will be written. 
  - update_flag - Optional **True** or **False** value telling the tools 
  whether you are updating only specific cells.&nbsp;

### [querytools.py](src/querytools.py)
This tool queries the **ResourceSrf** features for the most recently updated cells. 
Those cells can then be used as the inputs to the **generate** and **process** tools 
in order to process only recently updated areas. The inputs to the tool are:
  - workspace = The geodatabase that contains the ResourceSrf features to analyze.
  - out_location = The geodatabase where the selected cells will be written.
  - look_back_days = Typically set to 90
  - output_fcname = Name of the output cells to be written to the *out_location*&nbsp;

## The Batch Files
This section presents a description of the various batch files. 
- **run_results_pipeline.bat**	- THis is the file that actually gets run by the 
ScheduledTaskPipeline.bat file. 

## Running tools as batch files
### Processing from scratch
A typical deployment file looks like the 
[run_results_pipeline.bat](src/run_results_batches_pipeline.bat) file. 
In this file, we follow the following pattern:
1. Generate rtree for a specfic feature class.
```python generate_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" HydroAidNavigationSrf C:\RTREE```
2. Process the rtree for that feature class.
```python process_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" HydroAidNavigationSrf C:\RTREE "C:\sde_connection_files\results_db_conn_as_data_owner.sde"```
3. Delete the rtree fles.
```
del C:\RTREE\HydroAidNavigationSrf.idx /f /q /s
del C:\RTREE\HydroAidNavigationSrf.d
```
4. Generate the logical consistency rtree for that feature class.
```python generate_lc_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" HydroAidNavigationSrf C:\RTREE```
5. Process the logical consistency rtree for that feature class.
```python process_lc_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" HydroAidNavigationSrf C:\RTREE "C:\sde_connection_files\results_db_conn_as_data_owner.sde"```
6. Delete the rtree files.
```del C:\RTREE\HydroAidNavigationSrf* /f /q /s```

### Updating existing features
When running the tools to update only features in the topographic data store 
database that have been updated, make the following changes to the pattern above:
1. Query the ResourceSrf feature class for the cells that have been updated.
```python querytools.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" "C:\sde_connection_files\results_db_conn_as_data_owner.sde" 90 cells_to_update```
2. Generate rtree for a specfic feature class.
```python generate_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" HydroAidNavigationSrf C:\RTREE C:\sde_connection_files\results_db_conn_as_data_owner.sde\sotd.sotd.cells_to_update```
3. Process the rtree for that feature class.
```python process_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" HydroAidNavigationSrf C:\RTREE "C:\sde_connection_files\results_db_conn_as_data_owner.sde" True```
4. Delete the rtree files.
```
del C:\RTREE\HydroAidNavigationSrf.idx /f /q /s
del C:\RTREE\HydroAidNavigationSrf.d
```
5. Generate the logical consistency rtree for that feature class.
```python generate_lc_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" HydroAidNavigationSrf C:\RTREE "C:\sde_connection_files\results_db_conn_as_data_owner.sde\sotd.sotd.cells_to_update"```
6. Process the logical consistency rtree for that feature class.
```python process_lc_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" HydroAidNavigationSrf C:\RTREE "C:\sde_connection_files\results_db_conn_as_data_owner.sde" True```
7. Delete the rtree files.
```del C:\RTREE\HydroAidNavigationSrf* /f /q /s```


## Automating the process
Running these tools has been automated in 
[ScheduledTaskPipeline.bat](src/ScheduledTaskPipeline.bat). 
This is designed to be hooked up to the Windows Task Scheduler. 
This batch file does the following:
1. Change the working directory.
```
echo Changing Directories
D:
cd "C:\indicators\src"
```
2. Activate the ArcGIS Pro Python environment.
```
ECHO Open and Activate ArcGIS Pro Environment
CALL "C:\Program Files\ArcGIS\Pro\bin\Python\Scripts\activate.bat"
CALL activate arcgispro-py3-171-rtree
```
3. Run the stte of the data Python tools.
```
echo Running the Python Pipeline
CALL "C:\indicators\src\run_results_pipeline.bat"
```

