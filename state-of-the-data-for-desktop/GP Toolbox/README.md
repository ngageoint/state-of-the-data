# state-of-the-data-for-desktop
State of the Data for Desktop is a set of ArcGIS Desktop Geoprocessing tools that can be used from ArcMap or ArcGIS Pro to process the State of the Data metrics for feature classes, feature layers, and shapefiles. There are two separate toolboxes. The **State of the Data.tbx** is intended for creating new State of the Data assessments. The **State of the Data Enterprise.tbx** is intened to work with data that can be updated withing an enterprise geodatabase.

## State of the Data Toolbox
This toolbox contains all current State of the Data analysis scripts as ArcGIS geoprocessing tools.
![](http://nga.maps.arcgis.com/sharing/rest/content/items/b01f07bc755f480ab4052c64484b40fd/data?style=centerme)

## State of the Data Enterprise Toolbox
This toolbox is a subset of the tools in the State of the Data toolbox that have been designed to be run with feature services or enterprise geodatabase features and will output existing content suitability indicators. 
![](http://nga.maps.arcgis.com/sharing/rest/content/items/40d970a508df481d97d846266ef5f21e/data?style=centerme)

## Metrics Explained
### Attribution Assessment
- Identifies attribution deficiencies at the feature level for user defined attributes
### Attribution Assessment (TDS 6.1 Schema)
- Identifies attribution deficiencies at the feature level based on mission or data requirements
### Completeness
- Identifies areas which may have missing features
- Line: total line length; Point: feature count; Surface: area
### Logical Consistency
- Creates a statistical summary of the attribution assessment over user defined areas of interest
### Logical Consistency (TDS 6.1 Schema)
- Creates a statistical summary of the attribution assessment of TDS 6.1 data over user defined areas of interest
### Positional Accuracy
- Statistically summarizes the positional accuracy of a feature class based on feature level metadata
### Positional Offset
- Compares two corresponding line feature classes to:
  - Match similar features
  - Calculate their offset from each other
  - Summarize those results over a given area of interest
### Positional Offset (TDS 6.1 Schema)
- Compares two corresponding line feature classes (one of which is in the TDS 6.1 schema) to:
  - Match similar features
  - Calculate their offset from each other
  - Summarize those results over a given area of interest
### Source Lineage
- Statistically summarizes the source of features in a feature class given a selected source attribute field
- Source field could be editor name, organization, product, etc.
### Source Lineage (TDS 6.1 Schema)
- Statistically summarizes the source of features in the TDS 6.1 schema source given the values in the TDS source field
### Temporal Accuracy from Change Detection
- Identifies features that may require updating due to currency and detected change
### Temporal Currency
- Statistically summarizes the source date of features in a feature class given a selected source date attribute field
- Source date could be the last edit date, the feature creation date, the source data of the imagery from which the feature was derived from, etc.
### Temporal Currency (TDS 6.1 Schema)
- Statistically summarizes the source date of features in the TDS 6.1 schema given the values in the TDS source date field
### Temporal Currency Comparison
- Comapres two outputs from **Temporal** **Currency** and ranks cells in terms 
### Thematic Accuracy (TDS 6.1 Schema)
- Identifies areas where feature collection requirements properly correlate to population
### Thematic Summary
- Performs a statistical summary of a feature subtype or selected attribute
