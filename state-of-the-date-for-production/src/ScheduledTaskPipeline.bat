echo Changing Directories
C:
cd "C:\sotd\indicators\src"
ECHO Open and Activate ArcGIS Pro Environment
CALL "C:\Program Files\ArcGIS\Pro\bin\Python\Scripts\activate.bat"
CALL activate arcgispro-py3-171-rtree
echo Running the Python Pipeline
CALL "C:\sotd\indicators\src\run_sotd_pipeline.bat"