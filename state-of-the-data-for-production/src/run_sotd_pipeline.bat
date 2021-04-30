python generate_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" UtilityInfrastructurePnt D:\RTREE
python process_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" UtilityInfrastructurePnt D:\RTREE "C:\sde_connection_files\results_db_conn_as_data_owner.sde"
del D:\RTREE\UtilityInfrastructurePnt.idx /f /q /s
del D:\RTREE\UtilityInfrastructurePnt.dat /f /q /s
python generate_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" HydrographyCrv D:\RTREE
python process_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" HydrographyCrv D:\RTREE "C:\sde_connection_files\results_db_conn_as_data_owner.sde"
del D:\RTREE\HydrographyCrv.idx /f /q /s
del D:\RTREE\HydrographyCrv.dat /f /q /s
python generate_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" UtilityInfrastructureCrv D:\RTREE
python process_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" UtilityInfrastructureCrv D:\RTREE "C:\sde_connection_files\results_db_conn_as_data_owner.sde"
del D:\RTREE\UtilityInfrastructureCrv.idx /f /q /s
del D:\RTREE\UtilityInfrastructureCrv.dat /f /q /s
python generate_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" UtilityInfrastructureSrf D:\RTREE
python process_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" UtilityInfrastructureSrf D:\RTREE "C:\sde_connection_files\results_db_conn_as_data_owner.sde"
del D:\RTREE\UtilityInfrastructureSrf.idx /f /q /s
del D:\RTREE\UtilityInfrastructureSrf.dat /f /q /s
python generate_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" HydrographySrf D:\RTREE
python process_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" HydrographySrf D:\RTREE "C:\sde_connection_files\results_db_conn_as_data_owner.sde"
del D:\RTREE\HydrographySrf.idx /f /q /s
del D:\RTREE\HydrographySrf.dat /f /q /s
python generate_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" TransportationGroundPnt D:\RTREE
python process_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" TransportationGroundPnt D:\RTREE "C:\sde_connection_files\results_db_conn_as_data_owner.sde"
del D:\RTREE\TransportationGroundPnt.idx /f /q /s
del D:\RTREE\TransportationGroundPnt.dat /f /q /s
python generate_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" HydrographyPnt D:\RTREE
python process_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" HydrographyPnt D:\RTREE "C:\sde_connection_files\results_db_conn_as_data_owner.sde"
del D:\RTREE\HydrographyPnt.idx /f /q /s
del D:\RTREE\HydrographyPnt.dat /f /q /s
python generate_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" TransportationGroundCrv D:\RTREE
python process_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" TransportationGroundCrv D:\RTREE "C:\sde_connection_files\results_db_conn_as_data_owner.sde"
del D:\RTREE\TransportationGroundCrv.idx /f /q /s
del D:\RTREE\TransportationGroundCrv.dat /f /q /s
python generate_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" TransportationGroundSrf D:\RTREE
python process_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" TransportationGroundSrf D:\RTREE "C:\sde_connection_files\results_db_conn_as_data_owner.sde"
del D:\RTREE\TransportationGroundSrf.idx /f /q /s
del D:\RTREE\TransportationGroundSrf.dat /f /q /s
python generate_lc_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" UtilityInfrastructurePnt D:\RTREE
python process_lc_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" UtilityInfrastructurePnt D:\RTREE "C:\sde_connection_files\results_db_conn_as_data_owner.sde"
del D:\RTREE\UtilityInfrastructurePnt* /f /q /s
python generate_lc_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" HydrographyCrv D:\RTREE
python process_lc_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" HydrographyCrv D:\RTREE "C:\sde_connection_files\results_db_conn_as_data_owner.sde"
del D:\RTREE\HydrographyCrv* /f /q /s
python generate_lc_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" UtilityInfrastructureCrv D:\RTREE
python process_lc_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" UtilityInfrastructureCrv D:\RTREE "C:\sde_connection_files\results_db_conn_as_data_owner.sde"
del D:\RTREE\UtilityInfrastructureCrv* /f /q /s
python generate_lc_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" UtilityInfrastructureSrf D:\RTREE
python process_lc_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" UtilityInfrastructureSrf D:\RTREE "C:\sde_connection_files\results_db_conn_as_data_owner.sde"
del D:\RTREE\UtilityInfrastructureSrf* /f /q /s
python generate_lc_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" HydrographySrf D:\RTREE
python process_lc_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" HydrographySrf D:\RTREE "C:\sde_connection_files\results_db_conn_as_data_owner.sde"
del D:\RTREE\HydrographySrf* /f /q /s
python generate_lc_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" TransportationGroundPnt D:\RTREE
python process_lc_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" TransportationGroundPnt D:\RTREE "C:\sde_connection_files\results_db_conn_as_data_owner.sde"
del D:\RTREE\TransportationGroundPnt* /f /q /s
python generate_lc_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" HydrographyPnt D:\RTREE
python process_lc_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" HydrographyPnt D:\RTREE "C:\sde_connection_files\results_db_conn_as_data_owner.sde"
del D:\RTREE\HydrographyPnt* /f /q /s
python generate_lc_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" TransportationGroundCrv D:\RTREE
python process_lc_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" TransportationGroundCrv D:\RTREE "C:\sde_connection_files\results_db_conn_as_data_owner.sde"
del D:\RTREE\TransportationGroundCrv* /f /q /s
python generate_lc_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" TransportationGroundSrf D:\RTREE
python process_lc_rtree.py "C:\sde_connection_files\source_db_conn_as_viewer.sde" TransportationGroundSrf D:\RTREE "C:\sde_connection_files\results_db_conn_as_data_owner.sde"
del D:\RTREE\TransportationGroundSrf* /f /q /s
del D:\RTREE\ResourceSrf.p /f /q /s
