SET PGPASSWORD=@@@db.new_data_owner_pwd@@@
SET db_sotd_tds=@@@db_latest_tds.db_name@@@
SET db_sotd_mgcp=@@@db_latest_mgcp.db_name@@@

:: Identify pop_grid and RPM table presence in the sotd_tds_latest db.
for /f %%i in ('psql -t -A -h @@@db.pg_host@@@ -d %db_sotd_tds% -U @@@db.new_data_owner@@@ -p @@@db.pg_port@@@ -c "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'sotd' AND tablename = 'pop_grid')::int;"') do set pop_grid=%%i

if %pop_grid% EQU 0 (
	echo Missing table "pop_grid" in TDS. Copy feature class into %db_sotd_tds%.
    exit /b %pop_grid%
)

if %pop_grid% NEQ 0 (
	echo Table "pop_grid" exists in %db_sotd_tds%.
)

for /f %%i in ('psql -t -A -h @@@db.pg_host@@@ -d %db_sotd_mgcp% -U @@@db.new_data_owner@@@ -p @@@db.pg_port@@@ -c "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'sotd' AND tablename = 'pop_grid')::int;"') do set pop_grid=%%i

if %pop_grid% EQU 0 (
	echo Missing table "pop_grid" in MGCP. Copy feature class into %db_sotd_mgcp%.
    exit /b %pop_grid%
)

if %pop_grid% NEQ 0 (
	echo Table "pop_grid" exists in %db_sotd_mgcp%.
)

for /f %%i in ('psql -t -A -h @@@db.pg_host@@@ -d %db_sotd_tds% -U @@@db.new_data_owner@@@ -p @@@db.pg_port@@@ -c "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'sotd' AND tablename = 'rpm')::int;"') do set rpm=%%i

if %rpm% EQU 0 (
	echo Missing table "rpm" in tds. Copy feature class into %db_sotd_tds%.
    exit /b %rpm% 
)

if %rpm% NEQ 0 (
	echo Table "rpm" exists in %db_sotd_tds%.
)

for /f %%i in ('psql -t -A -h @@@db.pg_host@@@ -d %db_sotd_mgcp% -U @@@db.new_data_owner@@@ -p @@@db.pg_port@@@ -c "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'sotd' AND tablename = 'rpm')::int;"') do set rpm=%%i

if %rpm% EQU 0 (
	echo Missing table "rpm" in MGCP. Copy feature class into %db_sotd_mgcp%.
    exit /b %rpm%
)

if %rpm% NEQ 0 (
	echo Table "rpm" exists in %db_sotd_mgcp%.
)

:: Copy deployment files to S3
aws s3 cp ./inputs/yarn.sh @@@emr_configuration.s3_path_to_code@@@/

:: TDS - SOTD scala
aws s3 cp ./inputs/tds_sotd_job.scala @@@emr_configuration.s3_path_to_code@@@/

:: MGCP - SOTD confs and scala
aws s3 cp ./inputs/mgcp_sotd_job.scala @@@emr_configuration.s3_path_to_code@@@/


::Create cluster
echo Creating EMR Cluster
aws emr create-cluster ^
    --applications Name=Hadoop Name=Spark ^
    --tags ServerType=EMRNode Environment=@@@emr_configuration.tagEnvironment@@@ Product=@@@emr_configuration.tagProduct@@@ MissionOwner=@@@emr_configuration.tagMissionOwner@@@ Name=@@@emr_configuration.clusterName@@@ for-use-with-amazon-emr-managed-policies=true CLAP_OFF=NEVER CLAP_ON=NEVER^
    --ec2-attributes "{\"KeyName\":\"@@@emr_configuration.keyName@@@\",\"AdditionalSlaveSecurityGroups\":[\"@@@emr_configuration.additionalSlaveSecurityGroups@@@\"],\"InstanceProfile\":\"@@@emr_configuration.instanceProfile@@@\",\"SubnetId\":\"@@@emr_configuration.subnetId@@@\",\"EmrManagedSlaveSecurityGroup\":\"@@@emr_configuration.managedSlaveSecurityGroup@@@\",\"EmrManagedMasterSecurityGroup\":\"@@@emr_configuration.managedMasterSecurityGroup@@@\",@@@emr_configuration.serviceAccessSecurityGroup@@@\"AdditionalMasterSecurityGroups\":[\"@@@emr_configuration.additionalMasterSecurityGroup@@@\"]}" ^
    --release-label @@@emr_configuration.releaseLabel@@@ ^
    --log-uri @@@emr_configuration.logLocation@@@/ ^
    --instance-groups "[{\"InstanceCount\":1,\"EbsConfiguration\":{\"EbsBlockDeviceConfigs\":[{\"VolumeSpecification\":{\"SizeInGB\":512,\"VolumeType\":\"gp2\"},\"VolumesPerInstance\":4}]},\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"@@@emr_configuration.masterInstanceType@@@\",\"Name\":\"Master-1\"},{\"InstanceCount\":4,\"EbsConfiguration\":{\"EbsBlockDeviceConfigs\":[{\"VolumeSpecification\":{\"SizeInGB\":128,\"VolumeType\":\"gp2\"},\"VolumesPerInstance\":4}]},\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"@@@emr_configuration.workerInstanceType@@@\",\"Name\":\"Core-2\"}]" ^
    --ebs-root-volume-size 10 ^
    --service-role @@@emr_configuration.serviceRole@@@ ^
    --security-configuration @@@emr_configuration.securityConfiguration@@@ ^
    --auto-terminate ^
    --enable-debugging ^
    --name @@@emr_configuration.clusterName@@@ ^
    --scale-down-behavior TERMINATE_AT_TASK_COMPLETION ^
    --region @@@emr_configuration.awsRegion@@@ ^
    --output text > @@@emr_configuration.text_file_with_token@@@

echo Getting Cluster ID
FOR /F "Tokens=2-3" %%a in ( @@@emr_configuration.text_file_with_token@@@) do (set clusterid=%%a)
echo %clusterid%

SET PGPASSWORD=@@@db.new_data_owner_pwd@@@

::echo Waiting for cluster to start up...
::aws emr wait cluster-running --cluster-id %clusterid%

:: Run pre-jobs for sotd and CARTS
:: NOTE - This assumes that the psql path has been added to the machine as an environment variable. Confirm its existence if psql cannot be run from the batch file. 

:: TDS - SOTD
(echo \i 'D:/sotd/deployments/v@@@version@@@/_ready/sotd-jobs/inputs/sql/pre_tds_sotd_job.sql' ) | psql -h @@@db.pg_host@@@ -d @@@db_latest_tds.db_name@@@ -U @@@db.new_data_owner@@@ -p @@@db.pg_port@@@

:: MGCP - SOTD 
(echo \i 'D:/sotd/deployments/v@@@version@@@/_ready/sotd-jobs/inputs/sql/pre_mgcp_sotd_job.sql' ) | psql -h @@@db.pg_host@@@ -d @@@db_latest_mgcp.db_name@@@ -U @@@db.new_data_owner@@@ -p @@@db.pg_port@@@

:: copy yarn to machine
aws emr add-steps --cluster-id %clusterid% --steps Name="Copy Yarn",Jar="command-runner.jar",Args=[aws,s3,cp,@@@emr_configuration.s3_path_to_code@@@/yarn.sh,///home/hadoop/yarn.sh]
echo Yarn.sh File Moved to Cluster


::CHMOD
::chmod 777 ///home/hadoop/yarn.sh
echo Running chmod
aws emr add-steps --cluster-id %clusterid% --steps Name="Chmod Yarn",Jar="command-runner.jar",Args=[chmod,777,///home/hadoop/yarn.sh]


:: Four add-steps commands with lists of steps for SOTD-CARTS-TDS-GRID, SOTD-CARTS-MGCP, CARTS-TDS-SPT, and CARTS error reporting jobs
aws emr add-steps --cluster-id %clusterid% --steps file://./sotd_tds_steps.json
aws emr add-steps --cluster-id %clusterid% --steps file://./sotd_mgcp_steps.json