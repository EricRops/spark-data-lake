#!/bin/bash

# "source ./run.sh" to run this in shell

# Set important variable paths (EDIT THIS SECTION with your setup :D )
pem_file=""
# Bootstrap path is folder path only, not the file
bootstrap_path="s3://"
subnet_id=""

# Copy bootstrap file to S3 location
aws s3 cp ./bootstrap.sh $bootstrap_path

# Create EMR cluster and save the cluster ID as a variable
ID=$(aws emr create-cluster \
--name spark-data-lake \
--use-default-roles \
--release-label emr-5.30.0 \
--instance-count 2 \
--applications Name=Spark Name=Ganglia Name=Zeppelin \
--ec2-attributes KeyName=spark-cluster,SubnetId=$subnet_id \
--instance-type m4.large \
--bootstrap-actions Path=${bootstrap_path}bootstrap.sh \
--query ClusterId \
--output text)

# Wait until the cluster is running
aws emr wait cluster-running --cluster-id $ID
echo Cluster is running

# Get hostname for SSH access
hostname=$(aws emr describe-cluster --output text --cluster-id $ID --query Cluster.MasterPublicDnsName)

# Copy the PY scripts from the ./src folder into the EMR session (recursive for all files in the directory)
scp -r -i $pem_file \
./src \
hadoop@$hostname:~/

# SSH into the master node
ssh -i $pem_file hadoop@$hostname

#  ------------------ THIS SECTION NEEDS TO BE DONE MANUALLY ---------------------

# Run ETL Spark script in EMR 
# spark-submit src/etl.py

# Exit the EMR console
# exit

#  ------------------ THIS SECTION IS EXECUTED AFTER THE PY SCRIPT ---------------------

# Copy log file and query results to current Local directory (the dot represents the current directory)
scp -i $pem_file \
hadoop@$hostname:~/logfile.log .

scp -r -i $pem_file \
hadoop@$hostname:~/queries/ .

# Finally, terminate the cluster
aws emr terminate-clusters --cluster-ids $ID
