#!/usr/bin/env bash

# get public IP of the linux machine
ip=$(dig +short myip.opendns.com @resolver1.opendns.com)
echo $ip

# set PAGER to empty to not get cli output automatically being sent to vim
export PAGER=

# get sts info
aws sts get-caller-identity --output text

#start instance
aws ec2 start-instances --instance-ids i-059a0baa5fb90a263 --region ap-south-1 --output text
echo "starting instance..."
sleep 60

# get the public ip and status of the instance
public_ip=$(aws ec2 describe-instances --instance-ids i-059a0baa5fb90a263 --query 'Reservations[*].Instances[*].PublicDnsName' --region ap-south-1 --output text)
status=$(aws ec2 describe-instance-status --instance-ids i-059a0baa5fb90a263 --query 'InstanceStatuses[*].InstanceState.Name' --region ap-south-1 --output text)
echo $public_ip
echo $status

# while [ $status != "running" ] ; do
#   echo "instance not yet available"
#   sleep 5
# done

# check if the instance is available for use, if not, then wait for it
while [ $status != "running" ] ; do
  echo "instance not yet available"
  sleep 5
  status=$(aws ec2 describe-instance-status --instance-ids i-059a0baa5fb90a263 --query 'InstanceStatuses[*].InstanceState.Name' --region ap-south-1 --output text)
done

# ssh into the instance and run the set of commands to build
ssh -o StrictHostKeyChecking=accept-new -i "~/keys/redhood-keys/turbot.pem" ubuntu@$public_ip 'cat test.txt; uname -m; cd steampipe-postgres-fdw; git checkout main; git pull; git checkout $VERSION; rm -rf build-Linux; ./build.sh; exit'

# use scp to fetch the built binary out of the instance
scp -i "~/keys/redhood-keys/turbot.pem" ubuntu@$public_ip:/home/ubuntu/steampipe-postgres-fdw/build-Linux/steampipe_postgres_fdw.so .

# stop instance
aws ec2 stop-instances --instance-ids i-059a0baa5fb90a263 --region ap-south-1
