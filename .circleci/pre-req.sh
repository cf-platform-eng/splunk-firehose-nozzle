#!/usr/bin/env bash
set -e
#Install CF CLI
wget -q -O - https://packages.cloudfoundry.org/debian/cli.cloudfoundry.org.key | sudo apt-key add -
echo "deb https://packages.cloudfoundry.org/debian stable main" | sudo tee /etc/apt/sources.list.d/cloudfoundry-cli.list
#Add support for https apt sources
sudo apt-get install apt-transport-https ca-certificates
sudo apt-get update
sudo apt-get install cf-cli
#CF Login
cf login --skip-ssl-validation -a $API_ENDPOINT -u $API_USER -p $API_PASSWORD -o system -s system
#Create splunk-ci org and space
if [  "`cf o | grep "splunk-ci"`" == "splunk-ci" ]; then
   echo "Its here"
   cf target -o "splunk-ci" -s "splunk-ci"
else
   cf create-org splunk-ci
   cf target -o splunk-ci
   cf create-space splunk-ci
   cf target -o "splunk-ci" -s "splunk-ci"
fi