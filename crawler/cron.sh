#!/bin/bash
#First we will kill the process if it is older than 3 hours
cd /home/mango/repo/libtechIndiaCrawler/venv/
source bin/activate
cmd="python /home/mango/repo/libtechIndiaCrawler/crawler/main.py -e -lf $1"
#echo $cmd
#$cmd
myPID=$(pgrep -f "$cmd")

echo $myPID
if [ -z "$myPID" ]
then
  echo "Variable is empty"
else
  echo "Variable is not empty"
  myTime=`ps -o etimes= -p "$myPID"`
  echo $myTime
  if [ $myTime -gt 900000 ]
    then 
      echo "Time is about 3 hours"
      kill -9 $myPID
  fi
fi
pgrep -f "$cmd" || $cmd &> /tmp/$1.log
