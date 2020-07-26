#!/bin/bash
#First we will kill the process if it is older than 3 hours
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo DIR
cd $DIR/../venv
source bin/activate
#export PYTHONPATH="${PYTHONPATH}:/home/crawler/repo/libtechIndiaCrawler/"
export PYTHONPATH="${PYTHONPATH}:$HOME/repo/libtechIndiaCrawler/"
echo 
cmd="python $DIR/state_status.py -e "
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
  if [ $myTime -gt 28800 ]
    then 
      echo "Time is about 3 hours"
      kill -9 $myPID
  fi
fi
# 28800 corresponds to roughly 8 hours
pgrep -f "$cmd" || $cmd &> /tmp/$1.log
