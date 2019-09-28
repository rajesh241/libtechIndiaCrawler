#!/bin/bash
#First we will kill the process if it is older than 3 hours
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo DIR
cd $DIR/../venv
source bin/activate
cmd="python $DIR/main.py -e -pn $1"
#echo $cmd
#$cmd
myPID=$(pgrep -f "$cmd")
sleep $2
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
