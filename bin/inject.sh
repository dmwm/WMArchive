#!/bin/bash
##H Script to inject data into wmarchive
##H Usage: inject.sh <json-file> <number of calls, use -1 for infinitive loop>

if [ "$#" -lt 1  ]; then
    cat $0 | grep "^##H" | sed -e "s,##H,,g"
    exit 1
fi

### DO NOT CHANGE BELOW THIS LINE

url=https://cmsweb-testbed.cern.ch/wmarchive/data/
#url=http://localhost:8200/wmarchive/data/
jfile=$1
iters=$2
count=0
while True; do
    echo $count
    curl -L -k --key ~/.globus/userkey.pem --cert ~/.globus/usercert.pem \
        -X POST -d@$jfile -H "Content-Type: application/json" $url
    if [ $iters -eq -1 ]; then
        continue
    fi
    count=$((count+1))
    if [ $count -eq $iters ]; then
        break
    fi
done
