#!/bin/bash



READ=$1

SAVE=$2



echo "“C_CUSTKEY”,“C_NAME”,“C_ADDRESS”,“C_NATIONKEY”,“C_PHONE”,“C_ACCTBAL”,“C_MKTSEGMENT”,“C_COMMENT”" > $SAVE



COUNTER=1

while read LINE; do

        echo "$LINE" >> $SAVE

        COUNTER=$(($COUNTER+1))

done < $1 
