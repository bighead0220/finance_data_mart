#!/bin/bash

if [ $# -lt 2 ] ; then
  echo "Usage: $0 \"selectSQL (with SEGCONDITION macro in where clause ) \" dataFilename fileCountPerNode fromEncoding toEncoding"
  exit 1
fi
echo $1
selectSQL="$1"
#dataFilename=$2
echo $selectSQL
fileCountPerNode=$2
if [ -z $fileCountPerNode ] ; then
  fileCountPerNode=1
fi

fromEncoding=$4
if [ -z $fromEncoding ] ; then
  fromEncoding="utf-8"
fi

toEncoding=$5
if [ -z $toEncoding ] ; then
  toEncoding="utf-8"
fi

VSQL='/opt/vertica/bin/vsql -h 192.168.2.44 -U dbadmin -w vertica -d CPCIMDB'

# get array of node names
VSQL=`sed s/-a// <<<$VSQL`
VSQL=`sed s/-e// <<<$VSQL`
nodes=( `${VSQL} -Atq -c "select node_address from nodes where node_state='UP' order by node_address;" | tr "\\r\\n" " "` )
#nodes=( `${VSQL} -Atq -c "select node_name from nodes where node_state='UP' order by node_address;" | tr "\\r\\n" " "` )
if [ $? -eq 0 -a ${#nodes[*]} -gt 0 ] ; then
  for (( n=0 ; n<${#nodes[*]} ; n++ )) ; do
	exp="\(\(9223372036854775807\/\/${#nodes[*]}\)\+1\)\=${n}"
        echo $selectSQL
	fullSQL=`sed s/SEGCONDITION/$exp/ <<<$selectSQL`
        echo $fullSQL
    node=${nodes[$n]}
    
    if [ $fileCountPerNode -eq 1 ] ; then
        echo "/opt/vertica/bin/vsql -h 192.168.2.44 -U dbadmin -w vertica -d CPCIMDB -Atq -F \'^\' -c \" $fullSQL ; \"  2>&1 &"
        /opt/vertica/bin/vsql -h 192.168.2.44 -U dbadmin -w vertica -d CPCIMDB -Atq -F \'^\' -c \" $fullSQL ; \"  2>&1 &
    fi
  done

  wait
fi
