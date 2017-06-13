HadoopIP="22.224.64.99" #ip
HadoopUser="hdfs" #user
VerticaIP="22.224.65.171" # vertica ip
FullSql=$2


URL="http://$HadoopIP:50070/webhdfs/v1/psbcma/upload/$1";
USER=$HadoopUser;
curl -X DELETE "$URL?op=DELETE&user.name=$USER";

MSG=`curl -i -X PUT "$URL?op=CREATE&overwrite=true&user.name=$USER" 2>/dev/null | grep "Set-Cookie:\|Location:" | tr '\r' '\n'`;

Ck=`echo $MSG | awk -F 'Location:' '{print $1}' | awk -F 'Set-Cookie:' '{print $2}' | tr -d '[:blank:]\r\n'`
 
Loc=`echo $MSG | awk -F 'Location:' '{print $2}'| tr -d '[:blank:]\r\n'`

/opt/vertica/bin/vsql -h $VerticaIP -U dbadmin -w vertica -d CPCIMDB_test -C -Atq -F '^' -c "$FullSql"|curl -b "$Ck" -C - -s -S -X PUT -T - "$Loc" &

