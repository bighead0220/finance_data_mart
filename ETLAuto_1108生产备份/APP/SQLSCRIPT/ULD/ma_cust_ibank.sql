/*

*/




select exportdata(* using parameters cmd = '
(
URL="http://$hadoop_namenode_id:50070/webhdfs/v1/$path/$table_name/$TXDATE/$file_name-`hostname`"; 
USER="$hadoop_user"; 
curl -X DELETE "$URL?op=DELETE&user.name=$USER"; 
MSG=`curl -i -X PUT "$URL?op=CREATE&overwrite=true&user.name=$USER" 2>/dev/null | grep "Set-Cookie:\|Location:" | tr ''\r'' ''\n''`; 
Ck=`echo $MSG | awk -F ''Location:'' ''{print $1}'' | awk -F ''Set-Cookie:'' ''{print $2}''| tr -d ''[:blank:]\r\n''`; 
Loc=`echo $MSG | awk -F ''Location:'' ''{print $2}''| tr -d ''[:blank:]\r\n''`; 
curl -b "$Ck" -C - -s -S --retry 3 --retry-delay 10 -X PUT -T - "$Loc";
) 2>&1 > /dev/null
	',
                   separator = '$delimiter',
                   fromcharset = 'utf8',
                   tocharset = 'utf8') over(partition auto)
  from f_rdm.ma_cust_ibank;


select exportdata(* using parameters cmd = '
(
URL="http://$hadoop_namenode_id:50070/webhdfs/v1/$path/$table_name/$TXDATE/$table_name.ok"; 
USER="$hadoop_user"; 
curl -X DELETE "$URL?op=DELETE&user.name=$USER"; 
MSG=`curl -i -X PUT "$URL?op=CREATE&overwrite=true&user.name=$USER" 2>/dev/null | grep "Set-Cookie:\|Location:" | tr ''\r'' ''\n''`; 
Ck=`echo $MSG | awk -F ''Location:'' ''{print $1}'' | awk -F ''Set-Cookie:'' ''{print $2}''| tr -d ''[:blank:]\r\n''`; 
Loc=`echo $MSG | awk -F ''Location:'' ''{print $2}''| tr -d ''[:blank:]\r\n''`; 
curl -b "$Ck" -C - -s -S --retry 3 --retry-delay 10 -X PUT -T - "$Loc";
) 2>&1 > /dev/null
        ',
                   separator = '$delimiter',
                   fromcharset = 'utf8',
                   tocharset = 'utf8') over(partition auto)
  from dual;
