select exportdata(* 
	using parameters cmd='
(
URL="http://$hadoop_namenode_id:50070/webhdfs/v1/$path/$table_name/$file_name-`hostname`"; 
USER="$hadoop_user"; 
curl -X DELETE "$URL?op=DELETE&user.name=$USER"; 
MSG=`curl -i -X PUT "$URL?op=CREATE&overwrite=true&user.name=$USER" 2>/dev/null | grep "Set-Cookie:\|Location:" | tr ''\r'' ''\n''`; 
Ck=`echo $MSG | awk -F ''Location:'' ''{print $1}'' | awk -F ''Set-Cookie:'' ''{print $2}''| tr -d ''[:blank:]\r\n''`; 
Loc=`echo $MSG | awk -F ''Location:'' ''{print $2}''| tr -d ''[:blank:]\r\n''`; 
curl -b "$Ck" -X PUT -T - "$Loc";
) 2>&1 > /dev/null
	', separator='$delimiter', fromcharset='utf8', tocharset='gb18030'
	) over (partition auto) 
  from  f_fdm.cd_cd_table;
