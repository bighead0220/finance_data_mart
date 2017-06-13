#!/usr/bin/perl
use strict;
use bdp_pub;
use etl_unix;
my $PROJECT_CD = 'FDM';          #项目代码
my $VSQL_CFG_NAME = 'FDM_VSQL';  #app_config.ini中VSQL连接配置的名称 
#my $SQLPLUS_CFG_NAME = 'FDM_SQLPLUS'; #app_config.ini中SQLPLUS连�配置的，名称 
#T_SYS_ORG    T_SYS_ORG_DUMY
#T_SYS_ORG
my $ORG_CODE;
my $ORG_NAME;
my $ORG_LVL;
my $ORG_SHT_NAME;
my $ORG_STAT;
my $PID;
my $PNAME;

my ${DB_WORK_DATE};
my ${CONTROL_FILE} = $ARGV[0];
my $ret=BDP::parseDirInfo(${CONTROL_FILE});
${DB_WORK_DATE}=$ret->{TXDATE};
#my $vsql_connect="/opt/vertica/bin/vsql -Atq -F ^ -d CPCIMDB -h 192.168.2.44 -U dbadmin -w vertica";
#########my $vsql_connect="vsql -h 22.224.65.171 -U mart -w mart123 -d CPCIMDB_TEST -t";

my $VSQL_NODE_COUNT = int(ETL::getAppConfig($VSQL_CFG_NAME,'VSQL_NODE_COUNT'));
my $VSQL_IP_PREFIX = ETL::getAppConfig($VSQL_CFG_NAME,'VSQL_IP_PREFIX');
my $VSQL_IP_START = int(ETL::getAppConfig($VSQL_CFG_NAME,'VSQL_IP_START'));
my $randomnumber = int(rand($VSQL_NODE_COUNT))+$VSQL_IP_START;
$ENV{'VSQL_HOST'} = "${VSQL_IP_PREFIX}$randomnumber";
print 'VSQL_HOST:', $ENV{'VSQL_HOST'}, "\n";
$ENV{'VSQL_PORT'} = ETL::getAppConfig($VSQL_CFG_NAME,'VSQL_PORT');
$ENV{'VSQL_DATABASE'} = ETL::getAppConfig($VSQL_CFG_NAME,'VSQL_DATABASE');
$ENV{'VSQL_USER'} = ETL::getAppConfig($VSQL_CFG_NAME,'VSQL_USER');
$ENV{'VSQL_PASSWORD'} = ETL::Decrypt(ETL::getAppConfig($VSQL_CFG_NAME,'VSQL_PASSWORD'));
$randomnumber = int(rand($VSQL_NODE_COUNT))+$VSQL_IP_START;
my $VSQL_BACKUP_HOST = "${VSQL_IP_PREFIX}$randomnumber";
print 'VSQL_BACKUP_HOST:', $VSQL_BACKUP_HOST, "\n";

my $COMMAND = "vsql -B ${VSQL_BACKUP_HOST} -C -h $ENV{'VSQL_HOST'} -d $ENV{'VSQL_DATABASE'} -U $ENV{'VSQL_USER'} -w $ENV{'VSQL_PASSWORD'} -t";
print "$COMMAND","\n";
my $vsql="select 'insert into app.T_SYS_ORG(ORG_CODE,ORG_NAME,ORG_LVL,ORG_SHT_NAME,ORG_STAT,PID,PNAME)values('''||org_cd||''','''||org_nm||''','''||org_hrcy||''','''||org_sht_nm||''','''||org_stat_cd||''','''||up_org_cd||''','''||up_org_nm||''');' from f_fdm.f_org_info where org_hrcy in('01','02','03') and org_typ ='0100' and etl_date='${DB_WORK_DATE}'::date";
my $res=&run_vsql_comm($vsql,$COMMAND);
my @orainsert = `sqlplus -s app/123456\@22.224.34.13/fdm<<EOF 
truncate table app.t_sys_org;
$res; 
commit;
exit;
EOF`
;
#---vertica load data method---#
sub run_vsql_comm{
 my ($sql,$string) = @_ ;  
my $result = `$string<<EOF
\\set AUTOCOMMIT off 
$sql;
 \\q
EOF`
;
my $exitcode = $? >> 8 ;
my $RET_CODE = $? >> 8 ;
if($RET_CODE == 0 ){
  print $result;
   return "$result";
   }
   else{
   return 1;
   }
   ;
}
