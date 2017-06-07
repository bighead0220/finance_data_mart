#!/bin/usr/perl
#-----------------------------------------------
#--FDM export2ma Program
#--Script : export2ma.pl
#--Date:2016/8/22
#--duhy
#--使用说明：perl export2ma.pl tablename date 
#
#---------------------------------------------
use strict;
use Config::IniFiles; 
use Date::Manip;
#---全局变量声明-----------------------------
my $FDM_HOME =  $ENV{"AUTO_HOME"};
   if(!defined($FDM_HOME)){
     $FDM_HOME = "/FDM_DATA";
     }
unshift(@INC, $FDM_HOME."/bin/");
my $export2ma_etc = "/home/etl/ETLAuto/APP/FDM/export2ma.etc";
my $rfname = "/home/etl/ETLAuto/APP/FDM/export2ma.sql";
#require etl ;
open(STDERR, ">&STDOUT");
my $table_name = "ma_subj_info";
my $etl_date = "20160225";
#读取配置文件
my $var_cfg = Config::IniFiles->new( -file => $export2ma_etc);
my $path = $var_cfg->val('EXPORT2MA','PATH');
my $hadoop_namenode_id = $var_cfg->val('EXPORT2MA','NAMENODE');
my $hadoop_user = $var_cfg->val('EXPORT2MA','USER');
my $delimiter = $var_cfg->val('EXPORT2MA','DELIMITER');
my $retry = $var_cfg->val('EXPORT2MA','RETRY');
my $retrydelay = $var_cfg->val('EXPORT2MA','RETRYDELAY');
my $file_name = $var_cfg->val('EXPORT2MA',$table_name);
   $file_name  =~ s/YYYYMMDD/$etl_date/gi;
my $schema_ma = $var_cfg->val('EXPORT2MA','SCHEMA');
my $connect_string = $var_cfg->val('EXPORT2MA','CONNECTSTR');
 
my $sql = "";
unless(open(FS,$rfname)){
    print "can not open sql file $rfname $!\n";
    exit(1);
 }
 my $len = 1024*10;
 read(FS,$sql,$len);
 close(FS);
$sql =~ s/\$hadoop_namenode_id/$hadoop_namenode_id/gi ;
$sql =~ s/\$path/$path/gi ;
$sql =~ s/\$table_name/$table_name/gi ;
$sql =~ s/\$file_name/$file_name/gi ;
$sql =~ s/\$hadoop_user/$hadoop_user/gi ;
$sql =~ s/\$hadoop_user/$hadoop_user/gi;
$sql =~ s/\$schema_ma/$schema_ma/gi ;
$sql =~ s/\$retry/$retry/gi ;
$sql =~ s/\$retrydelay/$retrydelay/gi;
$sql =~ s/\$delimiter/$delimiter/gi;

print $sql ."\n";
print $connect_string ."\n";
my $ret = &run_VSQL_comm($sql,$connect_string);
exit($ret);

#----------------------------------
#名称: run_VSQL_comm()
#说明:
#入参: 1、sql语句 2、Vsql 连接串
#出参: 正确:0 错误: 1 
#----------------------------------
sub run_VSQL_comm{
 my ($sql,$string)  =  @_ ; 
 my $rc  =  open(VSQL,"| $string ");
 unless($rc){
  print "Could not invoke VSQL commond \n";
  return -1;
 }
# \\set AUTOCOMMIT OFF
#----below are VSQL scripts----
 print VSQL<<ENDOFINPUT;
 
  \\timing  
 
 $sql

 \\q
ENDOFINPUT

 close(VSQL);
 
 my $RET_CODE  = $? >> 8 ;
 if($RET_CODE  ==  0 ){
   return 0;
   }
   else{
   return 1;
   }
}
 
