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
my $export2ma_etc = "$FDM_HOME/APP/PUBLIC/FDM/export2ma.etc"; 
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
 
my $sql = "select * from f_fdm.f_Cap_FX_Postn ;"; 

my $comm = "/bin/bash $FDM_HOME/APP/PUBLIC/FDM/exp.sh $table_name/$file_name \"$sql\"  ";
my $ret = system($comm);
exit($ret);
 
