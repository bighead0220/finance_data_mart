#!/usr/bin/perl
#####################################################################
# Program : 通过vsql调用SQL脚本模板(逐步执行,支持TD中BTEQ工具的.IF ActivityCount <= 0 THEN .GOTO LABEL_NAME; && .LABEL LABEL_NAME 语法)
# Author  : wuzhg@teamsun.com.cn
# Date Time    :  2016/06/03
# Script File  :  call_vsql_step_by_step.pl
#====================================================================
# 修改历史:
#
# ###################################################################
use strict;
use bdp_pub;

my $PROJECT_CD = 'FDM';          #项目代码
my $VSQL_CFG_NAME = 'FDM_VSQL';  #app_config.ini中VSQL连接配置的名称 

if ( $#ARGV < 0 or $ARGV[0] !~ /^(.{3})_.{3}_(\d{2,4})_(.*)_(\d{8}).dir$/) {
   print "\n";
   print "Usage: $0  \n";
   print "Usage: 使用参数 \n";
   print "       CONTROL_FILE  -- 控制文件(SYS_JOBNAME_YYYYMMDD.dir) \n";
   exit(1);
}

open(STDERR, ">&STDOUT");

my $CtrlInfo = BDP::parseDirInfo($ARGV[0]);

my $sqlfile = $ARGV[0];
$sqlfile =~ s/_\d{8}.dir$/.sql/;
$sqlfile = substr($sqlfile, 4);

my $runSql = BDP::getSqlFileTxt($PROJECT_CD);
exit 1 if($runSql == -1);

unless(-d "${BDP::AUTO_HOME}/tmp/RUNSQL/$CtrlInfo->{SYS}") {
   `mkdir -p ${BDP::AUTO_HOME}/tmp/RUNSQL/$CtrlInfo->{SYS}`;
}

my $FH;
open($FH,">${BDP::AUTO_HOME}/tmp/RUNSQL/$CtrlInfo->{SYS}/$sqlfile");
unless($FH) {
   print "Create ${BDP::AUTO_HOME}/tmp/RUNSQL/$CtrlInfo->{SYS}/$sqlfile failed.\n";
   exit 1;
}
print $FH "\\timing\n";
print $FH $$runSql;
print $FH "\ncommit;\n";
close($FH);

my $ret = BDP::runVsqlCommand2($VSQL_CFG_NAME, $runSql);

exit($ret->{RetCode});
