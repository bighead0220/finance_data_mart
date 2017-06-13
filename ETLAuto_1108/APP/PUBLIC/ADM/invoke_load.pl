#!/usr/bin/perl
#####################################################################
# Program :  
# Author  :  
# Date Time :
# Script File  : 
#====================================================================
# 修改历史:
#
# ###################################################################
use strict;
use bdp_pub;
use Time::localtime;
use Time::Local;
use IPC::Open3;
use etl_unix;

my $PROJECT_CD = 'FDM';          #项目代码
my $VSQL_CFG_NAME = 'FDM_VSQL';  #app_config.ini中VSQL连接配置的名称 

if ( $#ARGV < 0 or $ARGV[0] !~ /^(.{3})_.{3}_(\d{2,4})_(.*)_(\d{8}).dir$/) {
   print "\n";
   print "Usage: $0  \n";
   print "Usage: 使用参数 \n";
   print "       CONTROL_FILE  -- 控制文件(SYS_JOBNAME_YYYYMMDD.dir) \n";
   exit(1);
}
my %Macro;
open(STDERR, ">&STDOUT");
my $CtrlInfo = BDP::parseDirInfo($ARGV[0]);

my $sqlfile = $ARGV[0];
$sqlfile =~ s/_\d{8}.dir$/.sql/;
$sqlfile = substr($sqlfile, 4);

my $runSql = getSqlFileTxt($PROJECT_CD);
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
#my $job_name=lc($CtrlInfo->{SUBJOBNAME});

$Macro{'$file_name'}=~ s/YYYYMMDD/$CtrlInfo->{TXDATE}/;
my $ret=system("curl -l -s -i \"http://$Macro{'$hadoop_namenode_id1'}:50070/webhdfs/v1/$Macro{'$path2'}/$Macro{'$file_name'}.ok?op=LISTSTATUS\" ");
if ($ret != 0){
   while(1){#判断ok文件是否到达
   my $result = `curl -l -s -i "http://$Macro{'$hadoop_namenode_id2'}:50070/webhdfs/v1/$Macro{'$path2'}/$Macro{'$file_name'}.ok?op=LISTSTATUS" | grep HTTP`;
   print "result : $result \n";
   if ($result =~ /200|OK/ ) {
         `sed -i 's/\$hadoop_namenode_id/$Macro{'$hadoop_namenode_id2'}/' ${BDP::AUTO_HOME}/tmp/RUNSQL/$CtrlInfo->{SYS}/$sqlfile`;
         last;
         }
   print "OK文件[ $CtrlInfo->{SUBJOBNAME}.ok ]未到达 Waiting ...30s \n";
   sleep 5;
        }
}else {
   while(1){#判断ok文件是否到达
   my $result = `curl -l -s -i "http://$Macro{'$hadoop_namenode_id1'}:50070/webhdfs/v1/$Macro{'$path2'}/$Macro{'$file_name'}.ok?op=LISTSTATUS" | grep HTTP`;
   print "*************$Macro{'$file_name'}*********\n";
   if ($result =~ /200|OK/ ) {
         `sed -i 's/\$hadoop_namenode_id/$Macro{'$hadoop_namenode_id1'}/' ${BDP::AUTO_HOME}/tmp/RUNSQL/$CtrlInfo->{SYS}/$sqlfile`;
         last;
         }
   print "OK文件[ $CtrlInfo->{SUBJOBNAME}.ok ]未到达 Waiting ...30s \n";
   sleep 5;
       }
}

#去掉回车换行符号

my $ret = BDP::runVsqlCommand($VSQL_CFG_NAME, "${BDP::AUTO_HOME}/tmp/RUNSQL/$CtrlInfo->{SYS}/$sqlfile");

exit($ret);

#===================================================
#根据控制文件,获取对应的sql文件内容
#===================================================
sub getSqlFileTxt {
   my $PROJECT_CD = shift;
   
   my $fname = $ARGV[0]; #控制文件名
   my $r = BDP::parseDirInfo($fname);

   $fname = "${BDP::AUTO_HOME}/APP/SQLSCRIPT/$r->{SYS}/" . lc($r->{SUBJOBNAME}) . '.sql';
   
   print " fname = $fname \n";   

   unless(open(FS,$fname)) {
      print "cann't open sql file $fname $!\n";
      return -1;  
   }

   my $str;
   my $len = 1024 * 1024 * 10;
   read(FS,$str,$len);
   close(FS);

   #读取unload.ini中的变量
   my $cfg_unload = "${BDP::AUTO_HOME}/etc/FDM_unload.ini";
   my %Files = &GetConfigs($cfg_unload,'ADM');

   
   #读取app_config.ini中SQL变量
   my $para = ETL::getAppParameters("PUBLIC_SQL_VARS");
   foreach (@$para) {
      $Macro{"\$${_}"} = ETL::getAppConfig("PUBLIC_SQL_VARS", $_);
   }
   
   $para = ETL::getAppParameters("${PROJECT_CD}_SQL_VARS");
   foreach (@$para) {
      $Macro{"\$${_}"} = ETL::getAppConfig("${PROJECT_CD}_SQL_VARS", $_);
   }
   
   #print Dumper(%Macro);

   #日期相关的变量
   $Macro{'$SYS'}     = $r->{SYS};                       #作业系统名
   $Macro{'$PROV'}    = $r->{PROV};                      #省码/业务代码(两位或四位数字)
   $Macro{'$TXDATE'}  = $r->{TXDATE};                    #数据日期/统计日期
   $Macro{'$JOBNAME'} = $r->{JOBNAME};                   #完整的作业名称,例如 SAV_44_AGREEMENT_INI
   $Macro{'$SUBJOBNAME'} = $r->{SUBJOBNAME};             #完整的作业名称省份代码后面的部分,例如 AGREEMENT_INI
   
   $Macro{'$YESTERDAY'}  = BDP::decDate($r->{TXDATE},-1);    #数据日期/统计日期($TXDATE)之前的一天
   $Macro{'$NEXTDAY'}    = BDP::decDate($r->{TXDATE}, 1);    #数据日期/统计日期($TXDATE)之后的一天

   my $tmpPeriodDay=BDP::getPeriodDay($r->{TXDATE});
   $Macro{'$PERIODBGNDAY'} = $$tmpPeriodDay[0];      #根据交易日期取所在旬的第一天
   $Macro{'$PERIODENDDAY'} = $$tmpPeriodDay[1];      #根据交易日期取所在旬的最后一天
   $Macro{'$PERIODNO'}     = $$tmpPeriodDay[2];      #根据交易日期取所在旬的序号20060101/20060102/20060102

   my $tmpMonthDay=BDP::getMonthDay($r->{TXDATE});
   $Macro{'$MONTHBGNDAY'} = $$tmpMonthDay[0];        #根据交易日期取月的第一天
   $Macro{'$MONTHENDDAY'} = $$tmpMonthDay[1];        #根据交易日期取月的最后一天
   $Macro{'$MONTHNO'}     = $$tmpMonthDay[2];        #根据交易日期取月200601/200602/200603

   my $tmpSeasonDay=BDP::getSeasonDay($r->{TXDATE});
   $Macro{'$SEASONBGNDAY'} = $$tmpSeasonDay[0];      #根据交易日期取季度的第一天
   $Macro{'$SEASONENDDAY'} = $$tmpSeasonDay[1];      #根据交易日期取季度的最后一天
   $Macro{'$SEASONNO'}     = $$tmpSeasonDay[2];      #根据交易日期取季度200601/200602/200603/200604

   my $tmpYearDay=BDP::getYearDay($r->{TXDATE});
   $Macro{'$YEARBGNDAY'} = $$tmpYearDay[0];          #根据交易日期取年的第一天
   $Macro{'$YEARENDDAY'} = $$tmpYearDay[1];          #根据交易日期取年的最后一天
   $Macro{'$YEARNO'}     = $$tmpYearDay[2];          #根据交易日期取年2006

   #替换sqL 表
   $Macro{'$table_name'} = $r->{SUBJOBNAME};


   $Macro{'$file_name'} = $Files{lc($r->{SUBJOBNAME})};
   $Macro{'YYYYMMDD'}  = $r->{TXDATE};
   #对sql文件中变量信息进行集中替换
   my $tmp;
   foreach (sort{length($b) <=> length($a)} keys %Macro) {
      $tmp = quotemeta($_);
      $str =~ s/$tmp/$Macro{$_}/gi;
   }
   #print " $r->{TXDATE}    =     $Macro{'$MONTHENDDAY'} \n";  
  # 控制月频度  
   print "This is a Month job running with date[$r->{TXDATE}] Exit! \n"  if $r->{TXDATE} ne $Macro{'$MONTHENDDAY'} ;
   exit 0 if $r->{TXDATE} ne $Macro{'$MONTHENDDAY'} ;
   
   return \$str;
}

#----------------------------------------------------
#名称: GetConfigs()
#说明:
#入参: 配置文件  配置文件中关键字名称
#出参: 正确:0 错误: 1 
#----------------------------------------------------

sub GetConfigs {
		 
    my ($g_cfgs,$key) = @_ ;
    if (!-e "$g_cfgs") {
    	  #my $time = getTime("YYYY-MM-DD HH:MI:SS");
        print "[ time] 找不到配置文件 [$g_cfgs] \n";
        exit(1);
    }
    my $g_config = Config::IniFiles->new( -file => "$g_cfgs");
    if (!$g_config) {
    	  #my $time = getTime("YYYY-MM-DD HH:MI:SS");
        print "[ time] 打开配置文件[$g_cfgs]失败 ! $! \n";
        exit(1);
    } 
   my %hash = IniToHash($g_config, $key);      
   %hash ; 
}
#----------------------------------------------------
#名称: IniToHash()
#说明:
#入参: 从ini文件转hash
#出参: 正确:0 错误: 1 
#----------------------------------------------------
sub IniToHash {

    my $ini = $_[0];
    my @para = $ini->Parameters($_[1]);
    my %hash_val;

    foreach  (@para) {
        $hash_val{$_} = ForceDel($ini->val($_[1], $_));
    }

    return %hash_val;
}
#----------------------------------------------------
#名称: ForceDel()
#说明: 去空格
#入参: 
#出参: 正确:0 错误: 1 
#----------------------------------------------------
sub ForceDel {

    defined $_[0] or return '';

    $_[0] =~ s/\s+//g;
    return $_[0];
}
