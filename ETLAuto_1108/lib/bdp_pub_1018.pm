##############################################
# Program : 大数据平台用到的公共函数
# Author  : wuzhg@teamsun.com.cn
# Date Time    :  2016/06/03
# Script File  :  bdp_pub.pm
# 说明：
# Modify Info:
#    2016/6/3 初始版本
#    2016/7/9 修改runVsqlCommand2函数超时时间由8小时改为4小时
#             保留SQL语句中Hint提示,并给INSERT/UPDATE/DELETE强制加上/*+ DIRECT */提示
#    2016/8/25 捕获vsql执行过程中的错误代码,并根据配置做返回值转换
##############################################
package BDP;

#引用的包
use strict;
use DBI;
use Time::localtime;
use Time::Local;
use IPC::Open3;
use etl_unix;
#use Data::Dumper;

#环境变量,在 ~/.bash_profile 文件中定义
$BDP::AUTO_HOME = $ENV{'AUTO_HOME'};

my %Macro;

#==================================================================
#判断是否是闰年
#若是闰年则返回 1 否则返回 0
#输入的信息前四位一定是年
#==================================================================
sub isYeapYear{
   my $tmpYear="$_[0]";

   $tmpYear=int(substr($tmpYear,0,4));

   return (($tmpYear % 4 eq 0 && $tmpYear % 100 ne 0) || $tmpYear % 400 eq 0) ? 1 : 0;
}


#根据日期($date)及两个日期之间的差值($count),计算新的日期
sub decDate {
   my ($date,$count) = @_;
      
   return ETL::addDeltaDays($date, $count);
}


#==================================================================
#解析dir控制文件名结构
#在$AUTO_HOME/DATA/process下的控制文件信息,文件名具有如下的特征
#BCD_BCD_11_CURRENCY_CD_20060309.dir(这是作业BCD_11_CURRENCY_CD 20060309 在目录$AUTO_HOME/DATA/process下的控制文件)
#==================================================================
sub parseDirInfo {

   my $dirfile = shift;
   my ($ret,$tmpIniTabNm);

   $dirfile =~ /^(.{3})_.{3}_(\d{2,4})_(.*)_(\d{8}).dir$/;
   $ret->{SYS} = $1;                      #作业系统名;
   $ret->{PROV} = $2;                     #省份/业务代码信息;
   $ret->{JOBNAME} = "${1}_${2}_${3}";    #作业名称;
   $ret->{TXDATE} = $4;                   #数据日期/交易日期;
   $ret->{SUBJOBNAME} = $3;               #作业名称省份代码后面的部分;
   $ret->{SDA_TABLE_NAME} =  $3;          #用于DQC取得SDATA库中的加载表的表名,同时对于初始加载去掉后面的INI

   return $ret;
}


#===================================================================
# format: YYYY MM DD HH MI SS
#===================================================================
sub getTime{

   my $ret = "@_";
   my $tc = localtime(time());
   $tc = sprintf("%4d%02d%02d%02d%02d%02d",$tc->year+1900,$tc->mon+1,
                 $tc->mday, $tc->hour, $tc->min,$tc->sec);
   my $tmp = substr($tc,0,4);
   $ret =~ s/YYYY/$tmp/g;

   $tmp = substr($tc,4,2);
   $ret =~ s/MM/$tmp/g;

   $tmp = substr($tc,6,2);
   $ret =~ s/DD/$tmp/g;

   $tmp = substr($tc,8,2);
   $ret =~ s/HH/$tmp/g;

   $tmp = substr($tc,10,2);
   $ret =~ s/MI/$tmp/g;

   $tmp = substr($tc,12,2);
   $ret =~ s/SS/$tmp/g;
   return $ret;
}

#===================================================
# 读取app_config.ini中的数据库配置,并通过DBI连接数据库
# 使用该方法前需要在/etc/odbcinst.ini中配置名称为Vertica的ODBC Driver:
#[Vertica]
#Description = vertica
#Driver = /opt/vertica/opt/vertica/lib64/libverticaodbc.so
#===================================================
sub connectDB {
   my ($db_cfg_name) = @_;

   my $VSQL_NODE_COUNT = int(ETL::getAppConfig($db_cfg_name,'VSQL_NODE_COUNT'));
   my $VSQL_IP_PREFIX = ETL::getAppConfig($db_cfg_name,'VSQL_IP_PREFIX');
   my $VSQL_IP_START = int(ETL::getAppConfig($db_cfg_name,'VSQL_IP_START'));

   my $randomnumber = int(rand($VSQL_NODE_COUNT))+$VSQL_IP_START;
   my $VSQL_HOST = "${VSQL_IP_PREFIX}$randomnumber";
   print 'VSQL_HOST:', $VSQL_HOST, "\n";
   my $VSQL_PORT = ETL::getAppConfig($db_cfg_name,'VSQL_PORT');
   my $VSQL_DATABASE = ETL::getAppConfig($db_cfg_name,'VSQL_DATABASE');
   my $VSQL_USER = ETL::getAppConfig($db_cfg_name,'VSQL_USER');
   my $VSQL_PASSWORD = ETL::Decrypt(ETL::getAppConfig($db_cfg_name,'VSQL_PASSWORD'));
   
   my $dbh = DBI->connect("dbi:ODBC:DRIVER={Vertica};Server=${VSQL_HOST};Port=${VSQL_PORT};Database=${VSQL_DATABASE}", $VSQL_USER, $VSQL_PASSWORD,
                          { AutoCommit => 1, PrintError => 1, RaiseError => 0 } );
   
   unless ( defined($dbh) ) { print $DBI::errstr; return undef; }
   
   return $dbh;
}

#===================================================
#根据控制文件,获取对应的sql文件内容
#===================================================
sub getSqlFileTxt {
   my $PROJECT_CD = shift;
   
   my $fname = $ARGV[0]; #控制文件名
   my $r = parseDirInfo($fname);

   $fname = "${BDP::AUTO_HOME}/APP/SQLSCRIPT/$r->{SYS}/" . lc($r->{SUBJOBNAME}) . '.sql';

   unless(open(FS,$fname)) {
      print "cann't open sql file $fname $!\n";
      return -1; #此处的 -1 在其他调用getSqlFileTxt的地方会有使用
   }

   my $str;
   my $len = 1024 * 1024 * 10;
   read(FS,$str,$len);
   close(FS);
   
   return replaceVariable($PROJECT_CD, \$str);
}

#===================================================
#替换变量
#===================================================
sub replaceVariable {
   my ($PROJECT_CD, $str) = @_;
   
   my $fname = $ARGV[0]; #控制文件名
   my $r = parseDirInfo($fname);
   
   #读取app_config.ini中SQL变量
   my $para = ETL::getAppParameters("PUBLIC_SQL_VARS");
   foreach (@$para) {
      $Macro{$_} = ETL::getAppConfig("PUBLIC_SQL_VARS", $_);
   }
   
   $para = ETL::getAppParameters("${PROJECT_CD}_SQL_VARS");
   foreach (@$para) {
      $Macro{$_} = ETL::getAppConfig("${PROJECT_CD}_SQL_VARS", $_);
   }
   
   #print Dumper(%Macro);

   #日期相关的变量
   $Macro{'SYS'}     = $r->{SYS};                       #作业系统名
   $Macro{'PROV'}    = $r->{PROV};                      #省码/业务代码(两位或四位数字)
   $Macro{'TXDATE'}  = $r->{TXDATE};                    #数据日期/统计日期
   $Macro{'JOBNAME'} = $r->{JOBNAME};                   #完整的作业名称,例如 SAV_44_AGREEMENT_INI
   $Macro{'SUBJOBNAME'} = $r->{SUBJOBNAME};             #完整的作业名称省份代码后面的部分,例如 AGREEMENT_INIE
   
   $Macro{'YEAR'}    = substr($r->{TXDATE}, 0, 4);      #4位年份
   $Macro{'MONTH'}   = substr($r->{TXDATE}, 4, 2);      #2位月份
   $Macro{'DAY'}     = substr($r->{TXDATE}, 6, 2);      #2位日期
   
   $Macro{'YESTERDAY'}  = &decDate($r->{TXDATE},-1);    #数据日期/统计日期($TXDATE)之前的一天
   $Macro{'NEXTDAY'}    = &decDate($r->{TXDATE}, 1);    #数据日期/统计日期($TXDATE)之后的一天

   my $tmpPeriodDay=&getPeriodDay($r->{TXDATE});
   $Macro{'PERIODBGNDAY'} = $$tmpPeriodDay[0];      #根据交易日期取所在旬的第一天
   $Macro{'PERIODENDDAY'} = $$tmpPeriodDay[1];      #根据交易日期取所在旬的最后一天
   $Macro{'PERIODNO'}     = $$tmpPeriodDay[2];      #根据交易日期取所在旬的序号20060101/20060102/20060102

   my $tmpMonthDay=&getMonthDay($r->{TXDATE});
   $Macro{'MONTHBGNDAY'} = $$tmpMonthDay[0];        #根据交易日期取月的第一天
   $Macro{'MONTHENDDAY'} = $$tmpMonthDay[1];        #根据交易日期取月的最后一天
   $Macro{'MONTHNO'}     = $$tmpMonthDay[2];        #根据交易日期取月200601/200602/200603

   my $tmpSeasonDay=&getSeasonDay($r->{TXDATE});
   $Macro{'SEASONBGNDAY'} = $$tmpSeasonDay[0];      #根据交易日期取季度的第一天
   $Macro{'SEASONENDDAY'} = $$tmpSeasonDay[1];      #根据交易日期取季度的最后一天
   $Macro{'SEASONNO'}     = $$tmpSeasonDay[2];      #根据交易日期取季度200601/200602/200603/200604

   my $tmpYearDay=&getYearDay($r->{TXDATE});
   $Macro{'YEARBGNDAY'} = $$tmpYearDay[0];          #根据交易日期取年的第一天
   $Macro{'YEARENDDAY'} = $$tmpYearDay[1];          #根据交易日期取年的最后一天
   $Macro{'YEARNO'}     = $$tmpYearDay[2];          #根据交易日期取年2006

   #对sql文件中变量信息进行集中替换
   my $tmp;
   foreach (sort{length($b) <=> length($a)} keys %Macro) {
      $tmp = quotemeta($_);
      $$str =~ s/\$\{${tmp}\}/$Macro{$_}/gi;
      $$str =~ s/\$${tmp}/$Macro{$_}/gi;
   }

   return $str;
}

#===================================================
#调用vsql执行SQL文件,并将查询结果导出文本文件
#参数1: 导出文件名
#参数2: 字段分隔符
#参数3: 行分隔符
#参数4: 查询SQL文件名
#===================================================
sub runVsqlExport {

   my ($db_cfg_name, $filename, $delimiter, $terminator, $runSql) = @_;
   my %returncode;
   
   #读取app_config.ini中错误代码对应返回值
   my $para = ETL::getAppParameters("VSQL_EXIT_CODE");
   foreach (@$para) {
      $returncode{$_} = ETL::getAppConfig("VSQL_EXIT_CODE", $_);
   }

   my $VSQL_NODE_COUNT = int(ETL::getAppConfig($db_cfg_name,'VSQL_NODE_COUNT'));
   my $VSQL_IP_PREFIX = ETL::getAppConfig($db_cfg_name,'VSQL_IP_PREFIX');
   my $VSQL_IP_START = int(ETL::getAppConfig($db_cfg_name,'VSQL_IP_START'));

   my $randomnumber = int(rand($VSQL_NODE_COUNT))+$VSQL_IP_START;
   $ENV{'VSQL_HOST'} = "${VSQL_IP_PREFIX}$randomnumber";
   print 'VSQL_HOST:', $ENV{'VSQL_HOST'}, "\n";
   $ENV{'VSQL_PORT'} = ETL::getAppConfig($db_cfg_name,'VSQL_PORT');
   $ENV{'VSQL_DATABASE'} = ETL::getAppConfig($db_cfg_name,'VSQL_DATABASE');
   $ENV{'VSQL_USER'} = ETL::getAppConfig($db_cfg_name,'VSQL_USER');
   $ENV{'VSQL_PASSWORD'} = ETL::Decrypt(ETL::getAppConfig($db_cfg_name,'VSQL_PASSWORD'));
   $randomnumber = int(rand($VSQL_NODE_COUNT))+$VSQL_IP_START;
   my $VSQL_BACKUP_HOST = "${VSQL_IP_PREFIX}$randomnumber";
   print 'VSQL_BACKUP_HOST:', $VSQL_BACKUP_HOST, "\n";
   
   my $expfile = $filename;
   if ( defined($terminator) && $terminator ne '') {
      $terminator = quotemeta($terminator);
      $expfile = "${filename}.p";
      if ( -p $expfile ) { unlink($expfile); }
      system("mkfifo $expfile");
      system("cat $expfile | perl -p -e 's/\\n/$terminator\\n/' > $filename &");
   }
   
   my $ret;
   my @result;
   if (substr($delimiter,0,2) eq "\$'") {
      @result = readpipe("vsql -a -B ${VSQL_BACKUP_HOST} -C -F $delimiter -At -o $expfile -f $runSql 2>&1");
   } else {
      @result = readpipe("vsql -a -B ${VSQL_BACKUP_HOST} -C -F '$delimiter' -At -o $expfile -f $runSql 2>&1");
   }
   
   print "@result\n";
   $ret = $? >> 8;
   foreach (@result) {
      s/^\s+//; s/\s+$//;
      if (/^vsql: could not connect to server/) {
         $ret = 11; last;
      } elsif (/server closed the connection unexpectedly/) {
         $ret = 13; last;
      } elsif (/ERROR\s+(\d+):/ || /FATAL\s+(\d+):/ || /ROLLBACK\s+(\d+):/) {
         $ret = $1; last;
      }
   }

   #根据错误号和配置参数进行返回值转换
   if ($ret != 0) {
      print "Error Code:$ret\n";
      if (exists $returncode{$ret}) {
         $ret = $returncode{$ret};
      } else {
         $ret = ($ret < 100 ? $ret : 1);
      }
   }
   
   if (defined($terminator) && $terminator ne '') { unlink($expfile); }
   
   print "Return Code:$ret\n";
   return $ret;
}


#===================================================
#运行vsql命令,参数为SQL文件名
#===================================================
sub runVsqlCommand {
   my ($db_cfg_name, $runSql) = @_;
   my %returncode;
   
   #读取app_config.ini中错误代码对应返回值
   my $para = ETL::getAppParameters("VSQL_EXIT_CODE");
   foreach (@$para) {
      $returncode{$_} = ETL::getAppConfig("VSQL_EXIT_CODE", $_);
   }

   my $VSQL_NODE_COUNT = int(ETL::getAppConfig($db_cfg_name,'VSQL_NODE_COUNT'));
   my $VSQL_IP_PREFIX = ETL::getAppConfig($db_cfg_name,'VSQL_IP_PREFIX');
   my $VSQL_IP_START = int(ETL::getAppConfig($db_cfg_name,'VSQL_IP_START'));

   my $randomnumber = int(rand($VSQL_NODE_COUNT))+$VSQL_IP_START;
   $ENV{'VSQL_HOST'} = "${VSQL_IP_PREFIX}$randomnumber";
   print 'VSQL_HOST:', $ENV{'VSQL_HOST'}, "\n";
   $ENV{'VSQL_PORT'} = ETL::getAppConfig($db_cfg_name,'VSQL_PORT');
   $ENV{'VSQL_DATABASE'} = ETL::getAppConfig($db_cfg_name,'VSQL_DATABASE');
   $ENV{'VSQL_USER'} = ETL::getAppConfig($db_cfg_name,'VSQL_USER');
   $ENV{'VSQL_PASSWORD'} = ETL::Decrypt(ETL::getAppConfig($db_cfg_name,'VSQL_PASSWORD'));
   $randomnumber = int(rand($VSQL_NODE_COUNT))+$VSQL_IP_START;
   my $VSQL_BACKUP_HOST = "${VSQL_IP_PREFIX}$randomnumber";
   print 'VSQL_BACKUP_HOST:', $VSQL_BACKUP_HOST, "\n";
   
   my $ret;
   my @result = readpipe("vsql -a -B ${VSQL_BACKUP_HOST} -C -v ON_ERROR_STOP=on -v AUTOCOMMIT=off -f $runSql 2>&1");
   
   print "@result\n";
   $ret = $? >> 8;
   if ($ret != 0) {
      foreach (@result) {
         s/^\s+//; s/\s+$//;
         if (/^vsql: could not connect to server/) {
            $ret = 11; last;
         } elsif (/server closed the connection unexpectedly/) {
            $ret = 13; last;
         } elsif (/ERROR\s+(\d+):/ || /FATAL\s+(\d+):/ || /ROLLBACK\s+(\d+):/) {
            $ret = $1; last;
         }
      }
   }

   #根据错误号和配置参数进行返回值转换
   if ($ret != 0) {
      print "Error Code:$ret\n";
      if (exists $returncode{$ret}) {
         $ret = $returncode{$ret};
      } else {
         $ret = ($ret < 100 ? $ret : 1);
      }
   }
   
   print "Return Code:$ret\n";
   return $ret;
}


#===================================================
#运行vsql命令,参数为SQL串的地址引用
#===================================================
sub runVsqlCommand1 {
   my ($db_cfg_name, $runSql) = @_;
   my %returncode;
   
   #读取app_config.ini中错误代码对应返回值
   my $para = ETL::getAppParameters("VSQL_EXIT_CODE");
   foreach (@$para) {
      $returncode{$_} = ETL::getAppConfig("VSQL_EXIT_CODE", $_);
   }

   my $VSQL_NODE_COUNT = int(ETL::getAppConfig($db_cfg_name,'VSQL_NODE_COUNT'));
   my $VSQL_IP_PREFIX = ETL::getAppConfig($db_cfg_name,'VSQL_IP_PREFIX');
   my $VSQL_IP_START = int(ETL::getAppConfig($db_cfg_name,'VSQL_IP_START'));

   my $randomnumber = int(rand($VSQL_NODE_COUNT))+$VSQL_IP_START;
   $ENV{'VSQL_HOST'} = "${VSQL_IP_PREFIX}$randomnumber";
   print 'VSQL_HOST:', $ENV{'VSQL_HOST'}, "\n";
   $ENV{'VSQL_PORT'} = ETL::getAppConfig($db_cfg_name,'VSQL_PORT');
   $ENV{'VSQL_DATABASE'} = ETL::getAppConfig($db_cfg_name,'VSQL_DATABASE');
   $ENV{'VSQL_USER'} = ETL::getAppConfig($db_cfg_name,'VSQL_USER');
   $ENV{'VSQL_PASSWORD'} = ETL::Decrypt(ETL::getAppConfig($db_cfg_name,'VSQL_PASSWORD'));
   $randomnumber = int(rand($VSQL_NODE_COUNT))+$VSQL_IP_START;
   my $VSQL_BACKUP_HOST = "${VSQL_IP_PREFIX}$randomnumber";
   print 'VSQL_BACKUP_HOST:', $VSQL_BACKUP_HOST, "\n";
   
   my $ret;
   my @result = readpipe("vsql -a -B ${VSQL_BACKUP_HOST} -C -v ON_ERROR_STOP=on -v AUTOCOMMIT=off 2>&1 <<EOF\n$$runSql\nEOF");
   
   print "@result\n";
   $ret = $? >> 8;
   if ($ret != 0) {
      foreach (@result) {
         s/^\s+//; s/\s+$//;
         if (/^vsql: could not connect to server/) {
            $ret = 11; last;
         } elsif (/server closed the connection unexpectedly/) {
            $ret = 13; last;
         } elsif (/ERROR\s+(\d+):/ || /FATAL\s+(\d+):/ || /ROLLBACK\s+(\d+):/) {
            $ret = $1; last;
         }
      }
   }

   #根据错误号和配置参数进行返回值转换
   if ($ret != 0) {
      print "Error Code:$ret\n";
      if (exists $returncode{$ret}) {
         $ret = $returncode{$ret};
      } else {
         $ret = ($ret < 100 ? $ret : 1);
      }
   }
   
   print "Return Code:$ret\n";
   return $ret;
}

#===================================================
#运行vsql命令,参数为SQL串的地址引用
#===================================================
sub runVsqlCommand2 {
   my ($db_cfg_name, $runSql) = @_;
   my %returncode;
   
   #读取app_config.ini中错误代码对应返回值
   my $para = ETL::getAppParameters("VSQL_EXIT_CODE");
   foreach (@$para) {
      $returncode{$_} = ETL::getAppConfig("VSQL_EXIT_CODE", $_);
   }
   
   my %last_on_error = (4568=>1, 2624=>1, 4876=>1);

   my $VSQL_NODE_COUNT = int(ETL::getAppConfig($db_cfg_name,'VSQL_NODE_COUNT'));
   my $VSQL_IP_PREFIX = ETL::getAppConfig($db_cfg_name,'VSQL_IP_PREFIX');
   my $VSQL_IP_START = int(ETL::getAppConfig($db_cfg_name,'VSQL_IP_START'));

   my $randomnumber = int(rand($VSQL_NODE_COUNT))+$VSQL_IP_START;
   $ENV{'VSQL_HOST'} = "${VSQL_IP_PREFIX}$randomnumber";
   print 'VSQL_HOST:', $ENV{'VSQL_HOST'}, "\n";
   $ENV{'VSQL_PORT'} = ETL::getAppConfig($db_cfg_name,'VSQL_PORT');
   $ENV{'VSQL_DATABASE'} = ETL::getAppConfig($db_cfg_name,'VSQL_DATABASE');
   $ENV{'VSQL_USER'} = ETL::getAppConfig($db_cfg_name,'VSQL_USER');
   $ENV{'VSQL_PASSWORD'} = ETL::Decrypt(ETL::getAppConfig($db_cfg_name,'VSQL_PASSWORD'));
   $randomnumber = int(rand($VSQL_NODE_COUNT))+$VSQL_IP_START;
   my $VSQL_BACKUP_HOST = "${VSQL_IP_PREFIX}$randomnumber";
   print 'VSQL_BACKUP_HOST:', $VSQL_BACKUP_HOST, "\n";

   local(*Reader,*Writer);
   my @Msg;
   my $ret;
   my $rc;
   my $flag = 0;
   my $TimeOut = 8*3600;
   my @statements;
   
   my $sqlstr = $$runSql;
   
   #去除/**/注释信息,但保留原有Hint("/*+"开头)
   my $s = 0;
   my $t = 0;
   while (1) {
      $s = index($sqlstr ,'/*', $s);
      if ($s == -1) { last; }
      if (substr($sqlstr,$s+2,1) eq '+') {
         $s += 2;
      } else {
         $t = index($sqlstr, '*/', $s);
         $sqlstr = substr($sqlstr,0,$s) . substr($sqlstr,$t+2);
         $s = 0;
      }
   }
   
   #去除--注释中的分号
   my ($str1, $str2);
   while ( $sqlstr =~ /--(.*)/g) {
      $str1 = $str2 = $1;
      if (index($1,';') != -1) {
         $str1 = quotemeta($str1);
         $str2 =~ s/;//g;
         $sqlstr =~ s/--${str1}/--${str2}/;
      }
   }
   
   @statements = split /;\s+/, $sqlstr;

   $|++;
   my $pid = open3(\*Writer, \*Reader, \*Reader, "vsql -a -B ${VSQL_BACKUP_HOST} -C");

   eval {
      local $SIG{INT}  = sub { alarm 0; die "killed\n" };
      local $SIG{QUIT} = sub { alarm 0; die "killed\n" };
      local $SIG{TERM} = sub { alarm 0; die "killed\n" };
      local $SIG{ALRM} = sub { alarm 0; die "alarm\n" };
      alarm $TimeOut;

      print Writer "\\set AUTOCOMMIT off\n";
      print Writer "\\set ON_ERROR_STOP on\n";
      print Writer "\\timing\n";
      
      #检查连接是否成功
      while (my $line = <Reader>) {
         print $line;
         push @Msg, $line;
         chomp($line);
         $line =~ s/^\s+//;
         $line =~ s/\s+$//;
         if ($line =~ /^vsql: could not connect to server/) {
            $flag = 11; last;
         } elsif ($line =~ /FATAL\s+(\d+):/) {
            $flag = $1; last;
         } else {
            last;
         }
      }
      
      if ($flag) { print "Error Code:$flag\n"; }
      if (exists $returncode{$flag}) {
         $flag = $returncode{$flag};
      } else {
         $flag = ($flag < 100 ? $flag : 1);
      }
      
      if ($flag) {
         print "Return Code:$flag\n";
         pop @Msg;
         $ret->{RetCode} = $flag;
         $ret->{RetMsg} = \@Msg;
         alarm 0;
         return $ret;
      }
      
      my $sql;
      my $activitycount = 0;
      my @label;
      my $label;
      my @tmp;
      
      #逐条执行SQL语句,并获取交互输出以检查是否执行成功
      foreach (my $i=0; $i<=$#statements; $i++) {
         $sql = $statements[$i];
         $sql =~ s/^\s+//;
         $sql =~ s/\s+$//;
         if ($sql eq '') {next;}
         
         if ($sql =~ /INSERT(.*)[INTO|\n]/i && index($1,'/*+') == -1) {
            $sql =~ s/INSERT\s+/INSERT \/*+ DIRECT *\/ /i;
         } elsif ($sql =~ /DELETE(.*)[\w+|\n]/i && index($1,'/*+') == -1) {
            $sql =~ s/DELETE\s+/DELETE \/*+ DIRECT *\/ /i;
         } elsif ($sql =~ /UPDATE(.*)[\w+|\n]/i && index($1,'/*+') == -1) {
            $sql =~ s/UPDATE\s+/UPDATE \/*+ DIRECT *\/ /i;
         }
         
         if ($sql =~ /ActivityCount\s*(\<*\>*\=*)\s*(\d+)\s+(.*)$/i && @label==0) {
            if (($1 eq '<' && $activitycount < $2) ||
                ($1 eq '<=' && $activitycount <= $2) ||
                ($1 eq '>' && $activitycount > $2) ||
                ($1 eq '>=' && $activitycount >= $2) ||
                ($1 eq '=' && $activitycount == $2)) {
               @tmp = split(/\s+/,$3);
               $label = $tmp[$#tmp];
               print "$sql\n\n";
               print "label $label start.\n\n";
               push @label,$label;
            } else {
               print "$sql\n\n";
               print "statment ignored.\n\n";
            }
         } elsif ($sql =~ /\.GOTO\s+(.*)$/i && @label==0) {
            @tmp = split(/\s+/,$1);
            $label = $tmp[$#tmp];
            print "$sql\n\n";
            print "label $label start.\n\n";
            push @label,$label;
         } elsif ($sql =~ /\.LABEL\s+(.*)$/i) {
            @tmp = split(/\s+/,$1);
            $label = $tmp[$#tmp];
            if ($label eq $label[$#label]) {
               print "$sql\n\n";
               print "label $label end.\n\n";
               pop @label;
            } else {
               print "$sql\n\n";
               print "statment ignored.\n\n";
            }
         } elsif (@label>0) {
            print "$sql\n\n";
            print "statment ignored.\n\n";
         } else {
            print Writer "${sql}\n;\n";
            $flag = 0;
            @Msg = ();
            my $result_start = 0;
            $activitycount = -1;
            while (my $line = <Reader>) {
               print $line;
               push @Msg, $line;
               chomp($line);
               $line =~ s/^\s+//;
               $line =~ s/\s+$//;
               if (index($line,'----')>=0) {
                  $result_start = 1;
               } elsif ($line =~ /^(\d+)$/ && $result_start==1) {
                  $activitycount = $1;
               } elsif ($line =~ /^Time:/ || ($flag>0)) {
                  last;
               } elsif ($line =~ /server closed the connection unexpectedly/) { #session has been killed
                  $flag = 13; last;
               } elsif ($line =~ /ERROR\s+(\d+):/ || $line =~ /ROLLBACK\s+(\d+):/) {
                  $flag = $1; last;
               }
            }
            print "\n";
            last if ($flag>0);
         }
      }

      if ($flag == 0) {
         print Writer "commit;\n";
         print Writer "\\q\n";
      }      
      close Writer;
      close Reader;
      waitpid($pid,0);
      
      #根据错误号和配置参数进行返回值转换
      if ($flag) { print "Error Code:$flag\n"; }
      if (exists $returncode{$flag}) {
         $flag = $returncode{$flag};
      } else {
         $flag = ($flag < 100 ? $flag : 1);
      }
      
      print "Return Code:$flag\n";
      $ret->{RetCode} = $flag;
      if ($flag) {
         pop @Msg;
         $ret->{RetMsg} = \@Msg;
      } else {
         @Msg = ();
         $Msg[0] = 'Call vsql success.';
         $ret->{RetMsg} = \@Msg;
      }
      alarm 0;
   }; # end of eval
   
   if ($@ eq "alarm\n") {
      close Writer;
      close Reader;
      kill 9,$pid;
      waitpid($pid,0);
      
      print "Call vsql timeout.\n";   
      $ret->{RetCode} = 10;
      @Msg = ();
      $Msg[0] = 'Call vsql timeout.';
      $ret->{RetMsg} = \@Msg;
   } elsif ($@ eq "killed\n") {
      print "Call vsql has been killed\n";
      $ret->{RetCode} = 9;
      @Msg = ();
      $Msg[0] = 'Call vsql has been killed.';
      $ret->{RetMsg} = \@Msg;
   }
   
   return $ret;
}


#===================================================
#运行beeline命令,参数为SQL文件名
#===================================================
sub runBeelineCommand {
   my ($hive_cfg_name, $runSql) = @_;

   my $HIVE_URL = ETL::getAppConfig($hive_cfg_name,'HIVE_URL');
   my $HIVE_USER = ETL::getAppConfig($hive_cfg_name,'HIVE_USER');
   my $HIVE_PWDFILE = ETL::getAppConfig($hive_cfg_name,'HIVE_PWDFILE');
   
   my $ret;
   my @result = readpipe("beeline -u ${HIVE_URL} -n ${HIVE_USER} -w ${HIVE_PWDFILE} -f $runSql 2>&1");
   
   print "@result\n";
   $ret = $? >> 8;
   
   print "Return Code:$ret\n";
   if ($ret == 0) {
      if (grep(/JOB FINISHED SUCCESSFULLY/, @result)) {
         return $ret;
      } else {
         print "Job does not finished successfully, reset return code to 1\n";
         return 1;
      }
   } else {
      return $ret;
   }   
}


#===================================================
#根据输入的日期信息,得到此日期所在旬的第一天和最后一天,同时返回旬的序号
#例如20060101 20060110 20060101
#日期格式统一是YYYYMMDD
#===================================================
sub getPeriodDay {
   my $tmpDate="$_[0]";

   my @tmpDays=(31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31);

   my ($tmpYear,$tmpMonth,$tmpDay) = (int(substr($tmpDate,0,4)),int(substr($tmpDate,4,2)),int(substr($tmpDate,6,2)));

   my @tmpPeriodDay;
   if($tmpDay >=1 && $tmpDay <=10)
   {
      $tmpPeriodDay[0]=sprintf("%04d%02d%02d",$tmpYear,$tmpMonth,1);
      $tmpPeriodDay[1]=sprintf("%04d%02d%02d",$tmpYear,$tmpMonth,10);
      $tmpPeriodDay[2]=sprintf("%04d%02d%02d",$tmpYear,$tmpMonth,1);
   }elsif($tmpDay >=11 && $tmpDay <=20){
      $tmpPeriodDay[0]=sprintf("%04d%02d%02d",$tmpYear,$tmpMonth,11);
      $tmpPeriodDay[1]=sprintf("%04d%02d%02d",$tmpYear,$tmpMonth,20);
      $tmpPeriodDay[2]=sprintf("%04d%02d%02d",$tmpYear,$tmpMonth,2);
   }elsif($tmpDay >=21 && $tmpDay <=31){
      #闰年
      my $tmpEndDate = $tmpDays[$tmpMonth-1];
      $tmpEndDate = $tmpDays[$tmpMonth-1] + isYeapYear($tmpYear) if ( $tmpMonth == 2 );

      $tmpPeriodDay[0]=sprintf("%04d%02d%02d",$tmpYear,$tmpMonth,21);
      $tmpPeriodDay[1]=sprintf("%04d%02d%02d",$tmpYear,$tmpMonth,$tmpEndDate);
      $tmpPeriodDay[2]=sprintf("%04d%02d%02d",$tmpYear,$tmpMonth,3);
   }

   return \@tmpPeriodDay;
}


#===================================================
#根据输入的日期信息,得到此日期所在月的第一天和最后一天的日期值,考虑闰年
#日期格式统一是YYYYMMDD,输入信息至少有6位表示年/月信息
#===================================================
sub getMonthDay {
   my $tmpDate="$_[0]";

   my @tmpDays=(31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31);
   my ($tmpYear,$tmpMonth) = (int(substr($tmpDate,0,4)),int(substr($tmpDate,4,2)));

   my ($tmpBgnDate,$tmpEndDate)=(1,$tmpDays[$tmpMonth-1]);

   $tmpEndDate = 29 if ( $tmpMonth == 2 && isYeapYear($tmpYear));

   my @tmpMonthDay;

   $tmpMonthDay[0]=sprintf("%04d%02d%02d",$tmpYear,$tmpMonth,$tmpBgnDate);
   $tmpMonthDay[1]=sprintf("%04d%02d%02d",$tmpYear,$tmpMonth,$tmpEndDate);
   $tmpMonthDay[2]=sprintf("%04d%02d",$tmpYear,$tmpMonth);

   return \@tmpMonthDay;
}


#===================================================
#根据输入的日期信息,得到此日期所在季度的第一天和最后一天的日期值,考虑闰年
#日期格式统一是YYYYMMDD
#===================================================
sub getSeasonDay {
   my $tmpDate="$_[0]";

   my @tmpSeasonDay;
   if(substr($tmpDate,4) ge "0101" && substr($tmpDate,4) le "0331")
   {
      $tmpSeasonDay[0]=substr($tmpDate,0,4)."0101";
      $tmpSeasonDay[1]=substr($tmpDate,0,4)."0331";
      $tmpSeasonDay[2]=substr($tmpDate,0,4)."01";
   }
   elsif(substr($tmpDate,4) ge "0401" && substr($tmpDate,4) le "0630")
   {
      $tmpSeasonDay[0]=substr($tmpDate,0,4)."0401";
      $tmpSeasonDay[1]=substr($tmpDate,0,4)."0630";
      $tmpSeasonDay[2]=substr($tmpDate,0,4)."02";
   }
   elsif(substr($tmpDate,4) ge "0701" && substr($tmpDate,4) le "0930")
   {
      $tmpSeasonDay[0]=substr($tmpDate,0,4)."0701";
      $tmpSeasonDay[1]=substr($tmpDate,0,4)."0930";
      $tmpSeasonDay[2]=substr($tmpDate,0,4)."03";
   }
   elsif(substr($tmpDate,4) ge "1001" && substr($tmpDate,4) le "1231")
   {
      $tmpSeasonDay[0]=substr($tmpDate,0,4)."1001";
      $tmpSeasonDay[1]=substr($tmpDate,0,4)."1231";
      $tmpSeasonDay[2]=substr($tmpDate,0,4)."04";
   }

   return \@tmpSeasonDay;
}


#===================================================
#根据输入的日期信息,得到此日期所在年的第一天和最后一天的日期值
#日期格式统一是YYYYMMDD
#===================================================
sub getYearDay {
   my $tmpDate="$_[0]";

   my @tmpYearDay;

   $tmpYearDay[0]=substr($tmpDate,0,4)."0101";
   $tmpYearDay[1]=substr($tmpDate,0,4)."1231";
   $tmpYearDay[2]=substr($tmpDate,0,4);

   return \@tmpYearDay;
}


#===================================================
#计算指定日期自公元1年1月1日开始的天数(考虑闰年),日期格式YYYYMMDD
#===================================================
sub DateLocal
{
   my $tmpDate="$_[0]";
   my ($yy, $mm, $dd) = (int(substr($tmpDate,0,4)),int(substr($tmpDate,4,2)),int(substr($tmpDate,6,2)));

   my @MonthDays = (31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31);
   my $Days;
   my $LeapYear = isYeapYear($yy);

   $Days = ($yy - 1) * 365 + int(($yy - 1) / 4) - int(($yy - 1) / 100) + int(($yy - 1) / 400);

   for (my $i = 1; $i < $mm; $i++)
   {
      $Days += $MonthDays[$i - 1];
      if ($i == 2) { $Days += $LeapYear; }
   }

   $Days += ($dd - 1);
   return $Days;
}


#===================================================
# 计算从 $tmpDate1 到 $tmpDate2 的天数
# 如果前一个日期后于后一个日期，则得到负数
# 方法: 均从公元 1 年开始计算经过的天数，然后用两个天数相减
#===================================================
sub DateDiff
{
   my($tmpDate1,$tmpDate2)=@_;

   return &DateLocal($tmpDate2) - &DateLocal($tmpDate1);
}

###############################################################################
#
#18位身份证号的验证
#我国新版18位身份证号的最后一位是校验位,根据 MOD 11-2算法,计算出来的
#简单的说就是前17位的权值依次为循环(7,9,10,5,8,4,2,1,6,3),首先加权计算和
#也就是:ID[1] * 7 + ID[2] * 9 + ... + ID[9] * 6 + ID[10] * 3 + ID[11] * 7 + ... + ID[16] * 4 + ID[17] * 2
#	      --^--^-^------^--^--
#重新回到7,9,10... 算出来的和,对11取模,余数是[0~10]间的一个值
#然后按照以下关系,得到最后一位校验位的值:
#'0'  =>  '1','1'  =>  '0','2'  =>  'X','3'  =>  '9','4'  =>  '8',
#'5'  =>  '7','6'  =>  '6','7'  =>  '5','8'  =>  '4','9'  =>  '3','10' =>  '2'
###############################################################################
#对17位或者18位长度的身份证计算校验位
sub IdenCardParityBit {
   my ($tmpCardID)=@_;

   my %tmpMap=('0'  =>  '1',
               '1'  =>  '0',
               '2'  =>  'X',
               '3'  =>  '9',
               '4'  =>  '8',
               '5'  =>  '7',
               '6'  =>  '6',
               '7'  =>  '5',
               '8'  =>  '4',
               '9'  =>  '3',
               '10' =>  '2');

   my (@tmpParityBitArray,$tmpParityBit);
   #生成校验位信息
   @tmpParityBitArray=split(//,$tmpCardID);
   $tmpParityBit=$tmpMap{($tmpParityBitArray[0 ] * 7  +
                          $tmpParityBitArray[1 ] * 9  +
                          $tmpParityBitArray[2 ] * 10 +
                          $tmpParityBitArray[3 ] * 5  +
                          $tmpParityBitArray[4 ] * 8  +
                          $tmpParityBitArray[5 ] * 4  +
                          $tmpParityBitArray[6 ] * 2  +
                          $tmpParityBitArray[7 ] * 1  +
                          $tmpParityBitArray[8 ] * 6  +
                          $tmpParityBitArray[9 ] * 3  +
                          $tmpParityBitArray[10] * 7  +
                          $tmpParityBitArray[11] * 9  +
                          $tmpParityBitArray[12] * 10 +
                          $tmpParityBitArray[13] * 5  +
                          $tmpParityBitArray[14] * 8  +
                          $tmpParityBitArray[15] * 4  +
                          $tmpParityBitArray[16] * 2) % 11};
   return $tmpParityBit;
}
# end IdenCardParityBit


###############################################################################
#身份证信息的转换与验证
#返回一个数组[0]身份证号[1]是否有效标志
###############################################################################
sub IdenCardConvert {
   my ($tmpCardID)=@_;

   my (@tmpCardIDInfo);

   #$flag=1表示是有效身份证,flag=0表示是无效身份证信息
   my ($flag,$tmpParityBit)=(0,'');

   $tmpCardID =~ s/\s//g;
   $tmpCardID =~ tr[a-z][A-Z];
   #验证身份证号码
   #一种情况是全部由数字组成
   #一种情况是前面全部是数字,最后一个是字母 X
   if (($tmpCardID =~ /^\d+$/) || ($tmpCardID =~ /^\d+X$/))
   {
      if(length($tmpCardID) eq 18)
      {
         $tmpParityBit = IdenCardParityBit($tmpCardID);

         #验证18位身份证的校验位是否正确
         $flag = 1 if ($tmpParityBit eq substr($tmpCardID,17));
      }elsif(length($tmpCardID) eq 15 || length($tmpCardID) eq 17)
      {
         #15位转化成17位
         if (length($tmpCardID) eq 15)
         {
            $tmpCardID =~ /^(.{6})(.+)$/;
            $tmpCardID = $1."19".$2;
         }

         #生成校验位信息
         $tmpParityBit = IdenCardParityBit($tmpCardID);
         $tmpCardID = $tmpCardID.$tmpParityBit;

         $flag = 1;
      }
   }# (($tmpCardID =~ /^\d+$/) || ($tmpCardID =~ /^\d+X$/))

   $tmpCardIDInfo[0]=$tmpCardID;
   $tmpCardIDInfo[1]=$flag;

   return \@tmpCardIDInfo;
}

1;
