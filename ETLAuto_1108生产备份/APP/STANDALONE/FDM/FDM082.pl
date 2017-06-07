#!/usr/bin/perl
#---------------
#Author:Liuxz
#Function:将dw_sdata.cos_000_sectype where transtype='SE' and owner='1245'的叶子节点（不包括中间节点）全部递归出来，并插入F_FDM.CD_CD_TABLE作为FDM082代码类
#---------------
# ------------ program section ------------
use strict;
use bdp_pub;
use etl_unix;
my ${DB_WORK_DATE};
my $VSQL_CFG_NAME = 'FDM_VSQL';  #app_config.ini中VSQL连接配置的名称 
if ( $#ARGV < 0 ) {
   print "Usage: [perl 程序名 Control_File] (Control_File format: YYYYMMDD) \n";
   print "\n";
   exit(1);
}

# Get the first argument
my ${CONTROL_FILE} = $ARGV[0];
my $ret=BDP::parseDirInfo(${CONTROL_FILE});
${DB_WORK_DATE}=$ret->{TXDATE};
#print "db_work_date= ${DB_WORK_DATE}\n";
=pod
if (${CONTROL_FILE} =~/[0-9]{8}($|\.)/) {
   ${DB_WORK_DATE} = substr($&,0,8);
}
else{
   print "Usage: [perl 程序名 Control_File] (Control_File format: YYYYMMDD) \n";
   print "\n";
   exit(1);
}
=cut
;


#-------------获取1245下的所有叶子节点
#my $vsql = $ENV{"VSQL_PATH"};
my $VSQL_NODE_COUNT = int(ETL::getAppConfig($VSQL_CFG_NAME,'VSQL_NODE_COUNT'));
my $VSQL_IP_PREFIX = ETL::getAppConfig($VSQL_CFG_NAME,'VSQL_IP_PREFIX');
my $VSQL_IP_START = int(ETL::getAppConfig($VSQL_CFG_NAME,'VSQL_IP_START'));
my $randomnumber = int(rand($VSQL_NODE_COUNT))+$VSQL_IP_START;
$ENV{'VSQL_HOST'} = "${VSQL_IP_PREFIX}$randomnumber";
#print 'VSQL_HOST:', $ENV{'VSQL_HOST'}, "\n";
$ENV{'VSQL_PORT'} = ETL::getAppConfig($VSQL_CFG_NAME,'VSQL_PORT');
#print "VSQL_PORT is $ENV{'VSQL_PORT'}\n";
$ENV{'VSQL_DATABASE'} = ETL::getAppConfig($VSQL_CFG_NAME,'VSQL_DATABASE');
#print "VSQL_DATABASE is $ENV{'VSQL_DATABASE'}";
$ENV{'VSQL_USER'} = ETL::getAppConfig($VSQL_CFG_NAME,'VSQL_USER');
#print "VSQL_USER is $ENV{'VSQL_USER'}";
$ENV{'VSQL_PASSWORD'} = ETL::Decrypt(ETL::getAppConfig($VSQL_CFG_NAME,'VSQL_PASSWORD'));
#print "VSQL_PASSWORD is $ENV{'VSQL_PASSWORD'}\n";
my %hash;
my $connect_string = "vsql -Aqt -F '^' ";
my $count=1;
my @result_list; 
my $lastresult='';
my $all;
my @all_array;
my @column;
my @column_array;
my $column_ref;
my $key_result;
my $record;
my @column0;
my @column_array;
my @thekey;
my @son;
my @result_list;
my @son_result;

#retrive all records from cos_000_sectype to $all
$all = &run_vsql_comm("select thekey,name,owner,thelevel from dw_sdata.cos_000_sectype where transtype='SE' and start_dt<='${DB_WORK_DATE}'::date and end_dt>'${DB_WORK_DATE}'::date",$connect_string);

#define son-nodes-getting function
sub son{
    @son='';
    my $thekey=shift;
    foreach my $key (keys %hash){
        if($hash{$key}->[1] == $thekey){
            push @son,"$key\n";
        }
}
    return @son
}

#split $all by \n into @all_array
@all_array=split(/\n/,$all); 

#loop @all_array 
foreach (@all_array){
    @column=split(/\^/,$_);  #split $record by ^ into @column
    my @column_array=($column[1],$column[2],$column[3]);
    $column_ref=\@column_array; 
    $hash{$column[0]}=$column_ref;
}

# Get the son nodes of 1245
@son_result=&son('1245');
foreach(@son_result){
    chomp $_;
    print $_;
    print "Begin...\n";
    &findx($_)
}

#loop the @result_list,and constitute the non-empty results to a string, to be used by sql statement.
foreach(@result_list){
    if ($_ ne ''){
    $lastresult = "'".$_."'".",".$lastresult;
    
}
    #print $lastresult;
}
chop $lastresult;

#To judge if thelevel field is G when $thekey as owner
sub isyezi{
	 my $thekey = shift;
     if ($hash{$thekey}->[2] eq 'G'){
	    return 'G'
	}
	return 0
}

#To get the recursive result of thekey when thelevel is G,otherwise push the thekey to @result_list once thelevel is T
sub findx{
	 my $thekey = shift ;
	 if (&isyezi($thekey) ne 'G'){
	    push @result_list,$thekey;
	 	return
	 } 
	 my @result=&son($thekey);
     foreach my $thekey1(@result){ 
     	 #chomp $thekey1;
      	 chomp $thekey1;
	     findx($thekey1);
	   }
	}
	
#Run vsql function
sub run_vsql_comm{
 my ($sql,$string) = @_ ;  
my $result = `$string<<EOF
\\timing
\\set AUTOCOMMIT off 
\\set ON_ERROR_STOP on 
 $sql 
 \\q
EOF`
;

my $exitcode = $? >> 8 ;


my $RET_CODE = $? >> 8 ;
if($RET_CODE == 0 ){
   return $result;
   }
   else{
   return -1;
   }
}

my $sqltext="
/*数据回退区*/
delete from f_fdm.CD_CD_TABLE
where Cd_Typ_Encd = 'FDM082';
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_fdm.CD_CD_TABLE
(Cd_Typ_Encd												--代码类型编码
,Cd_Typ_Cn_Desc									            --代码类型中文描述
,Cd                    							            --代码值
,Cd_Desc						                            --代码描述
,Memo							                            --备注
,Cd_Load_Mode											    --代码加载方式
,Modi_Dt									                --修改日期
,Modi_Reasn								                    --修改原因
)
SELECT	'FDM082'											AS	Cd_Typ_Encd
	   ,'资金存放产品代码'									AS	Cd_Typ_Cn_Desc
	   ,thekey												AS	Cd
	   ,name												AS	Cd_Desc
	   ,''													AS	Memo
	   ,'2'										            AS	Cd_Load_Mode										          
	   ,'${DB_WORK_DATE}'::date 					        AS	Modi_Dt
	   ,''												    AS	Modi_Reasn
from dw_sdata.cos_000_sectype
where thekey in (${lastresult})
and start_dt<='${DB_WORK_DATE}'::date and end_dt>'${DB_WORK_DATE}'::date
;
commit
;
/*数据处理区END*/
"
;
print "$sqltext\n";
&run_vsql_comm($sqltext,$connect_string);
