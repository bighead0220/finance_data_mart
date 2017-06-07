#!/bin/usr/perl
#------------------------------------------------------------------------
#--PCRM invoke online check
#--Script : online_check.pl
#--Date:2015/06/24
#--使用说明：perl online_check.pl 
#--逻辑说明：1、检查配置SQL文件中source table 与文件中的真正引用的表是否对应。
#             
#               
#-------------------------------------------------------------------------
use strict; 
use Config::IniFiles;
use warnings;
use Time::localtime;
use Config::IniFiles; 
use File::Basename;
 

# 1、检查sql脚本中的SOURCE TABLE与文件引用中的表是否对应
# 2、检查controlM依赖条件
#
#
#
my $sqlpath = '/etl/ETLAuto/APP/SQLSCRIPT/FDM/';
my $binpath = '/etl/ETLAuto/APP/PUBLIC/FDM/';
my @source_tables;
my @all_tables;
my $target_table='';
my %HashTable; 
my $query_gp_string = "vsql -h 22.224.65.2 -d CPCIMDB -U dbadmin -w Cms!123 -t";
#查询 列信息的sql 
my $sql = " select upper(table_name) table_name										
              from tables a	 
              where lower(table_schema) in ('f_fdm','f_adm','f_rdm','dw_sdata')											
              order by 1	"; 
my @columns = `$query_gp_string<<ENDOFDROP
     $sql 
     \\q
ENDOFDROP`; 

chop @columns; 
 
sub trim{	
	  my @array = @_;
	  foreach(@array){
	     s/^\s+|\s+$//g ; 
	  }	
	  return @array ;
	}
	
sub get_sourcetable{
 
	opendir(DIR,$sqlpath) || die "Cann't Open $sqlpath $! ";
	my $flag=0;
	while( my $file=readdir(DIR) ){
		 
		 @source_tables=(); 
		 my $line;
		 my @split_line;
		 
	   next if $file =~ /^\.{1,2}$/;
	   my $cmd = ' awk -f '.$binpath.'/strip_sql_comment.awk '.$sqlpath.$file .'> /tmp/'.$file;
	   system($cmd);
	   #获得脚本使用的所有表
	   for(my $num=0;$num<@columns;$num++){
	   	   next if $columns[$num] eq '';        
         my $cmd = 'grep -iwq '.$columns[$num].' /tmp/'.$file .'&& echo 0 || echo 1 ';
         my $result = `$cmd`;
         next if $result==1;
         push(@source_tables,trim($columns[$num]));
         #if () ne trim($columns[$num])){
         #print uc(substr($file,0,length($file)-4)) ." $columns[$num] \n"  ;
       }
     
     my $targetname;
     
     uc(substr($file,length($file)-3)) eq '.PL' ? $targetname = uc(substr($file,0,length($file)-2)) :  $targetname = uc(substr($file,0,length($file)-4));
     #print uc(substr($file,length($file)-3)) eq '.PL' ? $targetname = uc(substr($file,0,length($file)-3)) :  $targetname = uc(substr($file,0,length($file)-4)) ,"\n";
     #print uc(substr($file,length($file)-3));
     #print "uc(substr($file,length($file)-3))------: $targetname";
     #w变量存储在脚本中引用但是在属于手动加载不需要配置在调度依赖中去的。
     my @w =('CD_RF_STD_CD_TRAN_REF');
     foreach my $var(@source_tables){
     	next if $target_table =~ /^FDM/ && $var eq 'CD_CD_TABLE';
        next if uc($var) eq $target_table ;
     	next if index($targetname,$var) eq '0';
     	next if grep  {$_ eq uc($var)} @w ;
     	
     	if($var =~ /^F_|^CD_CD_TABLE/ ){
     		$var ='[FDM]'.$var; 
     	}else{
     		$var ='DWS_00_'.$var; 
     		}#||
        
     	print " FDM_00_$targetname = $var \n"if('[FDM]'.$targetname ne $var ) ; 
      }
      
      }
	closedir(DIR);
	
 }
 

get_sourcetable();
print "FDM082 FDM086 需要单独处理";
	
