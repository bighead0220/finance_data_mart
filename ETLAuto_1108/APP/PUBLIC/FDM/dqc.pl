#!/usr/bin/perl
# dqc.pl



my @result = `/opt/vertica/bin/vsql -v ON_ERROR_STOP=1 -v AUTOCOMMIT=OFF -d CPCIMDB -h 192.168.2.44 -U dbadmin -w vertica -t<<eof
select RULE_ID,'select '||field_en_name || ' from '||tab_en_name ||' where 1=2 and etl_date =date group by '||field_en_name||' having count(1) <> 1 ; '  from f_app.T_RD_VALIDATE_RULE where rule_type = 'C' 
;
\\q
eof`;
#print @result;
#chomp @result;

#my %hashsql ;

print "------------\n";
foreach my $row(@result)
{ 
  next if &trim($row) eq '';
  ($key,$val) = split (/\|/,$row);
  
  my $r = `/opt/vertica/bin/vsql -v ON_ERROR_STOP=1 -v AUTOCOMMIT=OFF -d CPCIMDB -h 192.168.2.44 -U dbadmin -w vertica -t -`  





}





















sub trim{	
	  my @array = @_;
	  foreach(@array){
	     s/^\s+|\s+$//g ; 
	  }	
	  return @array ;
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
