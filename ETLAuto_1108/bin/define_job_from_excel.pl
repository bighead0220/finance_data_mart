#!/usr/bin/perl -W
use strict;
use warnings;
use Unicode::Map;
use Spreadsheet::ParseExcel;
use Spreadsheet::ParseExcel::FmtUnicode;
use File::Path;

if (!defined($ENV{AUTO_HOME}) || $ENV{AUTO_HOME} eq '') {
   print "AUTO_HOME not defined, program terminated!\n";
   exit 1;
}

unshift(@INC, "$ENV{AUTO_HOME}/lib");
require etl_unix;

my $AUTO_HOME = $ENV{"AUTO_HOME"};

unless($ARGV[0]) {
   die "Usage : $0 [ExcelFilename]\n";
}

open(STDERR, ">&STDOUT");

my $filename = "$AUTO_HOME/LOG/define_job_". ETL::getCurrentFullDate() . ".log";
open LOGF,">>$filename" || die "Open $filename failed:$!";

my $local_enc = "CP936";
$filename = $ARGV[0];

my $calendarbu = ETL::getCurrentDateTime1();
print LOGF ETL::getCurrentDateTime() . "  " . "\n******************************";
print LOGF ETL::getCurrentDateTime() . "  " . "Start parsing file $filename\n";
print STDOUT "Start parsing file $filename\n";
print STDOUT "CalendarBU:${calendarbu}\n";

my $oFmtJ = Spreadsheet::ParseExcel::FmtUnicode->new(Unicode_Map => $local_enc);
my $excel = Spreadsheet::ParseExcel->new();
my $book = $excel->Parse($filename, $oFmtJ);
my $last_sheet = $book->{SheetCount} - 1;
my ($sheetname, @row, @col, @cellvalue, $row);
my ($dbh, $ins_sys, $ins_job, $ins_job_source, $ins_job_timewindow, $ins_job_dependency, $ins_job_stream);
my ($del_sys, $del_job, $del_job_source, $del_job_timewindow, $del_job_dependency, $del_job_stream);
my ($ins_job_calendar, $del_job_calendar, $upd_job_calendar);
my ($ins_job_group, $ins_job_groupchild, $del_job_group, $del_job_groupchild, $query_job, $retore_job);
my ($ins_script, $ins_job_step, $del_script, $del_job_step, $ins_job_related, $del_job_related, $query_job_source);
my ($query_job_calendar, $query_job_groupchild);
my ($del_job_trigger, $ins_job_trigger, $upd_job_type, $upd_timetrigger);
my ($del_job_trigger_id, $ins_job_trigger_id);
my ($del_job_trigger_calc, $ins_job_trigger_calc);
my ($del_job_trigger_week, $ins_job_trigger_week);
my ($del_job_trigger_month, $ins_job_trigger_month);
my ($del_job_trigger_interval, $ins_job_trigger_interval);
my ($del_group, $ins_group, $del_server_group, $ins_server_group);
my (@server, @script, @sys, @job, @jobstep, @jobdependency, @jobstream, @jobgroup, @datacalendar, @jobrelated, @triggerplan, @jobtrigger, @servergroup);
my (%oldcalendar, %oldgroupchild, %oldjobsource);
my (%source, %job, %server, %triggerplan);

my $AUTO_SERVER_IP;
my $AUTO_AGENT_PORT;
my $PRIMARY_SERVER;

my $AUTO_SERVER = $ETL::ETL_SERVER;
if ( !defined($AUTO_SERVER) || $AUTO_SERVER eq '' ) {
   print LOGF ETL::getCurrentDateTime() . "  " . "Could not get AUTO_SERVER configuration, terminate this program!\n";
   print STDERR "Could not get AUTO_SERVER configuration, terminate this program!\n";
   exit(1);
}

sub db_init {
   my $sth;
   my ($sys, $job,$server,$jobtype);
   print STDOUT "Initializing Database Connection\n";
   print LOGF ETL::getCurrentDateTime() . "  " . "Initializing Database Connection\n";

   $ENV{'NLS_LANG'} = 'AMERICAN_AMERICA.ZHS16GBK';
   $dbh = ETL::connectETL();
   unless ( $dbh ) {
      print LOGF ETL::getCurrentDateTime() . "  " . "Unable to connect database\n";
      print STDERR "Unable to connect database\n";
      exit 1;
   }

   ($AUTO_SERVER_IP, $AUTO_AGENT_PORT, $PRIMARY_SERVER) = ETL::getServerInfo($dbh, $AUTO_SERVER);

   print STDOUT "Preparing SQL Statement\n";
   print LOGF ETL::getCurrentDateTime() . "  " . "Preparing SQL Statement\n";
   $ins_group = $dbh->prepare("insert into ETL_Group(ETL_Group,Description) values(?,'')") || die $DBI::errstr;
   $del_group = $dbh->prepare("delete from ETL_Group where ETL_Group=?") || die $DBI::errstr;

   $ins_server_group = $dbh->prepare("insert into ETL_Server_Group(ETL_Group,ETL_Server,Enable) values(?,?,'1')") || die $DBI::errstr;
   $del_server_group = $dbh->prepare("delete from ETL_Server_Group where ETL_Group=? and ETL_Server=?") || die $DBI::errstr;

   $ins_script = $dbh->prepare("insert into ETL_Script(ScriptID,FilePath,FileName,ScriptType,Description) values(?,?,?,?,?)") || die $DBI::errstr;
   $del_script = $dbh->prepare("delete from ETL_Script where ScriptID=?") || die $DBI::errstr;

   $ins_sys = $dbh->prepare("insert into ETL_Sys(ETL_System,Description,DataKeepPeriod,LogKeepPeriod,RecordKeepPeriod,Priority,Concurrency) values(?,?,?,?,?,?,?)") || die $DBI::errstr;
   $del_sys = $dbh->prepare("delete from ETL_Sys where ETL_System=?") || die $DBI::errstr;

   $query_job = $dbh->prepare("select Enable,Last_StartTime,Last_EndTime,Last_JobStatus,Last_TXDate,Last_FileCnt,CalendarBU,RunningScript,JobSessionID,ExpectedRecord,Last_ETL_Server,Last_JobState from ETL_Job where ETL_System=? and ETL_Job=?") || die $DBI::errstr;
   $retore_job = $dbh->prepare("update ETL_Job set Enable=?,Last_StartTime=?,Last_EndTime=?,Last_JobStatus=?,Last_TXDate=?,Last_FileCnt=?,CalendarBU=?,RunningScript=?,JobSessionID=?,ExpectedRecord=?,Last_ETL_Server=?,Last_JobState=? where ETL_System=? and ETL_Job=?") || die $DBI::errstr;

   $ins_job = $dbh->prepare("insert into ETL_Job(ETL_System,ETL_Job,ETL_Server,Description,Frequency,JobType,Enable,Last_JobStatus,CubeFlag,CheckCalendar,AutoOff,JobSessionID,CheckLastStatus,TimeTrigger,Priority,AllowJumpFlag,CalendarBU) values(?,?,?,?,?,?,'1','Ready','N','N','N',0,'Y','N',?,?,?)") || die $DBI::errstr;
   $del_job = $dbh->prepare("delete from ETL_Job where ETL_System=? and ETL_Job=?") || die $DBI::errstr;

   $ins_job_source = $dbh->prepare("insert into ETL_Job_Source(Source,ETL_System,ETL_Job,Conv_File_Head,AutoFilter,Alert,BeforeHour,BeforeMin,OffsetDay,LastCount) values(?,?,?,?,'0','0',0,0,0,?)") || die $DBI::errstr;
   $del_job_source = $dbh->prepare("delete from ETL_Job_Source where Source=?") || die $DBI::errstr;
   $query_job_source = $dbh->prepare("select LastCount from ETL_Job_Source where Source=? and LastCount<>0") || die $DBI::errstr;

   $ins_job_step = $dbh->prepare("insert into ETL_Job_Step(ETL_System,ETL_Job,JobStepID,ScriptID,ScriptFile,Description,Enable) values(?,?,?,?,?,?,'1')") || die $DBI::errstr;
   $del_job_step = $dbh->prepare("delete from ETL_Job_Step where ETL_System=? and ETL_Job=? and JobStepID=?") || die $DBI::errstr;

   $ins_job_timewindow = $dbh->prepare("insert into ETL_Job_TimeWindow(ETL_System,ETL_Job,Allow,BeginHour,EndHour) values(?,?,'Y',?,?)") || die $DBI::errstr;
   $del_job_timewindow = $dbh->prepare("delete from ETL_Job_TimeWindow where ETL_System=? and ETL_Job=?") || die $DBI::errstr;

   $ins_job_dependency = $dbh->prepare("insert into ETL_Job_Dependency(ETL_System,ETL_Job,Dependency_System,Dependency_Job,Enable) values(?,?,?,?,'1')") || die $DBI::errstr;
   $del_job_dependency = $dbh->prepare("delete from ETL_Job_Dependency where ETL_System=? and ETL_Job=? and Dependency_System=? and Dependency_Job=?") || die $DBI::errstr;

   $ins_job_stream = $dbh->prepare("insert into ETL_Job_Stream(ETL_System,ETL_Job,ReturnCode,Stream_System,Stream_Job,Enable) values(?,?,0,?,?,'1')") || die $DBI::errstr;
   $del_job_stream = $dbh->prepare("delete from ETL_Job_Stream where ETL_System=? and ETL_Job=? and Stream_System=? and Stream_Job=? and ReturnCode=0") || die $DBI::errstr;

   $ins_job_calendar = $dbh->prepare("insert into DataCalendar(ETL_System,ETL_Job,CalendarYear,SeqNum,CalendarMonth,CalendarDay,CheckFlag) values(?,?,?,?,?,?,?)") || die $DBI::errstr;
   $del_job_calendar = $dbh->prepare("delete from DataCalendar where ETL_System=? and ETL_Job=?") || die $DBI::errstr;
   $upd_job_calendar = $dbh->prepare("update ETL_Job set CheckCalendar=? where ETL_System=? and ETL_Job=?") || die $DBI::errstr;
   $query_job_calendar = $dbh->prepare("select LPAD(to_char(CalendarYear),4,0)||LPAD(to_char(CalendarMonth),2,0)||LPAD(to_char(CalendarDay),2,0),CheckFlag from DataCalendar where ETL_System=? and ETL_Job=?") || die $DBI::errstr;

   $ins_job_group = $dbh->prepare("insert into ETL_Job_Group(GroupName,Description,ETL_System,ETL_Job,AutoOnChild) values(?,'',?,?,'Y')") || die $DBI::errstr;
   $del_job_group = $dbh->prepare("delete from ETL_Job_Group where GroupName=?") || die $DBI::errstr;

   $ins_job_groupchild = $dbh->prepare("insert into ETL_Job_GroupChild(GroupName,ETL_System,ETL_Job,Description,Enable,CheckFlag,TXDate,TurnOnFlag) values(?,?,?,'','1',?,?,'N')") || die $DBI::errstr;
   $del_job_groupchild = $dbh->prepare("delete from ETL_Job_GroupChild where GroupName=? and ETL_System=? and ETL_Job=?") || die $DBI::errstr;
   $query_job_groupchild = $dbh->prepare("select CheckFlag,TXDate from ETL_Job_GroupChild where GroupName=? and ETL_System=? and ETL_Job=? and (CheckFlag IS NOT NULL or TXDate IS NOT NULL)") || die $DBI::errstr;

   $ins_job_related = $dbh->prepare("insert into ETL_RelatedJob(ETL_System,ETL_Job,RelatedSystem,RelatedJob,CheckMode,OffsetDay,Description,Enable) values(?,?,?,?,'0',?,null,'1')") || die $DBI::errstr;
   $del_job_related = $dbh->prepare("delete from ETL_RelatedJob where ETL_System=? and ETL_Job=? and RelatedSystem=? and RelatedJob=?") || die $DBI::errstr;

   $ins_job_trigger = $dbh->prepare("insert into ETL_TimeTrigger(ETL_System,ETL_Job,TriggerID,StartHour,StartMin) values(?,?,?,?,?)") || die $DBI::errstr;
   $del_job_trigger = $dbh->prepare("delete from ETL_TimeTrigger where ETL_System=? and ETL_Job=?") || die $DBI::errstr;

   $ins_job_trigger_id = $dbh->prepare("insert into ETL_TimeTrigger_Plan(TriggerID,TriggerType,Description,StartHour,StartMin,OffsetDay) values(?,?,?,?,?,?)") || die $DBI::errstr;
   $del_job_trigger_id = $dbh->prepare("delete from ETL_TimeTrigger_Plan where TriggerID=?") || die $DBI::errstr;

   $ins_job_trigger_calc = $dbh->prepare("insert into ETL_TimeTrigger_Calendar(TriggerID,YearNum,MonthNum,DayNum) values(?,?,?,?)") || die $DBI::errstr;
   $del_job_trigger_calc = $dbh->prepare("delete from ETL_TimeTrigger_Calendar where TriggerID=?") || die $DBI::errstr;

   $ins_job_trigger_week = $dbh->prepare("insert into ETL_TimeTrigger_Weekly(TriggerID,Timing) values(?,?)") || die $DBI::errstr;
   $del_job_trigger_week = $dbh->prepare("delete from ETL_TimeTrigger_Weekly where TriggerID=?") || die $DBI::errstr;

   $ins_job_trigger_month = $dbh->prepare("insert into ETL_TimeTrigger_Monthly(TriggerID,Timing,EndOfMonth) values(?,?,?)") || die $DBI::errstr;
   $del_job_trigger_month = $dbh->prepare("delete from ETL_TimeTrigger_Monthly where TriggerID=?") || die $DBI::errstr;

   $ins_job_trigger_interval = $dbh->prepare("insert into ETL_TimeTrigger_Interval(TriggerID,Interval_Minutes) values(?,?)") || die $DBI::errstr;
   $del_job_trigger_interval = $dbh->prepare("delete from ETL_TimeTrigger_Interval where TriggerID=?") || die $DBI::errstr;

   $upd_job_type = $dbh->prepare("update ETL_Job set JobType=? where ETL_System=? and ETL_Job=?") || die $DBI::errstr;
   $upd_timetrigger = $dbh->prepare("update ETL_Job set TimeTrigger='Y' where ETL_System=? and ETL_Job=?") || die $DBI::errstr;

   print LOGF ETL::getCurrentDateTime() . "  " . "Read current job info from repository\n";
   print STDOUT "Read current job info from repository\n";
   $sth = $dbh->prepare("select ETL_System,ETL_Job,ETL_Server,JobType from ETL_Job") || die $DBI::errstr;
   $sth->execute();
   while (($sys,$job,$server,$jobtype) = $sth->fetchrow()) {
      $job{"${sys}_${job}"}->{'server'} = $server;
      $job{"${sys}_${job}"}->{'jobtype'} = $jobtype;
   }
   $sth->finish();

   print LOGF ETL::getCurrentDateTime() . "  " . "Read server && group info from repository\n";
   print STDOUT "Read server && group info from repository\n";
   $sth = $dbh->prepare("select ETL_Server from ETL_Server union all select ETL_Group from ETL_Group") || die $DBI::errstr;
   $sth->execute();
   while (($server) = $sth->fetchrow()) {
      $server{$server} = $server;
   }
   $sth->finish();
}

sub define_server_group {
   return if ($PRIMARY_SERVER eq '0');
   print STDOUT "Create Server Group...";
   print LOGF ETL::getCurrentDateTime() . "  " . "Create Server Group ...\n";
   my $last_group = '';
   my $count = 0;
   foreach my $group (sort {$a->[1] cmp $b->[1]} @servergroup) {
      if (!exists($server{$group->[0]})) {
         print LOGF ETL::getCurrentDateTime() . "  " . "WARNING!:Server not exists: $group->[0], ignored.\n";
         print STDOUT "WARNING!:Server not exists: $group->[0], ignored.\n";
         next;
      }
      if ($last_group ne $group->[1] && !exists($server{$group->[1]})) {
         $del_group->bind_param(1, $group->[1]);
         $del_group->execute() || die $DBI::errstr;
         $del_group->finish();

         $ins_group->bind_param(1, $group->[1]);
         $ins_group->execute() || die $DBI::errstr;
         $ins_group->finish();

         $server{$group->[1]} = $group->[1];
         print LOGF ETL::getCurrentDateTime() . "  " . "Defined Group:$group->[1]\n";
      }

      $del_server_group->bind_param(1, $group->[1]);
      $del_server_group->bind_param(2, $group->[0]);
      $del_server_group->execute() || die $DBI::errstr;
      $del_server_group->finish();

      $ins_server_group->bind_param(1, $group->[1]);
      $ins_server_group->bind_param(2, $group->[0]);
      $ins_server_group->execute() || die $DBI::errstr;
      $ins_server_group->finish();

      $last_group = $group->[1];
      print LOGF ETL::getCurrentDateTime() . "  " . "Defined Server Group $group->[1],$group->[0]\n";
      $count++;
   }
   print STDOUT "$count rows loaded.\n";
   print LOGF ETL::getCurrentDateTime() . "  " . "$count rows loaded.\n";
}

sub define_script {
   print STDOUT "Create script...";
   print LOGF ETL::getCurrentDateTime() . "  " . "Create script ...\n";
   my %script = ('perl' => 'pl','python' => 'py','java' => 'jar','shell' => 'sh');
   my $count = 0;
   foreach my $src (@script) {
      $src->[1] =~ s/\/$//;
      $source{$src->[0]}->{'ScriptType'} = $script{lc($src->[3])};
      $source{$src->[0]}->{'ScriptFile'} = "${AUTO_HOME}$src->[1]/$src->[2]";
      unless(-f $source{$src->[0]}->{'ScriptFile'}) {
         print LOGF ETL::getCurrentDateTime() . "  " . "WARNING!:Script not exists: $source{$src->[0]}->{'ScriptFile'}\n";
         print STDOUT "WARNING!:Script not exists: $source{$src->[0]}->{'ScriptFile'}\n";
      }
      next if ($PRIMARY_SERVER eq '0');

      $del_script->bind_param(1, $src->[0]);
      $del_script->execute() || die $DBI::errstr;
      $del_script->finish();

      $ins_script->bind_param(1, $src->[0]);
      $ins_script->bind_param(2, $src->[1]);
      $ins_script->bind_param(3, $src->[2]);
      $ins_script->bind_param(4, $script{lc($src->[3])});
      $ins_script->bind_param(5, $src->[4]);
      $ins_script->execute() || die $DBI::errstr;
      $ins_script->finish();
      print LOGF ETL::getCurrentDateTime() . "  " . "Defined script [$src->[0]],[$source{$src->[0]}->{'ScriptFile'}]\n";
      $count++;
   }
   print STDOUT "$count rows loaded.\n";
   print LOGF ETL::getCurrentDateTime() . "  " . "$count rows loaded.\n";
}

sub define_sys {
   return if ($PRIMARY_SERVER eq '0');
   print STDOUT "Create System...";
   print LOGF ETL::getCurrentDateTime() . "  " . "Create System ...\n";
   my $count = 0;
   foreach my $sys (@sys) {
      $del_sys->bind_param(1, $sys->[0]);
      $del_sys->execute() || die $DBI::errstr;
      $del_sys->finish();

      $ins_sys->bind_param(1, $sys->[0]);
      $ins_sys->bind_param(2, $sys->[1]);
      $ins_sys->bind_param(3, $sys->[2] eq '' ? 30 : $sys->[2]);
      $ins_sys->bind_param(4, $sys->[3] eq '' ? 30 : $sys->[3]);
      $ins_sys->bind_param(5, $sys->[4] eq '' ? 30 : $sys->[4]);
      $ins_sys->bind_param(6, $sys->[5] eq '' ? 0 : $sys->[6]);
      $ins_sys->bind_param(7, $sys->[6] eq '' ? 10  : $sys->[5]);
      $ins_sys->execute() || die $DBI::errstr;
      $ins_sys->finish();
      print LOGF ETL::getCurrentDateTime() . "  " . "Defined System $sys->[0]\n";
      $count++;
   }
   print STDOUT "$count rows loaded.\n";
   print LOGF ETL::getCurrentDateTime() . "  " . "$count rows loaded.\n";
}

sub define_job {
   return if ($PRIMARY_SERVER eq '0');
   print STDOUT "Create Job...";
   print LOGF ETL::getCurrentDateTime() . "  " . "Create Job ...\n";
   my $jobtype;
   my $count = 0;
   foreach my $job (@job) {
      if ($job->[3] ne '' && $job->[3] ne 'ALL_NODES' && $job->[3] ne 'TRIGGER_NODE' && !exists($server{$job->[3]})) {
         print LOGF ETL::getCurrentDateTime() . "  " . "WARNING!:Server not exists: $job->[3], ignored.\n";
         print STDOUT "WARNING!:Server not exists: $job->[3], ignored.\n";
         next;
      }
      if ($job->[4] eq '') {
         $job->[4] = '0';
      }
      my @freq = split(/,/, $job->[4]);
      if (@freq == 1 && $freq[0] eq '0') {
         $jobtype = 'D';
      } elsif ($freq[0] >= 41 && $freq[0] <= 47) {
         $jobtype = 'W';
      } elsif (($freq[0] >= 1 && $freq[0] <= 31) || $freq[0] == -1) {
         $jobtype = 'M';
      } else {
         $jobtype = 'D';
      }
      $job{"$job->[0]_$job->[1]"}->{'server'} = $job->[3] eq '' ? 'ALL_NODES' : $job->[3];
      $job{"$job->[0]_$job->[1]"}->{'jobtype'} = $jobtype;

      $query_job->bind_param(1, $job->[0]);
      $query_job->bind_param(2, $job->[1]);
      $query_job->execute() || die $DBI::errstr;
      my @jobinfo = $query_job->fetchrow();
      $query_job->finish();

      $del_job->bind_param(1, $job->[0]);
      $del_job->bind_param(2, $job->[1]);
      $del_job->execute() || die $DBI::errstr;
      $del_job->finish();

      $ins_job->bind_param(1, $job->[0]); #ETL_System
      $ins_job->bind_param(2, $job->[1]); #ETL_Job
      $ins_job->bind_param(3, $job->[3] eq '' ? 'ALL_NODES' : $job->[3]); #ETL_Server
      $ins_job->bind_param(4, $job->[2]); #Description
      $ins_job->bind_param(5, $job->[4]); #Frequency
      $ins_job->bind_param(6, $jobtype); #JobType
      $ins_job->bind_param(7, $job->[7] eq '' ? 0 : $job->[7]); #Priority
      $ins_job->bind_param(8, $job->[8] eq '是' ? 1 : 0); #AllowJumpFlag
      $ins_job->bind_param(9, $calendarbu); #CalendarBU
      $ins_job->execute() || die $DBI::errstr;
      $ins_job->finish();

      if (defined $jobinfo[0] && $jobinfo[0] ne '') {
         $retore_job->bind_param(1, $jobinfo[0]);
         $retore_job->bind_param(2, $jobinfo[1]);
         $retore_job->bind_param(3, $jobinfo[2]);
         $retore_job->bind_param(4, $jobinfo[3]);
         $retore_job->bind_param(5, $jobinfo[4]);
         $retore_job->bind_param(6, $jobinfo[5]);
         $retore_job->bind_param(7, $jobinfo[6]);
         $retore_job->bind_param(8, $jobinfo[7]);
         $retore_job->bind_param(9, $jobinfo[8]);
         $retore_job->bind_param(10, $jobinfo[9]);
         $retore_job->bind_param(11, $jobinfo[10]);
         $retore_job->bind_param(12, $jobinfo[11]);
         $retore_job->bind_param(13, $job->[0]);
         $retore_job->bind_param(14, $job->[1]);
         $retore_job->execute() || die $DBI::errstr;
         $retore_job->finish();
      }
      print LOGF ETL::getCurrentDateTime() . "  " . "Defined Job [$job->[0],$job->[1]]\n";
      $count++;
   }
   print STDOUT "$count rows loaded.\n";
   print LOGF ETL::getCurrentDateTime() . "  " . "$count rows loaded.\n";
}

sub define_job_source {
   return if ($PRIMARY_SERVER eq '0');
   print STDOUT "Create Job Source...";
   print LOGF ETL::getCurrentDateTime() . "  " . "Create Job Source ...\n";
   my $count = 0;
   foreach my $job (@job) {
      if (!exists($job{"$job->[0]_$job->[1]"})) {
         print LOGF ETL::getCurrentDateTime() . "  " . "WARNING!:Job not exists: $job->[0],$job->[1], ignored.\n";
         print STDOUT "WARNING!:Job not exists: $job->[0],$job->[1], ignored.\n";
         next;
      }

      $query_job_source->bind_param(1, $job->[1]);
      $query_job_source->execute() || die $DBI::errstr;
      my @tabrow = $query_job_source->fetchrow();
      $oldjobsource{$job->[1]} = $tabrow[0];
      $query_job_source->finish();

      $del_job_source->bind_param(1, $job->[1]);
      $del_job_source->execute() || die $DBI::errstr;
      $del_job_source->finish();

      $ins_job_source->bind_param(1, $job->[1]); #Source
      $ins_job_source->bind_param(2, $job->[0]); #ETL_System
      $ins_job_source->bind_param(3, $job->[1]); #ETL_Job
      $ins_job_source->bind_param(4, $job->[1]); #Conv_File_Head
      $ins_job_source->bind_param(5, (exists $oldjobsource{$job->[1]})?$oldjobsource{$job->[1]}:0); #LastCount
      $ins_job_source->execute() || die $DBI::errstr;
      $ins_job_source->finish();
      print LOGF ETL::getCurrentDateTime() . "  " . "Defined Job Source [$job->[0],$job->[1]],[$job->[1]]\n";
      $count++;
   }
   print STDOUT "$count rows loaded.\n";
   print LOGF ETL::getCurrentDateTime() . "  " . "$count rows loaded.\n";
}

sub define_job_step {
   return if ($PRIMARY_SERVER eq '0');
   print STDOUT "Create Job Step...";
   print LOGF ETL::getCurrentDateTime() . "  " . "Create Job Step ...\n";
   my $scriptfile='';
   my ($job_sys, $job_name);
   my $job_bin;
   my $count = 0;
   foreach my $step (@jobstep) {
      $step->[0] =~ /\[(.{3})\](.*)/;
      ($job_sys, $job_name) = ($1, $2);
      if (!exists($job{"${job_sys}_${job_name}"})) {
         print LOGF ETL::getCurrentDateTime() . "  " . "WARNING!:Job not exists: ${job_sys},${job_name}, ignored.\n";
         print STDOUT "WARNING!:Job not exists: ${job_sys},${job_name}, ignored.\n";
         next;
      }

      $del_job_step->bind_param(1, $job_sys);
      $del_job_step->bind_param(2, $job_name);
      $del_job_step->bind_param(3, $step->[1]);
      $del_job_step->execute() || die $DBI::errstr;
      $del_job_step->finish();

      $ins_job_step->bind_param(1, $job_sys); #ETL_System
      $ins_job_step->bind_param(2, $job_name); #ETL_Job
      $ins_job_step->bind_param(3, $step->[1]); #JobStepID
      $ins_job_step->bind_param(4, $step->[3]); #ScriptID
      $ins_job_step->bind_param(5, $source{$step->[3]}->{'ScriptFile'}); #ScriptFile
      $ins_job_step->bind_param(6, $step->[2]); #Description

      $ins_job_step->execute() || die $DBI::errstr;
      $ins_job_step->finish();
      print LOGF ETL::getCurrentDateTime() . "  " . "Defined Job Step [$job_sys,$job_name],[$step->[1]]\n";
      $count++;
   }
   print STDOUT "$count rows loaded.\n";
   print LOGF ETL::getCurrentDateTime() . "  " . "$count rows loaded.\n";
}

sub define_job_timewindow {
   return if ($PRIMARY_SERVER eq '0');
   my ($start_time, $end_time);
   print STDOUT "Create Job Timewindow...";
   print LOGF ETL::getCurrentDateTime() . "  " . "Create Job Timewindow ...\n";
   my $count = 0;
   foreach my $job (@job) {
      if (!exists($job{"$job->[0]_$job->[1]"})) {
         print LOGF ETL::getCurrentDateTime() . "  " . "WARNING!:Job not exists: $job->[0],$job->[1], ignored.\n";
         print STDOUT "WARNING!:Job not exists: $job->[0],$job->[1], ignored.\n";
         next;
      }

      $start_time = $job->[5] eq '' ? 0 : $job->[5];
      $end_time = $job->[6] eq '' ? 23 : $job->[6];

      $del_job_timewindow->bind_param(1, $job->[0]);
      $del_job_timewindow->bind_param(2, $job->[1]);
      $del_job_timewindow->execute() || die $DBI::errstr;
      $del_job_timewindow->finish();

      $ins_job_timewindow->bind_param(1, $job->[0]); #ETL_System
      $ins_job_timewindow->bind_param(2, $job->[1]); #ETL_Job
      $ins_job_timewindow->bind_param(3, $start_time); #BeginHour
      $ins_job_timewindow->bind_param(4, $end_time); #EndHour
      $ins_job_timewindow->execute() || die $DBI::errstr;
      $ins_job_timewindow->finish();
      print LOGF ETL::getCurrentDateTime() . "  " . "Defined Job Timewindow [$job->[0],$job->[1]],[$start_time,$end_time]\n";
      $count++;
   }
   print STDOUT "$count rows loaded.\n";
   print LOGF ETL::getCurrentDateTime() . "  " . "$count rows loaded.\n";
}

sub define_job_dependency {
   return if ($PRIMARY_SERVER eq '0');
   my ($srcsys, $srcjob, $depsys, $depjob);
   print STDOUT "Create Job Dependency...";
   print LOGF ETL::getCurrentDateTime() . "  " . "Create Job Dependency ...\n";
   my $count = 0;
   foreach my $job (sort {$a->[0] cmp $b->[0]} @jobdependency) {
      $job->[0] =~ /\[(.{3})\](.*)/;
      ($srcsys, $srcjob) = ($1, $2);
      $job->[1] =~ /\[(.{3})\](.*)/;
      ($depsys, $depjob) = ($1, $2);
      if (!exists($job{"${srcsys}_${srcjob}"})) {
         print LOGF ETL::getCurrentDateTime() . "  " . "WARNING!:Job not exists: ${srcsys},${srcjob}, ignored.\n";
         print STDOUT "WARNING!:Job not exists: ${srcsys},${srcjob}, ignored.\n";
         next;
      }
      if (!exists($job{"${depsys}_${depjob}"})) {
         print LOGF ETL::getCurrentDateTime() . "  " . "WARNING!:Dependency job not exists: ${depsys},${depjob}, ignored.\n";
         print STDOUT "WARNING!:Dependency job not exists: ${depsys},${depjob}, ignored.\n";
         next;
      }

      $del_job_dependency->bind_param(1, $srcsys);
      $del_job_dependency->bind_param(2, $srcjob);
      $del_job_dependency->bind_param(3, $depsys);
      $del_job_dependency->bind_param(4, $depjob);
      $del_job_dependency->execute() || die $DBI::errstr;
      $del_job_dependency->finish();

      $ins_job_dependency->bind_param(1, $srcsys); #ETL_System
      $ins_job_dependency->bind_param(2, $srcjob); #ETL_Job
      $ins_job_dependency->bind_param(3, $depsys); #Dependency_System
      $ins_job_dependency->bind_param(4, $depjob); #Dependency_Job
      $ins_job_dependency->execute() || die $DBI::errstr;
      $ins_job_dependency->finish();
      print LOGF ETL::getCurrentDateTime() . "  " . "Defined Job Dependency [$srcsys,$srcjob]==>[$depsys,$depjob]\n";
      $count++;
   }
   print STDOUT "$count rows loaded.\n";
   print LOGF ETL::getCurrentDateTime() . "  " . "$count rows loaded.\n";
}

sub define_job_stream {
   return if ($PRIMARY_SERVER eq '0');
   my ($srcsys, $srcjob, $streamsys, $streamjob);
   print STDOUT "Create Job Stream...";
   print LOGF ETL::getCurrentDateTime() . "  " . "Create Job Stream ...\n";
   my $count = 0;
   foreach my $job (sort {$a->[0] cmp $b->[0]} @jobstream) {
      $job->[0] =~ /\[(.{3})\](.*)/;
      ($srcsys, $srcjob) = ($1, $2);
      $job->[1] =~ /\[(.{3})\](.*)/;
      ($streamsys, $streamjob) = ($1, $2);
      if (!exists($job{"${srcsys}_${srcjob}"})) {
         print LOGF ETL::getCurrentDateTime() . "  " . "WARNING!:Job not exists: ${srcsys},${srcjob}, ignored.\n";
         print STDOUT "WARNING!:Job not exists: ${srcsys},${srcjob}, ignored.\n";
         next;
      }
      if (!exists($job{"${streamsys}_${streamjob}"})) {
         print LOGF ETL::getCurrentDateTime() . "  " . "WARNING!:Stream job not exists: ${streamsys},${streamjob}, ignored.\n";
         print STDOUT "WARNING!:Stream job not exists: ${streamsys},${streamjob}, ignored.\n";
         next;
      }

      $del_job_stream->bind_param(1, $srcsys);
      $del_job_stream->bind_param(2, $srcjob);
      $del_job_stream->bind_param(3, $streamsys);
      $del_job_stream->bind_param(4, $streamjob);
      $del_job_stream->execute() || die $DBI::errstr;
      $del_job_stream->finish();

      $ins_job_stream->bind_param(1, $srcsys); #ETL_System
      $ins_job_stream->bind_param(2, $srcjob); #ETL_Job
      $ins_job_stream->bind_param(3, $streamsys); #Stream_System
      $ins_job_stream->bind_param(4, $streamjob); #Stream_Job
      $ins_job_stream->execute() || die $DBI::errstr;
      $ins_job_stream->finish();
      print LOGF ETL::getCurrentDateTime() . "  " . "Defined Job Stream [$srcsys,$srcjob]-->[$streamsys,$streamjob]\n";
      $count++;
   }
   print STDOUT "$count rows loaded.\n";
   print LOGF ETL::getCurrentDateTime() . "  " . "$count rows loaded.\n";
}

sub define_data_calendar {
   return if ($PRIMARY_SERVER eq '0');
   print STDOUT "Create Data Calendar...";
   print LOGF ETL::getCurrentDateTime() . "  " . "Create Data Calendar ...\n";
   my $seqnum = 0;
   my $lastyear = '';
   my $lastjob = '';
   my ($jobsys, $jobnm);
   my $count = 0;
   foreach my $job (sort {if ($a->[0] eq $b->[0]) {$a->[1] cmp $b->[1]} else {$a->[0] cmp $b->[0]}} @datacalendar) {
      if ($job->[0] !~ /\[(.{3})\](.*)/) { next; }
      $job->[0] =~ /\[(.{3})\](.*)/;
      ($jobsys, $jobnm) = ($1, $2);
      if (!exists($job{"${jobsys}_${jobnm}"})) {
         print LOGF ETL::getCurrentDateTime() . "  " . "WARNING!:Job not exists: ${jobsys},${jobnm}, ignored.\n";
         print STDOUT "WARNING!:Job not exists: ${jobsys},${jobnm}, ignored.\n";
         next;
      }

     $job->[1] =~ /(\d{4})年(\d{1,2})月(\d{1,2})日/
     or $job->[1] =~ /(\d{4})\/(\d{1,2})\/(\d{1,2})/
     or $job->[1] =~ /(\d{4})\-(\d{1,2})\-(\d{1,2})/;
     my $calendar = sprintf("%04d%02d%02d", $1, $2, $3);

     if ($lastjob eq $jobnm) {
        if ($lastyear eq $1) {
           $seqnum ++;
        } else {
           $seqnum = 1;
        }
     } else {
        $query_job_calendar->bind_param(1, $jobsys);
        $query_job_calendar->bind_param(2, $jobnm);
        $query_job_calendar->execute() || die $DBI::errstr;
        while (my @tabrow = $query_job_calendar->fetchrow()) {
           $oldcalendar{jobnm}->{$tabrow[0]} = $tabrow[1];
        }
        $query_job_calendar->finish();

        $del_job_calendar->bind_param(1, $jobsys);
        $del_job_calendar->bind_param(2, $jobnm);
        $del_job_calendar->execute() || die $DBI::errstr;
        $del_job_calendar->finish();

        $seqnum = 1;
        $upd_job_calendar->bind_param(1, 'Y');
        $upd_job_calendar->bind_param(2, $jobsys);
        $upd_job_calendar->bind_param(3, $jobnm);
        $upd_job_calendar->execute() || die $DBI::errstr;
        $upd_job_calendar->finish();
        print LOGF ETL::getCurrentDateTime() . "  " . "Update CheckCalendar of [$jobsys,$jobnm] to 'Y'\n";
     }

     $ins_job_calendar->bind_param(1, $jobsys);
     $ins_job_calendar->bind_param(2, $jobnm);
     $ins_job_calendar->bind_param(3, $1);
     $ins_job_calendar->bind_param(4, $seqnum); #SeqNum
     $ins_job_calendar->bind_param(5, $2);
     $ins_job_calendar->bind_param(6, $3);
     $ins_job_calendar->bind_param(7, (exists $oldcalendar{$jobnm}->{$calendar})?$oldcalendar{$jobnm}->{$calendar}:'N');
     $ins_job_calendar->execute() || die $DBI::errstr;
     $ins_job_calendar->finish();

     $lastjob = $jobnm;
     $lastyear = $1;
     print LOGF ETL::getCurrentDateTime() . "  " . "Defined DataCalendar [$jobsys,$jobnm],[$job->[1]]\n";
      $count++;
   }
   print STDOUT "$count rows loaded.\n";
   print LOGF ETL::getCurrentDateTime() . "  " . "$count rows loaded.\n";
}

sub define_job_group {
   return if ($PRIMARY_SERVER eq '0');
   print STDOUT "Create Job Group...";
   print LOGF ETL::getCurrentDateTime() . "  " . "Create Job Group ...\n";
   my $last_job_group = '';
   my ($chldsys, $chldjob, $headsys, $headjob);
   my $count = 0;
   foreach my $job (sort {$a->[1] cmp $b->[1]} @jobgroup) {
      $job->[0] =~ /\[(.{3})\](.*)/;
      ($chldsys, $chldjob) = ($1, $2);
      $job->[1] =~ /\[(.{3})\](.*)/;
      ($headsys, $headjob) = ($1, $2);
      if (!exists($job{"${chldsys}_${chldjob}"})) {
         print LOGF ETL::getCurrentDateTime() . "  " . "WARNING!:Child Job not exists: ${chldsys},${chldjob}, ignored.\n";
         print STDOUT "WARNING!:Child Job not exists: ${chldsys},${chldjob}, ignored.\n";
         next;
      }
      if (!exists($job{"${headsys}_${headjob}"})) {
         print LOGF ETL::getCurrentDateTime() . "  " . "INFO!:Head Job not exists: ${headsys},${headjob}, define it as a virtual job.\n";
         print STDOUT "INFO!:Head Job not exists: ${headsys},${headjob}, define it as a virtual job.\n";
         $ins_job->bind_param(1, $headsys); #ETL_System
         $ins_job->bind_param(2, $headjob); #ETL_Job
         $ins_job->bind_param(3, 'ALL_NODES'); #ETL_Server
         $ins_job->bind_param(4, 'Virtual Head Job'); #Description
         $ins_job->bind_param(5, 0); #Frequency
         $ins_job->bind_param(6, 'V'); #JobType
         $ins_job->bind_param(7, 0); #Priority
         $ins_job->bind_param(8, 0); #AllowJumpFlag
         $ins_job->execute() || die $DBI::errstr;
         $ins_job->finish();

         $job{"${headsys}_${headjob}"}->{'server'} = 'ALL_NODES';
         $job{"${headsys}_${headjob}"}->{'jobtype'} = 'V';
      }

      if ($last_job_group ne $headjob && exists $job{"${headsys}_${headjob}"}) {
         $del_job_group->bind_param(1, $headjob);
         $del_job_group->execute() || die $DBI::errstr;
         $del_job_group->finish();

         $ins_job_group->bind_param(1, $headjob);
         $ins_job_group->bind_param(2, $headsys);
         $ins_job_group->bind_param(3, $headjob);
         $ins_job_group->execute() || die $DBI::errstr;
         $ins_job_group->finish();
         print LOGF ETL::getCurrentDateTime() . "  " . "Defined Job GroupHead:[$headsys,$headjob]\n";
      }

      $query_job_groupchild->bind_param(1, $headjob);
      $query_job_groupchild->bind_param(2, $chldsys);
      $query_job_groupchild->bind_param(3, $chldjob);
      $query_job_groupchild->execute() || die $DBI::errstr;
      while (my @tabrow = $query_job_groupchild->fetchrow()) {
         $oldgroupchild{$headjob}->{$chldjob}->{'CheckFlag'} = $tabrow[0];
         $oldgroupchild{$headjob}->{$chldjob}->{'TXDate'} = $tabrow[1];
      }
      $query_job_groupchild->finish();

      $del_job_groupchild->bind_param(1, $headjob);
      $del_job_groupchild->bind_param(2, $chldsys);
      $del_job_groupchild->bind_param(3, $chldjob);
      $del_job_groupchild->execute() || die $DBI::errstr;
      $del_job_groupchild->finish();

      $ins_job_groupchild->bind_param(1, $headjob);
      $ins_job_groupchild->bind_param(2, $chldsys);
      $ins_job_groupchild->bind_param(3, $chldjob);
      $ins_job_groupchild->bind_param(4, (exists $oldgroupchild{$headjob}->{$chldjob})?$oldgroupchild{$headjob}->{$chldjob}->{'CheckFlag'}:'N');
      $ins_job_groupchild->bind_param(5, (exists $oldgroupchild{$headjob}->{$chldjob})?$oldgroupchild{$headjob}->{$chldjob}->{'TXDate'}:undef);
      $ins_job_groupchild->execute() || die $DBI::errstr;
      $ins_job_groupchild->finish();

      $last_job_group = $headjob;
      print LOGF ETL::getCurrentDateTime() . "  " . "Defined Job GroupChild:[$chldsys,$chldjob] GroupHead:[$headsys,$headjob]\n";
      $count++;
   }
   print STDOUT "$count rows loaded.\n";
   print LOGF ETL::getCurrentDateTime() . "  " . "$count rows loaded.\n";
}

sub define_job_related {
   return if ($PRIMARY_SERVER eq '0');
   my ($srcsys, $srcjob, $relatesys, $relatejob,$offsetday);
   print STDOUT "Create Related Job...";
   print LOGF ETL::getCurrentDateTime() . "  " . "Create Related Job...\n";
   my $count = 0;
   foreach my $job (sort {$a->[0] cmp $b->[0]} @jobrelated) {
      $job->[0] =~ /\[(.{3})\](.*)/;
      ($srcsys, $srcjob) = ($1, $2);
      $job->[1] =~ /\[(.{3})\](.*)/;
      ($relatesys, $relatejob) = ($1, $2);
      if (!exists($job{"${srcsys}_${srcjob}"})) {
         print LOGF ETL::getCurrentDateTime() . "  " . "WARNING!:Job not exists: ${srcsys},${srcjob}, ignored.\n";
         print STDOUT "WARNING!:Job not exists: ${srcsys},${srcjob}, ignored.\n";
         next;
      }
      if (!exists($job{"${relatesys}_${relatejob}"})) {
         print LOGF ETL::getCurrentDateTime() . "  " . "WARNING!:Related job not exists: ${relatesys},${relatejob}, ignored.\n";
         print STDOUT "WARNING!:Related job not exists: ${relatesys},${relatejob}, ignored.\n";
         next;
      }

      $del_job_related->bind_param(1, $srcsys);
      $del_job_related->bind_param(2, $srcjob);
      $del_job_related->bind_param(3, $relatesys);
      $del_job_related->bind_param(4, $relatejob);
      $del_job_related->execute() || die $DBI::errstr;
      $del_job_related->finish();

      $ins_job_related->bind_param(1, $srcsys); #ETL_System
      $ins_job_related->bind_param(2, $srcjob); #ETL_Job
      $ins_job_related->bind_param(3, $relatesys); #RelatedSystem
      $ins_job_related->bind_param(4, $relatejob); #RelatedJob
      if (!defined($job->[2]) || $job->[2] eq '') {
         if ($job{"${relatesys}_${relatejob}"}->{'jobtype'} eq 'D') {
            $offsetday = 1;
         } elsif ($job{"${relatesys}_${relatejob}"}->{'jobtype'} eq 'W') {
            $offsetday = 7;
         } elsif ($job{"${relatesys}_${relatejob}"}->{'jobtype'} eq 'M') {
            $offsetday = 31;
         } else {
            $offsetday = 1;
         }
         $ins_job_related->bind_param(5, $offsetday); #OffsetDay
      } else {
         $ins_job_related->bind_param(5, $job->[2]); #OffsetDay
      }
      $ins_job_related->execute() || die $DBI::errstr;
      $ins_job_related->finish();
      print LOGF ETL::getCurrentDateTime() . "  " . "Defined Related Job Source:[$srcsys,$srcjob] Related:[$relatesys,$relatejob]\n";
      $count++;
   }
   print STDOUT "$count rows loaded.\n";
   print LOGF ETL::getCurrentDateTime() . "  " . "$count rows loaded.\n";
}

sub define_trigger_plan {
   return if ($PRIMARY_SERVER eq '0');
   my ($triggertype, $jobtype);
   print STDOUT "Create Trigger Plan...";
   print LOGF ETL::getCurrentDateTime() . "  " . "Create Trigger Plan...\n";
   my $count = 0;
   foreach my $plan (@triggerplan) {
      $plan->[2] =~ /\[(.{1})\](.*)/;
      $triggertype = $1;
      if ($triggertype eq 'I') {
         $jobtype = 'D';
      } elsif ($triggertype eq '9') {
         $jobtype = '9';
      } else {
         $jobtype = '';
      }

      $del_job_trigger_id->bind_param(1, $plan->[0]);
      $del_job_trigger_id->execute() || die $DBI::errstr;
      $del_job_trigger_id->finish();

      $ins_job_trigger_id->bind_param(1, $plan->[0]); #TriggerID
      $ins_job_trigger_id->bind_param(2, $triggertype); #TriggerType
      $ins_job_trigger_id->bind_param(3, $plan->[1]); #Decription
      $ins_job_trigger_id->bind_param(4, $plan->[3]); #StartHour
      $ins_job_trigger_id->bind_param(5, $plan->[4]); #StartMin
      $ins_job_trigger_id->bind_param(6, $plan->[9] eq '' ? 0 : $plan->[9]); #OffsetDay
      $ins_job_trigger_id->execute() || die $DBI::errstr;
      $ins_job_trigger_id->finish();
      print LOGF ETL::getCurrentDateTime() . "  " . "Defined Trigger Plan '$plan->[0],[$triggertype]' with StartHour:$plan->[3],StartMin:$plan->[4]\n";

      if ($triggertype eq '9') {
         my ($year, $month, $day);

         $del_job_trigger_calc->bind_param(1, $plan->[0]);
         $del_job_trigger_calc->execute() || die $DBI::errstr;
         $del_job_trigger_calc->finish();

         foreach my $calc(sort {if ($a->[0] eq $b->[0]) {$a->[1] cmp $b->[1]} else {$a->[0] cmp $b->[0]}} @datacalendar) {
            next if ($calc->[0] =~ /\[(.{3})\](.*)/ || $calc->[0] != $plan->[0]);
            $calc->[1] =~ /(\d{4})年(\d{1,2})月(\d{1,2})日/;
            ($year, $month, $day) = ($1, $2, $3);

            $ins_job_trigger_calc->bind_param(1, $plan->[0]); #TriggerID
            $ins_job_trigger_calc->bind_param(2, $year); #YearNum
            $ins_job_trigger_calc->bind_param(3, $month); #MonthNum
            $ins_job_trigger_calc->bind_param(4, $day); #DayNum
            $ins_job_trigger_calc->execute() || die $DBI::errstr;
            $ins_job_trigger_calc->finish();
            print LOGF ETL::getCurrentDateTime() . "  " . "Defined Trigger Plan Calendar '$plan->[0],[$calc->[1]]'\n";
         }
      } elsif ($triggertype eq 'I') {
         $del_job_trigger_interval->bind_param(1, $plan->[0]);
         $del_job_trigger_interval->execute() || die $DBI::errstr;
         $del_job_trigger_interval->finish();

         $ins_job_trigger_interval->bind_param(1, $plan->[0]); #TriggerID
         $ins_job_trigger_interval->bind_param(2, $plan->[8]); #Interval_Minutes
         $ins_job_trigger_interval->execute() || die $DBI::errstr;
         $ins_job_trigger_interval->finish();
         print LOGF ETL::getCurrentDateTime() . "  " . "Defined Trigger Plan Interval '$plan->[0],[$plan->[8] minutes]'\n";
      } elsif ($triggertype eq 'W') {
         $del_job_trigger_week->bind_param(1, $plan->[0]);
         $del_job_trigger_week->execute() || die $DBI::errstr;
         $del_job_trigger_week->finish();

         $ins_job_trigger_week->bind_param(1, $plan->[0]); #TriggerID
         $ins_job_trigger_week->bind_param(2, $plan->[5]); #Timing
         $ins_job_trigger_week->execute() || die $DBI::errstr;
         $ins_job_trigger_week->finish();
         print LOGF ETL::getCurrentDateTime() . "  " . "Defined Trigger Plan Week '$plan->[0],[$plan->[5]]'\n";
      } elsif ($triggertype eq 'M') {
         $del_job_trigger_month->bind_param(1, $plan->[0]);
         $del_job_trigger_month->execute() || die $DBI::errstr;
         $del_job_trigger_month->finish();

         $ins_job_trigger_month->bind_param(1, $plan->[0]); #TriggerID
         $ins_job_trigger_month->bind_param(2, $plan->[6]); #Timing
         $ins_job_trigger_month->bind_param(3, $plan->[7] eq '是' ? 'Y' : 'N'); #EndOfMonth
         $ins_job_trigger_month->execute() || die $DBI::errstr;
         $ins_job_trigger_month->finish();
         print LOGF ETL::getCurrentDateTime() . "  " . "Defined Trigger Plan MonthDay '$plan->[0],[$plan->[6]]', with EndOfMonth '",$plan->[7] eq '是' ? 'Y' : 'N',"'\n";
      }

      $triggerplan{$plan->[0]}->{'jobtype'} = $jobtype;
      $triggerplan{$plan->[0]}->{'starthour'} = $plan->[3];
      $triggerplan{$plan->[0]}->{'startmin'} = $plan->[4];
      $count++;
   }
   print STDOUT "$count rows loaded.\n";
   print LOGF ETL::getCurrentDateTime() . "  " . "$count rows loaded.\n";
}

sub define_job_trigger {
   return if ($PRIMARY_SERVER eq '0');
   print STDOUT "Create Job Trigger...";
   print LOGF ETL::getCurrentDateTime() . "  " . "Create Job Trigger...\n";
   my ($job_sys, $job_name);
   my $count = 0;
   foreach my $trigger (@jobtrigger) {
      next if ($trigger->[1] !~ /^\d+$/);
      $trigger->[0] =~ /\[(.{3})\](.*)/;
      ($job_sys, $job_name) = ($1, $2);
      if (!exists($job{"${job_sys}_${job_name}"})) {
         print LOGF ETL::getCurrentDateTime() . "  " . "WARNING!:Job not exists: ${job_sys},${job_name}, ignored.\n";
         print STDOUT "WARNING!:Job not exists: ${job_sys},${job_name}, ignored.\n";
         next;
      }

      $del_job_trigger->bind_param(1, $job_sys);
      $del_job_trigger->bind_param(2, $job_name);
      $del_job_trigger->execute() || die $DBI::errstr;
      $del_job_trigger->finish();

      if (exists $triggerplan{$trigger->[1]}) {
         $ins_job_trigger->bind_param(1, $job_sys); #ETL_System
         $ins_job_trigger->bind_param(2, $job_name); #ETL_Job
         $ins_job_trigger->bind_param(3, $trigger->[1]); #TriggerID
         $ins_job_trigger->bind_param(4, $triggerplan{$trigger->[1]}->{'starthour'}); #StartHour
         $ins_job_trigger->bind_param(5, $triggerplan{$trigger->[1]}->{'startmin'}); #StartMin
         $ins_job_trigger->execute() || die $DBI::errstr;
         $ins_job_trigger->finish();
         print LOGF ETL::getCurrentDateTime() . "  " . "Defined Job Trigger '[$job_sys,$job_name],$trigger->[1]'\n";

         if ($triggerplan{$trigger->[1]}->{'jobtype'} ne '') {
            $upd_job_type->bind_param(1, $triggerplan{$trigger->[1]}->{'jobtype'}); #JobType
            $upd_job_type->bind_param(2, $job_sys); #ETL_System
            $upd_job_type->bind_param(3, $job_name); #ETL_Job
            $upd_job_type->execute() || die $DBI::errstr;
            $upd_job_type->finish();
            $job{"${job_sys}_${job_name}"}->{'jobtype'} = $triggerplan{$trigger->[1]}->{'jobtype'};
            print LOGF ETL::getCurrentDateTime() . "  " . "Update JobType of '[$job_sys,$job_name]' to '$triggerplan{$trigger->[1]}->{'jobtype'}'\n";
         }

         $upd_timetrigger->bind_param(1, $job_sys); #ETL_System
         $upd_timetrigger->bind_param(2, $job_name); #ETL_Job
         $upd_timetrigger->execute() || die $DBI::errstr;
         $upd_timetrigger->finish();
         print LOGF ETL::getCurrentDateTime() . "  " . "Update TimeTrigger of '[$job_sys,$job_name]' to 'Y'\n";
      } else {
         print LOGF ETL::getCurrentDateTime() . "  " . "WARNING!:Trigger Plan ID '$trigger->[1]' not exists,ignored.\n";
         print STDOUT "WARNING!:Trigger Plan ID '$trigger->[1]' not exists,ignored.\n";
      }
      $count++;
   }
   print STDOUT "$count rows loaded.\n";
   print LOGF ETL::getCurrentDateTime() . "  " . "$count rows loaded.\n";
}

sub parseExcel {
   foreach my $worksheet ( @{ $book->{Worksheet} }[ 0 .. $last_sheet ] ) {
      next
         if not defined $worksheet->{MaxRow}
         or not defined $worksheet->{MaxCol};

     $sheetname = $worksheet->get_name();

     next if not ($sheetname eq '服务器组' or $sheetname eq '作业脚本' or $sheetname eq '作业系统'
                  or $sheetname eq '作业' or $sheetname eq '作业步骤' or $sheetname eq '作业组触发'
                  or $sheetname eq '作业依赖' or $sheetname eq '作业触发' or $sheetname eq '作业关联'
                  or $sheetname eq '作业定时触发' or $sheetname eq '定时触发计划' or $sheetname eq '数据日历');

     @row = $worksheet->{MinRow} .. $worksheet->{MaxRow};
     @col = $worksheet->{MinCol} .. $worksheet->{MaxCol};

     print STDOUT "Read worksheet [$sheetname]...";
     print LOGF ETL::getCurrentDateTime() . "  " . "Read worksheet [$sheetname]...";

     my $i = 0;
     foreach $row ( @{ $worksheet->{Cells} }[ @row ] ) {
        next if ($i++ == 0);
        @cellvalue = map {
           $_ = $_->Value() if ref $_;
           $_ = '' if not defined $_;
           $_ =~ s/\r//g;
           $_ = '' . $_ . '';
        } @$row[ @col ];

        if ( $cellvalue[0] eq '' ) {
           $i--; last;
        }

        if ( $sheetname eq '服务器组' ) {
           push @servergroup, [ @cellvalue ];
        } elsif ( $sheetname eq '作业脚本' ) {
           push @script, [ @cellvalue ];
        } elsif ( $sheetname eq '作业系统' ) {
           push @sys, [ @cellvalue ];
        } elsif ( $sheetname eq '作业' ) {
           push @job, [ @cellvalue ];
        } elsif ( $sheetname eq '作业步骤' ) {
           push @jobstep, [ @cellvalue ];
        } elsif ( $sheetname eq '作业组触发' ) {
           push @jobgroup, [ @cellvalue ];
        } elsif ( $sheetname eq '作业触发' ) {
           push @jobstream, [ @cellvalue ];
        } elsif ( $sheetname eq '作业依赖' ) {
           push @jobdependency, [ @cellvalue ];
        } elsif ( $sheetname eq '作业关联' ) {
           push @jobrelated, [ @cellvalue ];
        } elsif ( $sheetname eq '作业定时触发' ) {
           push @jobtrigger, [ @cellvalue ];
        } elsif ( $sheetname eq '定时触发计划' ) {
           push @triggerplan, [ @cellvalue ];
        } elsif ( $sheetname eq '数据日历' ) {
           push @datacalendar, [ @cellvalue ];
        }
     }
     
     $i--;
     print STDOUT "$i rows readed.\n";
     print LOGF "$i rows readed.\n";
   }
}

sub main {
   &parseExcel;
   &db_init;
   &define_server_group;
   &define_script;
   &define_trigger_plan;
   &define_sys;
   &define_job;
   &define_job_step;
   &define_job_source;
   &define_job_timewindow;
   &define_job_dependency;
   &define_job_stream;
   &define_job_group;
   &define_job_related;
   &define_data_calendar;
   &define_job_trigger;
   $dbh->disconnect() if $dbh;
   print LOGF ETL::getCurrentDateTime() . "  " . "Closed Database Connection.\n";
   print LOGF ETL::getCurrentDateTime() . "  " . "Define job finished.\n";
   print STDOUT "Define job finished.\n";
}

main;

close(LOGF);

exit 0;

__END__