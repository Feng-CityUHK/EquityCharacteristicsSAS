/* ********************************************* */
/*       load compq                              */
/* ********************************************* */
data comp1;
set comp.fundq;
where indfmt='INDL' and datafmt='STD' and consol='C' and popsrc='D'
  and datadate>="01JAN1950"d;
keep gvkey datadate rdq fyearq fqtr;
run;

proc export data = comp1(where=(year(datadate)=2018))
outfile='/scratch/cityuhk/xinhe_mandy/eqchars/abr_comp1.csv' dbms=csv replace; run;

/* ********************************************* */
/*       prepare merging with crsp               */
/* ********************************************* */
/* Add Historical PERMNO identifier */
proc sql;
  create table comp2
  as select a.*, b.lpermno as permno, b.linkprim
  from comp1 as a, crsp.ccmxpf_linktable as b
  where a.gvkey = b.gvkey and
  b.LINKTYPE in ("LU","LC") and
 (b.LINKDT <= a.datadate) and (a.datadate <= b.LINKENDDT or missing(b.LINKENDDT));
quit;

proc export data = comp2(where=(year(datadate)=2018))
outfile='/scratch/cityuhk/xinhe_mandy/eqchars/abr_comp2.csv' dbms=csv replace; run;

/* ********************************************* */
/*       rdq: the first trading on(after) rdq    */
/* ********************************************* */
proc sql;
  create view eads1
     as select a.*, b.date as rdq1 format=date9.
     from (select distinct rdq from comp2) a
     left join (select distinct date from crsp.dsi) b
     on 5>=b.date-a.rdq>=0
     group by rdq
     having b.date-a.rdq=min(b.date-a.rdq);
  create table comp3
     as select a.*, b.rdq1
     label='Adjusted Report Date of Quarterly Earnings'
     from comp2 a left join eads1 b
     on a.rdq=b.rdq
     order by a.gvkey, a.fyearq desc, a.fqtr desc;
quit;

proc export data = comp3(where=(year(datadate)=2018))
outfile='/scratch/cityuhk/xinhe_mandy/eqchars/abr_comp3.csv' dbms=csv replace; run;

/* ********************************************* */
/*      crsp abnormal return                     */
/* ********************************************* */

%let msfvars = permco permno prc ret vol shrout cfacpr cfacshr;
%let msevars = ncusip exchcd shrcd siccd ;

%crspmerge(s=d,start=01jan1950,end=31dec2018,
	sfvars=&msfvars,sevars=&msevars,
  filters=exchcd in (1,2,3) and shrcd in (10,11));
proc sql; create table mydaily
	as select a.*, b.dlret,
	  sum(1,ret)*sum(1,dlret)-1 as retadj "Return adjusted for delisting",
	  abs(a.prc)*a.shrout as MEq 'Market Value of Equity'
	 from Crsp_d a left join crsp.dsedelist(where=(missing(dlret)=0)) b
	 on a.permno=b.permno and a.date=b.DLSTDT
	 order by a.date, a.permno, MEq;
quit;

/* sprtrn */
data crspsp500d;
set crsp.dsi;
keep date sprtrn;
run;

proc sort data=crspsp500d nodupkey; by date;run;
proc export data = crspsp500d(where=(year(date)=2018))
outfile='/scratch/cityuhk/xinhe_mandy/eqchars/abr_sprtrn.csv' dbms=csv replace; run;

/* abnormal return */
proc sql;
create table mydaily1 as
select a.date, a.permno, a.ret, a.retadj, b.sprtrn, a.retadj-b.sprtrn as abrd
from mydaily a left join crspsp500d b
on a.date=b.date
order by a.permno, a.date;
quit;

proc export data = mydaily1(where=(year(date)>=2018))
outfile='/scratch/cityuhk/xinhe_mandy/eqchars/abr_mydaily1.csv' dbms=csv replace; run;

/* ********************************************* */
/*       date count regarding to rdq             */
/* ********************************************* */
proc sql;
create table comp4 as
select a.*, b.date, b.abrd
from comp3 a left join mydaily1 b
on a.permno=b.permno and
intnx('day',a.rdq1,-10,'E')<=b.date<=intnx('day',a.rdq1,5,'E')
order by a.permno, a.rdq1, b.date;
quit;

proc export data = comp4(where=(year(datadate)=2018))
outfile='/scratch/cityuhk/xinhe_mandy/eqchars/abr_comp4.csv' dbms=csv replace; run;

/* delete missing return */
data comp4;
set comp4;
if missing(abrd) then delete;
run;

proc sort data=comp4;
by permno rdq1 date;
run;

/*count*/
data temp0; set comp4;
  by permno rdq1 date;
  if date=rdq1 then c_1=0;
  else if date>rdq1 then c_1=1;
  else if date<rdq1 then c_1=(-1);
  format date date9. abrd percent7.4;
run;
data temp_1; set temp0;
if c_1=-1;
run;
proc sort data=temp_1;
by permno rdq1 descending date;
run;

data temp1;set temp_1;
by permno rdq1;
if first.rdq1 then count=c_1;
else count + c_1;
run;

data temp_2; set temp0;
if c_1>=0;
run;
proc sort data=temp_2;
by permno rdq1 date;
run;

data temp2;set temp_2;
by permno rdq1;
if first.rdq1 then count=c_1;
else count + c_1;
run;

proc sql;
create table temp3
as select * from temp1
union
select * from temp2;
quit;
proc sort data=temp3; by permno rdq1 count;run;

proc export data = temp3(where=(year(datadate)=2018))
outfile='/scratch/cityuhk/xinhe_mandy/eqchars/abr_temp3.csv' dbms=csv replace; run;

/* ********************************************* */
/*       calculate abr as the group sum          */
/* ********************************************* */
data temp4;
set temp3;
where count between -2 and 1;
run;

proc export data = temp4(where=(year(datadate)=2018))
outfile='/scratch/cityuhk/xinhe_mandy/eqchars/abr_temp4.csv' dbms=csv replace; run;

proc sql;
create
  table temp5 as
select
  a.*,
  mean(abrd) as abr
from
  temp4 a
  group by permno, rdq1
order by
  permno, rdq1, date;
quit;

proc export data = temp5(where=(year(datadate)=2018))
outfile='/scratch/cityuhk/xinhe_mandy/eqchars/abr_temp5.csv' dbms=csv replace; run;

data temp6;
set temp5(rename=(DATE=rdqplus1));
where count=1;
keep gvkey permno datadate rdq rdqplus1 abr;
run;

proc export data = temp6(where=(year(datadate)=2018))
outfile='/scratch/cityuhk/xinhe_mandy/eqchars/abr_temp6.csv' dbms=csv replace; run;

/* ********************************************* */
/*       populate the quarterly abr to monthly   */
/* ********************************************* */
proc sql;
  create
    table rdqabr as
  select
    a.*, b.date format=date9.
  from
    temp6 a left join (select distinct date from crsp.msf) b
  on
    a.rdqplus1<b.date and
    intnx('month',a.datadate,12,'E')>=b.date
  order by
    a.permno, b.date, a.datadate desc
  ;
quit;

proc export data = rdqabr(where=(year(datadate)>=2017))
outfile='/scratch/cityuhk/xinhe_mandy/eqchars/v7_1_abr_dup.csv' dbms=csv replace; run;

proc sort data=rdqabr nodupkey; by permno date; run;

proc export data = rdqabr(where=(year(datadate)>=2017))
outfile='/scratch/cityuhk/xinhe_mandy/eqchars/v7_1_abr.csv' dbms=csv replace; run;

libname chars '/scratch/cityuhk/xinhe_mandy/eqchars';
data chars.v7_1_abr; set rdqabr; run;
