/* ********************************************* */
/* ********************************************* */
/* Calculate HXZ Replicating Anormalies          */
/* Revision in analyst forecast                  */
/* Chan Jegadeesh Lakonishok 1996 Momentum Strategy */
/* ********************************************* */
/* ********************************************* */

/* ********************************************* */
/*       load iclink                             */
/* ********************************************* */
libname chars '/scratch/cityuhk/xinhe/eqchars';
data iclink; set chars.iclink; run;

proc export data = iclink
outfile='/scratch/cityuhk/xinhe/eqchars/iclink.csv' dbms=csv replace; run;

/* ********************************************* */
/*  Merging IBES and CRSP using ICLINK table     */
/*  Merging last month price                     */
/* ********************************************* */
proc sql;
create table IBES_CRSP as select
  a.ticker, a.statpers, a.meanest, a.fpedats, a.anndats_act, a.curr_act, a.fpi,
  c.permno, c.date, c.prc, c.cfacpr
from
  ibes.statsum_epsus as a, work.ICLINK as b, crsp.msf as c
where
  /* merging rules */
  a.ticker=b.ticker and
  b.permno=c.permno and
  intnx('month',a.STATPERS,0,'E') = intnx('month',c.date,1,'E') and
  /* filtering IBES */
  a.statpers<a.ANNDATS_ACT and     /*only keep summarized forecasts prior to earnings annoucement*/
  a.measure='EPS' and
  not missing(a.medest) and
  not missing(a.fpedats) and
  (a.fpedats-a.statpers)>=0 and
  a.CURCODE='USD' and
  ( a.CURR_ACT='USD' or missing(a.CURR_ACT) ) and
  a.fpi in ('1','2')
order by
  a.ticker, a.fpedats, a.statpers
;
quit;

data ic; set IBES_CRSP; run;  /* a short name */

proc export data = ic
outfile='/scratch/cityuhk/xinhe/eqchars/ic.csv' dbms=csv replace; run;


/* ********************************************* */
/*  Merging last month forecast                  */
/* ********************************************* */

proc sql;
create table ic1 as select
  a.*,
  b.statpers as statpers_last_month,
  b.meanest as meanest_last_month
from
  ic a left join ic b
on
  a.ticker=b.ticker and
  a.permno=b.permno and
  a.fpedats=b.fpedats and
  intnx('month',a.statpers,0,'E') = intnx('month',b.statpers,1,'E')
order by
  a.ticker, a.permno, a.fpedats, a.statpers
;
quit;

proc sort data=ic1 nodupkey; by ticker fpedats statpers; run;

proc export data = ic1
outfile='/scratch/cityuhk/xinhe/eqchars/ic1.csv' dbms=csv replace; run;

/* ********************************************* */
/*  Drop empty "last month"                      */
/*  calculate HXZ RE                             */
/* ********************************************* */
data ic2; set ic1;
if missing(statpers_last_month) then delete;
prc_adj = prc/cfacpr;
if prc_adj<=0 then delete;
monthly_revision = (meanest - meanest_last_month)/prc_adj;
permno_fpedats = catx('-', permno, fpedats);
run;

proc sort data=ic2 nodupkey; by permno_fpedats statpers; run;

data ic2; set ic2;
retain count;
by permno_fpedats statpers;
if first.permno_fpedats then count=1;
else count+1;
run;

proc export data = ic2
outfile='/scratch/cityuhk/xinhe/eqchars/ic2.csv' dbms=csv replace; run;

/* ********************************************* */
/*  calc RE  (CJL)                     */
/* ********************************************* */

data ic3;
set ic2;
hxz_re=.;
if count=4 then hxz_re = ( lag1(monthly_revision) + lag2(monthly_revision) + lag3(monthly_revision)
)/3;
if count=5 then hxz_re = ( lag1(monthly_revision) + lag2(monthly_revision) + lag3(monthly_revision) + lag4(monthly_revision)
)/4;
if count=6 then hxz_re = ( lag1(monthly_revision) + lag2(monthly_revision) + lag3(monthly_revision) + lag4(monthly_revision) + lag5(monthly_revision) )/5;
if count>=7 then hxz_re = ( lag1(monthly_revision) + lag2(monthly_revision) + lag3(monthly_revision) + lag4(monthly_revision) + lag5(monthly_revision) + lag6(monthly_revision) )/6;
run;

proc export data = ic3
outfile='/scratch/cityuhk/xinhe/eqchars/ic3.csv' dbms=csv replace; run;

/* ********************************************* */
/* retain one obs for each ticker-statpers        */
/* ********************************************* */

data ic4;
set ic3(drop=DATE	PRC	CFACPR	statpers_last_month	meanest_last_month	prc_adj	monthly_revision	permno_fpedats);
if count<4 then delete;
run;

proc sort data=ic4; by ticker statpers fpedats; run;
proc sort data=ic4 nodupkey; by ticker statpers; run;

proc export data = ic4
outfile='/scratch/cityuhk/xinhe/eqchars/ic4.csv' dbms=csv replace; run;


/* ********************************************* */
/*  save re                                      */
/* ********************************************* */

data ic5;
set ic4;
rename statpers=date;
run;

data re;
set ic5(drop=	MEANEST	FPI	count);
run;

libname chars '/scratch/cityuhk/xinhe/eqchars';
data chars.v7_1_re; set re; run;

proc export data = re(where=(year(date)>=2017))
outfile='/scratch/cityuhk/xinhe/eqchars/v7_1_re.csv' dbms=csv replace; run;
