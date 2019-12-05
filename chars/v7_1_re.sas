/* ********************************************* */
/* ********************************************* */
/* Calculate HXZ Replicating Anormalies          */
/* Revision in analyst forecast                  */
/* ********************************************* */
/* ********************************************* */

/* ********************************************* */
/*       load iclink                             */
/* ********************************************* */
libname chars '/scratch/cityuhk/xinhe_mandy/eqchars';
data iclink; set chars.iclink; run;

proc export data = iclink
outfile='/scratch/cityuhk/xinhe_mandy/eqchars/iclink.csv' dbms=csv replace; run;

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
  a.CURR_ACT='USD' and
  a.FISCALP = 'QTR' and
  a.fpi in ('6','7','8') /* and */
  /* a.ticker in ('AAPL','BABA','GOOG','AMZN') */
order by
  a.ticker, a.fpedats, a.statpers
;
quit;

data ic; set IBES_CRSP; run;  /* a short name */

proc export data = ic
outfile='/scratch/cityuhk/xinhe_mandy/eqchars/ic.csv' dbms=csv replace; run;

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
  intnx('month',a.statpers,0,'E') = intnx('month',b.statpers,1,'E')
order by
  a.ticker, a.permno, a.fpedats, a.statpers
;
quit;

proc sort data=ic1 nodupkey; by ticker fpedats statpers; run;

proc export data = ic1
outfile='/scratch/cityuhk/xinhe_mandy/eqchars/ic1.csv' dbms=csv replace; run;

/* ********************************************* */
/*  Drop empty "last month"                      */
/*  Drop tow far forecasts (larger than 6 month ago) */
/*  calculate HXZ RE                             */
/* ********************************************* */
data ic2; set ic1;
if missing(statpers_last_month) then delete;
/* atmost 6 months */
if intnx('month',statpers,7,'E') <= fpedats then delete;
/* remove the most recent month */
if intnx('month',statpers,0,'E') = intnx('month',fpedats,0,'E') then delete;
prc_adj = prc/cfacpr;
monthly_revision = (meanest - meanest_last_month)/prc_adj;
run;

proc export data = ic2
outfile='/scratch/cityuhk/xinhe_mandy/eqchars/ic2.csv' dbms=csv replace; run;

/* ********************************************* */
/*  Count the number of obs for each rdq         */
/* ********************************************* */
/*Count number of estimates reported on primary/diluted basis */
proc sql;
create table ic3 as select
  a.*,
  sum(curr_act='USD') as n_count,
  mean(monthly_revision) as hxz_re
from
  ic2 a
group by
  ticker, fpedats
order by
  ticker,fpedats,statpers
;
quit;

proc export data = ic3
outfile='/scratch/cityuhk/xinhe_mandy/eqchars/ic3.csv' dbms=csv replace; run;

/* ********************************************* */
/* retain one obs for each ticker-fpedats        */
/* ********************************************* */

data ic4;
set ic3(drop=STATPERS CURR_ACT FPI DATE PRC CFACPR MEANEST
statpers_last_month	meanest_last_month monthly_revision);
if n_count<4 then delete;
run;

proc sort data=ic4 nodupkey; by ticker fpedats; run;

proc export data = ic4
outfile='/scratch/cityuhk/xinhe_mandy/eqchars/ic4.csv' dbms=csv replace; run;

/* ********************************************* */
/*       populate the quarterly re to monthly   */
/* ********************************************* */
proc sql;
  create
    table re as
  select
    a.*, b.date format=date9.
  from
    ic4 a left join (select distinct date from crsp.msf) b
  on
    a.fpedats<=b.date and
    intnx('month',a.fpedats,12,'E')>=b.date
  order by
    a.permno, b.date, a.fpedats desc
  ;
quit;

proc export data = re(where=(year(fpedats)>=2017))
outfile='/scratch/cityuhk/xinhe_mandy/eqchars/v7_1_re_dup.csv' dbms=csv replace; run;

proc sort data=re nodupkey; by permno date; run;
/* ********************************************* */
/*  save re                                      */
/* ********************************************* */

libname chars '/scratch/cityuhk/xinhe_mandy/eqchars';
data chars.v7_1_re; set re; run;

proc export data = re(where=(year(fpedats)>=2017))
outfile='/scratch/cityuhk/xinhe_mandy/eqchars/v7_1_re.csv' dbms=csv replace; run;
