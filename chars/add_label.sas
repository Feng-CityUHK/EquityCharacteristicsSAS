libname chars '/scratch/cityuhk/xinhe_mandy/eqchars';

%let vars_rank =
  mom12m hxz_abr hxz_sue hxz_re
  bm ep cfp sp
  agr ni acc
  op roe
  seas1a adm rdm
  me svar beta mom1m
;

/* ********************************************* */
/*  Load data                    */
/* ********************************************* */

data da;
set chars.v7_1_rank_label;
keep
  public_date   permno   gvkey   sic  cusip
  me bm mom12m
  rank_me rank_bm rank_mom12m
  ;
run;

/* ********************************************* */
/*  DGTW  label                   */
/*  simple Breakpoints */
/*  monthly dependent sorting */
/*  ME BM MOM */
/*  No industry adjustment for BM */
/* ********************************************* */

/* sort on size */
data da;
set da;
if      rank_me<=-0.6 then dgtw_port_me = 1;
if -0.6<rank_me<=-0.2 then dgtw_port_me = 2;
if -0.2<rank_me<= 0.2 then dgtw_port_me = 3;
if  0.2<rank_me<= 0.6 then dgtw_port_me = 4;
if  0.6<rank_me       then dgtw_port_me = 5;
run;

/* sort on bm */
proc sort data=da; by public_date dgtw_port_me; run;

proc univariate data=da noprint;
  where rank_bm~=0;
  var rank_bm;
  by public_date dgtw_port_me;
  output out=bm_bp pctlpre=rank_bm pctlpts= 20 to 80 by 20;
run;

proc print data=bm_bp; run;

proc sql;
  create table da as
  select a.*,
  b.rank_bm20,b.rank_bm40,b.rank_bm60,b.rank_bm80
  from da as a left join bm_bp as b
  on a.public_date=b.public_date and a.dgtw_port_me=b.dgtw_port_me;
quit;

data da;
set da;
if             rank_bm <= rank_bm20 then dgtw_port_bm=1;
if rank_bm20 < rank_bm <= rank_bm40 then dgtw_port_bm=2;
if rank_bm40 < rank_bm <= rank_bm60 then dgtw_port_bm=3;
if rank_bm60 < rank_bm <= rank_bm80 then dgtw_port_bm=4;
if rank_bm80 < rank_bm              then dgtw_port_bm=5;
run;

proc print data=da(obs=100); run;

/* sort on mom12m */
proc sort data=da; by public_date dgtw_port_me dgtw_port_bm; run;

proc univariate data=da noprint;
  where rank_mom12m~=0;
  var rank_mom12m;
  by public_date dgtw_port_me dgtw_port_bm;
  output out=mom12m_bp pctlpre=rank_mom12m pctlpts= 20 to 80 by 20;
run;

proc print data=mom12m_bp; run;

proc sql;
  create table da as
  select a.*,
  b.rank_mom12m20,b.rank_mom12m40,b.rank_mom12m60,b.rank_mom12m80
  from da as a left join mom12m_bp as b
  on a.public_date=b.public_date and a.dgtw_port_me=b.dgtw_port_me;
quit;

data da;
set da;
if                 rank_mom12m <= rank_mom12m20 then dgtw_port_mom12m=1;
if rank_mom12m20 < rank_mom12m <= rank_mom12m40 then dgtw_port_mom12m=2;
if rank_mom12m40 < rank_mom12m <= rank_mom12m60 then dgtw_port_mom12m=3;
if rank_mom12m60 < rank_mom12m <= rank_mom12m80 then dgtw_port_mom12m=4;
if rank_mom12m80 < rank_mom12m              then dgtw_port_mom12m=5;
run;

proc print data=da(obs=100); run;

/* combine */
data da;
set da;
DGTW_PORT=put(dgtw_port_me,1.)||put(dgtw_port_bm,1.)||put(dgtw_port_mom,1.);
run;

proc sort data=da out=da nodupkey; by public_date permno; run;
proc print data=da(obs=100);run;

data chars.v7_1_rank_label;
set da;
run;
