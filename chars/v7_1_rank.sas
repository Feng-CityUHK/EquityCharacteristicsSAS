libname chars '/scratch/cityuhk/xinhe/eqchars';

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
set chars.firmchars_v7_1_final;
keep
  public_date   permno   gvkey   sic  cusip
  FFI5_desc   FFI5   FFI10_desc   FFI10   FFI12_desc   FFI12   FFI17_desc   FFI17
  FFI30_desc   FFI30   FFI38_desc   FFI38   FFI48_desc   FFI48  FFI49_desc   FFI49
  &vars_rank;
run;

data chars.v7_1_rank_label;
set da;
run;

/* ********************************************* */
/*  Loop function                   */
/* ********************************************* */

%macro loop(vars_rank);
%local i next_name;
%do i=1 %to %sysfunc(countw(&vars_rank));
   %let next_name = %scan(&vars_rank, &i);

   /* ********************************************* */
   /*  Get ranks                      */
   /* ********************************************* */

   proc rank data=da out=darank ties=mean;
      by public_date;
      var &next_name;
      ranks rk_&next_name;
   run;

   /* ********************************************* */
   /*  get no. of obs. in each date-var             */
   /* ********************************************* */
   proc sql;
   create table
   	darank1 as
   select
   	a.*, count(a.rk_&next_name) as n_&next_name
   from
   	darank a group by public_date
   order by
   	public_date, permno
   ;
   quit;

   /* ********************************************* */
   /*  standardize to -1 1                      */
   /* ********************************************* */

   data darank2;
   set darank1;
   rank_&next_name = (rk_&next_name-1)/(n_&next_name-1)*2-1;
   if missing(rank_&next_name) then rank_&next_name=0;
   run;

   /* ********************************************* */
   /*  merge to da                      */
   /* ********************************************* */
   proc sql;
   create table da as
   select a.*, b.rank_&next_name
   from da a left join darank2 b on
   a.permno=b.permno and a.public_date=b.public_date
   order by public_date, permno;
   quit;

   /* ********************************************* */
   /*  assign bucket label                      */
   /* ********************************************* */
   data da;
   set da;
   port_&next_name=.;
   if      rank_&next_name<=-0.8 then port_&next_name=0;
   if -0.8<rank_&next_name<=-0.6 then port_&next_name=1;
   if -0.6<rank_&next_name<=-0.4 then port_&next_name=2;
   if -0.4<rank_&next_name<=-0.2 then port_&next_name=3;
   if -0.2<rank_&next_name<=   0 then port_&next_name=4;
   if    0<rank_&next_name<= 0.2 then port_&next_name=5;
   if  0.2<rank_&next_name<= 0.4 then port_&next_name=6;
   if  0.4<rank_&next_name<= 0.6 then port_&next_name=7;
   if  0.6<rank_&next_name<= 0.8 then port_&next_name=8;
   if  0.8<rank_&next_name       then port_&next_name=9;
   run;

   data chars.v7_1_rank_label;
   set da;
   run;

%end;
%mend;

%loop(&vars_rank);

/* ********************************************* */
/*  DGTW  label                   */
/*  simple Breakpoints */
/*  monthly dependent sorting */
/*  ME BM MOM */
/*  No industry adjustment for BM */
/* ********************************************* */
data da;
set chars.v7_1_rank_label;
run;

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
DGTW_PORT=put(dgtw_port_me,1.)||put(dgtw_port_bm,1.)||put(dgtw_port_mom12m,1.);
run;

proc sort data=da out=da nodupkey; by public_date permno; run;
proc print data=da(obs=100);run;

data chars.v7_1_rank_label;
set da;
run;

/* ********************************************* */
/*  Final results                     */
/* ********************************************* */
%let vars =
mom12m hxz_abr hxz_sue hxz_re
bm ep cfp sp
agr ni acc
op roe
seas1a adm rdm
me svar beta mom1m
;

%let rank_vars =
rank_mom12m rank_hxz_abr rank_hxz_sue rank_hxz_re
rank_bm rank_ep rank_cfp rank_sp
rank_agr rank_ni rank_acc
rank_op rank_roe
rank_seas1a rank_adm rank_rdm
rank_me rank_svar rank_beta rank_mom1m
;

/* ********************************************* */
/*  Final results                     */
/* ********************************************* */

data da;
set chars.v7_1_rank_label;
keep
    public_date   permno   gvkey   sic  cusip
    &vars
    &rank_vars
    FFI49 FFI49_desc
    DGTW_PORT
    port_me port_beta port_svar;
run;

proc export data=da
outfile="/scratch/cityuhk/xinhe/eqchars/rank_final.csv" dbms=csv replace; run;

proc export data=da(where=(year(public_date)=2018))
outfile="/scratch/cityuhk/xinhe/eqchars/rank_final2018.csv" dbms=csv replace; run;


data da;
set da;
weight_port = me;
run;

data eqchars;
set da;
keep
    public_date   permno   gvkey   sic  cusip
    weight_port
    &vars
    &rank_vars
    FFI49 FFI49_desc
    DGTW_PORT
    port_me port_beta port_svar;
run;

proc export data=da
outfile="/scratch/cityuhk/xinhe/eqchars/rank_final_jingyu.csv" dbms=csv replace; run;

proc export data=da(where=(year(public_date)=2018))
outfile="/scratch/cityuhk/xinhe/eqchars/rank_final_jingyu2018.csv" dbms=csv replace; run;

/* merge_returns */

proc sql;
creat table da1 as
select a.*, b.ret from
da a left join crsp.msf b on
a.permno = b.permno and
intnx('month',a.public_date,0,'E') = intnx('month',b.date,-1,'E')
order by public_date, permno;
quit;

proc export data=da1
outfile="/scratch/cityuhk/xinhe/eqchars/eqchars_final.csv" dbms=csv replace; run;

proc export data=da1(where=(year(public_date)=2018))
outfile="/scratch/cityuhk/xinhe/eqchars/eqchars_final_2018.csv" dbms=csv replace; run;
