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

data chars.v7_1_rank;
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
data da_bm_na;
set da;
where rank_bm=0;
dgtw_port_bm = 3;
run;

data da_bm;
set da;
where rank_bm~=0;
run;

proc sort data=da_bm out=da_bm1 nodupkey; by public_date dgtw_port_me permno; run;
proc rank data=da_bm1 out=da_bm2 group=5;
  by public_date dgtw_port_me;
  var rank_bm;
  ranks dgtw_port_bm;
run;

data da_bm2;
set da_bm2;
dgtw_port_bm = dgtw_port_bm+1;
run;

data da;
set da_bm_na da_bm2;
run;

proc sort data=da out=da nodupkey; by public_date permno; run;
proc print data=da(obs=100);run;

/* sort on mom */
data da_mom_na;
set da;
where rank_mom12m=0;
dgtw_port_mom = 3;
run;

data da_mom;
set da;
where rank_mom12m~=0;
run;

proc sort data=da_mom out=da_mom1 nodupkey;
by public_date dgtw_port_me dgtw_port_bm permno;
run;

proc rank data=da_mom1 out=da_mom2 group=5;
  by public_date dgtw_port_me dgtw_port_bm;
  var rank_mom12m;
  ranks dgtw_port_mom;
run;

data da_mom2;
set da_mom2;
dgtw_port_mom = dgtw_port_mom+1;
run;

data da;
set da_mom_na da_mom2;
run;

data da;
set da;
DGTW_PORT=put(dgtw_port_me,1.)||put(dgtw_port_bm,1.)||put(dgtw_port_mom,1.);
run;

proc sort data=da out=da nodupkey; by public_date permno; run;
proc print data=da(obs=100);run;


/* ********************************************* */
/*  Final results                     */
/* ********************************************* */

data da;
set chars.v7_1_rank_label;
run;

proc export data=da
outfile="/scratch/cityuhk/xinhe/eqchars/rank_final.csv" dbms=csv replace; run;

proc export data=da(where=(year(public_date)=2018))
outfile="/scratch/cityuhk/xinhe/eqchars/rank_final2018.csv" dbms=csv replace; run;
