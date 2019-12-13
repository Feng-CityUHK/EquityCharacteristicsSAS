libname chars '/scratch/cityuhk/xinhe_mandy/eqchars';


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
