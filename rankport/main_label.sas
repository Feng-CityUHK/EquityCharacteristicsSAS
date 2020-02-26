%include 'portfolio_chars_feb2020.sas';

%let uni_begdt = 01JAN1976;
%let uni_enddt = 31DEC2018;

libname chars '/scratch/cityuhk/xinhe/eqchars';

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
    ret date
    public_date   permno   gvkey   sic  cusip
    &vars
    &rank_vars

    FFI10 FFI10_desc
    FFI30 FFI30_desc
    FFI49 FFI49_desc

    DGTW_PORT

    port_mom12m port_hxz_abr port_hxz_sue port_hxz_re
    port_bm port_ep port_cfp port_sp
    port_agr port_ni port_acc
    port_op port_roe
    port_seas1a port_adm port_rdm
    port_me port_svar port_beta port_mom1m;
run;

data da;
set da;
weight_port = me;
run;

data eqchars;
set da;
keep
    ret date
    public_date   permno   gvkey   sic  cusip
    weight_port
    &vars
    &rank_vars

    FFI10 FFI10_desc
    FFI30 FFI30_desc
    FFI49 FFI49_desc

    DGTW_PORT

    port_mom12m port_hxz_abr port_hxz_sue port_hxz_re
    port_bm port_ep port_cfp port_sp
    port_agr port_ni port_acc
    port_op port_roe
    port_seas1a port_adm port_rdm
    port_me port_svar port_beta port_mom1m;
run;

/* ********************************************************************************* */

%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=port_mom12m, AVR=mean, weight=weight_port, Input=eqchars, vars=&rank_vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=port_hxz_re, AVR=mean, weight=weight_port, Input=eqchars, vars=&rank_vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=port_hxz_sue, AVR=mean, weight=weight_port, Input=eqchars, vars=&rank_vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=port_hxz_abr, AVR=mean, weight=weight_port, Input=eqchars, vars=&rank_vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=port_bm, AVR=mean, weight=weight_port, Input=eqchars, vars=&rank_vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=port_ep, AVR=mean, weight=weight_port, Input=eqchars, vars=&rank_vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=port_cfp, AVR=mean, weight=weight_port, Input=eqchars, vars=&rank_vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=port_sp, AVR=mean, weight=weight_port, Input=eqchars, vars=&rank_vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=port_agr, AVR=mean, weight=weight_port, Input=eqchars, vars=&rank_vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=port_ni, AVR=mean, weight=weight_port, Input=eqchars, vars=&rank_vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=port_acc, AVR=mean, weight=weight_port, Input=eqchars, vars=&rank_vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=port_op, AVR=mean, weight=weight_port, Input=eqchars, vars=&rank_vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=port_roe, AVR=mean, weight=weight_port, Input=eqchars, vars=&rank_vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=port_seas1a, AVR=mean, weight=weight_port, Input=eqchars, vars=&rank_vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=port_adm, AVR=mean, weight=weight_port, Input=eqchars, vars=&rank_vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=port_rdm, AVR=mean, weight=weight_port, Input=eqchars, vars=&rank_vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=port_me, AVR=mean, weight=weight_port, Input=eqchars, vars=&rank_vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=port_svar, AVR=mean, weight=weight_port, Input=eqchars, vars=&rank_vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=port_beta, AVR=mean, weight=weight_port, Input=eqchars, vars=&rank_vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=port_mom1m, AVR=mean, weight=weight_port, Input=eqchars, vars=&rank_vars);

%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=FFI10_desc, AVR=mean, weight=weight_port, Input=eqchars, vars=&rank_vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=FFI30_desc, AVR=mean, weight=weight_port, Input=eqchars, vars=&rank_vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=FFI49_desc, AVR=mean, weight=weight_port, Input=eqchars, vars=&rank_vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=DGTW_PORT, AVR=mean, weight=weight_port, Input=eqchars, vars=&rank_vars);
