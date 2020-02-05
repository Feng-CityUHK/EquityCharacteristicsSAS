library(abind)

target_var_list = c('mom12m_Mean', 'hxz_abr_Mean', 'hxz_sue_Mean', 'hxz_re_Mean',
'bm_Mean', 'ep_Mean', 'cfp_Mean', 'sp_Mean',
'agr_Mean', 'ni_Mean', 'acc_Mean',
'op_Mean', 'roe_Mean',
'seas1a_Mean', 'adm_Mean', 'rdm_Mean',
'me_Mean', 'svar_Mean', 'beta_Mean', 'mom1m_Mean')

length(target_var_list)

# read pivot tables by python
for (ind in c('port_me','port_beta','port_svar','FFI49_desc','DGTW_PORT')){
#for (ind in c('BM','INV','OP','MOM1M','MOM12M','MOM60M','ACC','BETA','CFP','EP','NI','RVAR','SVAR')){
  print(ind)
  n=10
  if (ind=='FFI49_desc'){
    n=49
  }
  if (ind=='DGTW_PORT'){
    n=125
  }
  i=0
  for (var_name in target_var_list){
    i=i+1
    pvtb <- read.csv(paste0(getwd(),'/pivot_result/ranklabel_',ind,'/',var_name,'.csv'))
    m <- pvtb[,2:(n+1)]
    assign(paste("df", i, sep = ""), m)
  }
  print(i)
  dim(m)

  lhs <- paste0('df', 1:i, seq="")
  comd <- paste(lhs, collapse=",")
  eq <- paste0('bigar <- abind( ', comd, ', along=3 )')
  eval(parse(text=eq))
  ar <- array(bigar,
              dim=c(dim(m)[1],n,length(target_var_list)),
              dimnames=list(
                dates <- pvtb[,1],
                ind_names <- c(names(pvtb)[-1]),
                target_var_list
              )
              )

  eval(parse(text=paste0('chars_',ind,'<-','ar')))

  y <- ar[,,'bm_Mean']
  #print(y)
}


rm(list=setdiff(ls(), c("chars_port_me","chars_port_beta","chars_port_svar",
                        "chars_FFI49_desc","chars_DGTW_PORT"
                        )))

# x<-a6['1990-01-31', , ]
# y<-a6[ , ,'bm_Mean']
# z<-a6[ , 'ME2_BM2', ]

# a6['1990-01-31', ,'bm_Mean']

#chars_bm <- a6[,c(6,21,22,23,24,5,1,2,3,4,10,6,7,8,9,15,11,12,13,14,20,16,17,18,19),]

# # adjust sequence
# a6['1990-12-31',,'mktcap_Mean']
# b10 <- chars[,c(6,1,5,2,3,9,8,4,10,7),]
# b30 <- a30[,c(13,2,24,14,3,16,7,15,6,28,8,6,11,10,1,5,18,9,19,29,26,23,4,21,27,30,22,17,12,20),]
# b49 <- a49[,c(2,19,41,5,40,45,20,7,6,12,24,30,15,10,38,47,6,13,43,17,28,16,3,1,39,22,21,31,14,32,48,44,35,9,23,42,11,27,34,8,46,49,37,29,4,26,36,18,33),]
#
# chars<-b10
# a30<-b30
# a49<-b49
# rm(list=setdiff(ls(), c("chars_bm")))
