# create tsplot folder
if (file.exists("tsplot")){
} else{
  dir.create(file.path(getwd(),"tsplot"))
}

# load in rdata
load('rank_20zt_port.RData')

# list variables of interest
target_var_list = c('rank_mom12m_Mean', 'rank_hxz_abr_Mean', 'rank_hxz_sue_Mean', 'rank_hxz_re_Mean',
                    'rank_bm_Mean', 'rank_ep_Mean', 'rank_cfp_Mean', 'rank_sp_Mean',
                    'rank_agr_Mean', 'rank_ni_Mean', 'rank_acc_Mean',
                    'rank_op_Mean', 'rank_roe_Mean',
                    'rank_seas1a_Mean', 'rank_adm_Mean', 'rank_rdm_Mean',
                    'rank_me_Mean', 'rank_svar_Mean', 'rank_beta_Mean', 'rank_mom1m_Mean')


length(target_var_list)

# list different sort classes
sort_list <- c('port_me','port_beta','port_svar','FFI49_desc','DGTW_PORT')
#sort_list <- c('BM','INV','OP','MOM1M','MOM12M','MOM60M','ACC','BETA','CFP','EP','NI','RVAR','SVAR')

### plot
for (sort in sort_list){
  #SORT <- toupper(sort)
  SORT<-sort
  if (file.exists(paste0("tsplot/chars_",sort))){
  } else{
    dir.create(file.path(getwd(),paste0("tsplot/chars_",sort)))
  }

  tx <- paste0('a10 <- chars_',SORT)
  eval(parse(text=tx))

  ind_list = names(a10['1976-07-31',,'rank_me_Mean'])

  for (var in target_var_list){
    print(var)

    mainDir <- getwd()
    subDir <- paste0("tsplot/chars_",sort,"/",var)
    if (file.exists(subDir)){
      #setwd(file.path(mainDir, subDir))
    } else {
      dir.create(file.path(mainDir, subDir))
      #setwd(file.path(mainDir, subDir))
    }

    # png(paste0(getwd(),"/tsplot/sort10_",sort,"/",var,"/",var,'_boxplot.png'), width = 3200, height = 2400)
    # # make plot
    # boxplot(a10[,,var], main=paste0('boxplot of ',var,' time:1963JUN-2018DEC'))
    # dev.off()

    png(paste0(getwd(),"/tsplot/chars_",sort,"/",var,"/",'allinone.png'), width = 1600, height = 1200)

    first_port <- colnames(a10)[1]
    plot(as.Date(names(a10[,paste0(first_port),var])),
          a10[,paste0(first_port),var],
          main=paste0('time series of ',var,' of ',sort,' time:1976JAN-2018DEC'),
          xlab = "date",
          ylab = "value",
          type='l',
         ylim = c(-1,1),
         col=1
         )

    i = 0
    for (ind in ind_list){
      i=i+1
      # make plot
      lines(as.Date(names(a10[,ind,var])),
           a10[,ind,var],
           type='l',
           col=i)
    }
    legend('topright', legend=c(1:10),
           col=c(1:10), lty=1, cex=0.8)
    dev.off()
  }

}



# in one plot
