

for (ind in c('port_me','port_beta','port_svar','FFI49_desc','DGTW_PORT')){
#for (ind in c('BM','INV','OP','MOM1M','MOM12M','MOM60M','ACC','BETA','CFP','EP','NI','RVAR','SVAR')){
  print(ind)
  # read <- paste0("a <- read.csv(","'download_return/ff6_me_",ind,".csv')")
  read <- paste0("a <- read.csv(","'replicate_return/vwret2_",ind,".csv')")
  eval(parse(text=read))
  a = a[,c(-1)]
  # eval(parse(text=paste0("ret_6_size_",ind," <- a[a[,1]>=197301 & a[,1]<=201812,-1]")))
  eval(parse(text=paste0("ret_",ind," <- a")))
  # eval(parse(text=paste0("ret_25_size_",ind," <- a")))
}

rm('a','ind','read')
