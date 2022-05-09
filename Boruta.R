rm(list=ls())
host="hta-project.cf9mllj1rhry.us-east-2.rds.amazonaws.com"
port=3306
dbname="hta_project"
user="Sagi"
password = "HTAproject2022"
my_db=dbConnect(RMySQL::MySQL(),dbname=dbname,host=host,port=port,user=user,password=password)
print(getwd())

#install.packages("RMySQL")
#install.packages('Boruta')
#install.packages("dbConnect")
#install.packages('odbc')
library('Boruta')
library(dplyr)
library(RMySQL)
library(odbc)
# TODO: Read from database 
df<- read.csv("all_players.csv")

columns_to_not_train  = c("X","date","opponent","position","name","score","date_downloaded")
target = "instat_index"
n_iterations_boruta = 100 

POSITION_TO_RESEARCH = "LM" # "LD" "LM" "RM" "RD" "DM" "CM" "CD" "F"
run_boruta_and_write_z_to_csv = function(df,POSITION_TO_RESEARCH,columns_to_not_train,target,n_iterations_boruta,my_db){
  
  train = df[df$position == POSITION_TO_RESEARCH,]
  x_train =train[ , !(names(train) %in% c(columns_to_not_train,target))]
  y_train = train[, (names(train) %in% c(target))]
  
  bor <- Boruta(
    x_train,
    y_train,
    pValue = 0.01,
    mcAdj = TRUE,
    maxRuns = n_iterations_boruta,
    holdHistory = TRUE
  )
  
  z.scores.df = do.call(rbind.data.frame, bor[2]) # zscore of boruta for each featue rows = X iterations, cols= features
  # Drop rejected columns from boruta: 
  cols_to_drop <-  unique(which(z.scores.df == -Inf, arr.ind=TRUE)[,2])
  rownames(z.scores.df) <- 1:nrow(z.scores.df)
  z.scores.df <- z.scores.df[-cols_to_drop]
  # Write to DB
  if (paste0('zscores_',POSITION_TO_RESEARCH) %in% dbListTables(my_db)){
    dbRemoveTable(my_db,paste0('zscores_',POSITION_TO_RESEARCH))
    print('dropped table')
  }
  dbWriteTable(my_db,paste0('zscores_',POSITION_TO_RESEARCH),z.scores.df)
  print('Created Table')
  #write.csv(z.scores.df,paste('zscores_',POSITION_TO_RESEARCH,'.csv',sep=''), row.names = FALSE)
  return(bor)
}

# Run one positionn manually
#bor = run_boruta_and_write_z_to_csv(df,POSITION_TO_RESEARCH,columns_to_not_train,target,n_iterations_boruta,my_db)

# Run all positions
for (i in c("LD", "LM", "RM", "RD", "DM" ,"CM", "CD","F")){
  print(i)
  bor = run_boruta_and_write_z_to_csv(df,i,columns_to_not_train,target,n_iterations_boruta,my_db)
}


dbListTables(my_db)