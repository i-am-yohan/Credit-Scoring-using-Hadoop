#Importing the data and isntalling the packages
args = commandArgs(trailingOnly=TRUE)
list.of.packages <- 'readr'
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

library('readr')

#Mortgage Data
MTG <- read.csv(paste(args[1],'Home_Credit_Defaults.csv',sep='/') , stringsAsFactors = TRUE)

#Vehicle Loan Data
RL <- read.csv(paste(args[1],'Vehicle_Loan_Defaults.csv',sep='/'), stringsAsFactors = TRUE)

#Renaming the target variable to match
names(MTG)[names(MTG) == "TARGET"] <- "DEFAULT_IND"
names(RL)[names(RL) == "LOAN_DEFAULT"] <- "DEFAULT_IND"

#Need ID columns - Put Vehichle Loans as Negative to identify
names(MTG)[names(MTG) == "SK_ID_CURR"] <- "ID"
RL["ID"] <- RL["UNIQUEID"]*-1
MTG
#Identify the two loan types by portfolio
MTG['PORTFOLIO'] <- 'Mortgage'
RL['PORTFOLIO'] <- 'Vehicle Loan'

#LTV - important for Credit scoring
MTG['LTV'] <- MTG['AMT_CREDIT']/MTG['AMT_GOODS_PRICE']
RL['LTV'] <- RL['LTV']/100

#Installment amount
MTG['INTSLMNT_AMT'] <- MTG['AMT_ANNUITY']/12
RL['INTSLMNT_AMT'] <- RL['PRIMARY_INSTAL_AMT']*0.013 #Convert Rubles to USD

#Convert Date of Birth to age of client at application/Disbursal
MTG['AGE'] <- MTG['DAYS_BIRTH']/-365.25
RL['AGE'] <- (as.Date(RL$DISBURSAL_DATE, format='%Y-%m-%d') - as.Date(RL$DATE_OF_BIRTH, format='%Y-%m-%d'))/365.25

#Mobile Phone Provided - Might leave this one out
MTG['MOBILE'] <- MTG['FLAG_MOBIL']
RL['MOBILE'] <- RL['MOBILENO_AVL_FLAG']

#External Credit Score - Bring to same scale
MTG['EXT_SCORE'] <- (MTG['Avg_Ext_Scre'] - min(MTG['Avg_Ext_Scre']))/(max(MTG['Avg_Ext_Scre']) - min(MTG['Avg_Ext_Scre']))
RL['EXT_SCORE'] <- (RL['PERFORM_CNS_SCORE'] - min(RL['PERFORM_CNS_SCORE']))/(max(RL['PERFORM_CNS_SCORE']) - min(RL['PERFORM_CNS_SCORE']))

#The columns I need to keep
Keep_Cols <- c("DEFAULT_IND", "ID","PORTFOLIO" , "LTV" , 'INTSLMNT_AMT' , 'RL' , 'AGE'  , 'EXT_SCORE')

#The columns I need
MTG_out <- MTG[names(MTG) %in% Keep_Cols]
RL_out <- RL[names(RL) %in% Keep_Cols]

Output <- rbind(MTG_out , RL_out)

write.csv(Output , 'Input_Data/MR_Input.csv' , row.names = FALSE, quote = FALSE)


