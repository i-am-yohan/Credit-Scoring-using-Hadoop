#Ensure Kaggle API and R are installed before running this script

#1. Make and Assign Directories
#cd ~/DIA_Project

#2. Add Input Data
mkdir Input_Data
cd Input_Data

#3. Home Credit Defaults
mkdir Home_Credit_Defaults
cd Home_Credit_Defaults
~/.local/bin/kaggle competitions download -c home-credit-default-risk
unzip home-credit-default-risk.zip
#Remove unnecessary data -  save some space
rm bureau.csv
rm credit_card_balance.csv
rm sample_submission.csv
rm bureau_balance.csv
rm application_test.csv
rm installments_payments.csv
rm POS_CASH_balance.csv
rm previous_application.csv

#4. Vehicle Loan Defaults
cd ../
mkdir Vehicle_Loan_Defaults
cd Vehicle_Loan_Defaults
wget https://github.com/NishantBhavsar/ltfs-vehicle-loan-default-prediction/raw/master/input/train.csv

#5. Clean the data using R
cd ../../
mkdir Input_Data/Cleaned_Data
Rscript Scripts/Data_Clean.R Input_Data/Home_Credit_Defaults/application_train.csv Input_Data/Vehicle_Loan_Defaults/train.csv Input_Data/Cleaned_Data

#6. Combine Common Columns
Rscript Scripts/Conform_And_Combine.R Input_Data/Cleaned_Data

#7. Start Hadoop ensure as distribution
#start-dfs.sh
#start-yarn.sh

#8. Send to HDFS
cd Input_Data
hdfs dfs -mkdir /Project
hdfs dfs -copyFromLocal MR_Input.csv /Project

#9. Execute Java hadoop map-reduce
# WARNING: This command removes previous output data,
# comment it if this behavior is not desired
cd ../
cd Scripts
hdfs dfs -rm -r /Project/Project_Out

# Compile
hadoop com.sun.tools.javac.Main DIA_Project_Development.java
# Generate JAR
jar cf DIA_Project_Development.jar *.*
# Execute
hadoop jar DIA_Project_Development.jar DIA_Project_Development /Project/MR_Input.csv /Project/Project_Out


#10. Run the logistic regression
# Compile
hadoop com.sun.tools.javac.Main Logistic_Regression_Driver.java Logistic_Regression_Mapper.java Logistic_Regression_Reducer.java
# Generate JAR
jar cf Logistic_Regression_Driver.jar *.*
# Execute
hadoop jar Logistic_Regression_Driver.jar Logistic_Regression_Driver 4 0.01 200 /Project/Project_Out/ABT /Project/Project_Out/Log_Reg_Theta



