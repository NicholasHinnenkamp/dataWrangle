[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-c66648af7eb3fe8bc4f294546bfd86ef473780cde1dea487d3c4ff354943c9ae.svg)](https://classroom.github.com/online_ide?assignment_repo_id=9109572&assignment_repo_type=AssignmentRepo)
# csci422-datawrangling-assgnmt (75 points total)

This assignment uses PySpark in Azure Databricks to clean a mock up of a real world data format.  
After cleaning, several analysis questions will need to be answered by querying the cleaned dataset.

## About the data
The input dataset is a mockup of data sourced from an electrical meter reading system.  Each row of the data represents a full day of hourly samples for a given meter. In addition to the value of electrical usage in kilowatt-hours for each hour, there is a QC code indicating if the reading is a good read or not. A QC code of '3' represents a good reading; any other value is a bad reading.

The input data format is an ASCII formatted file. The delimiter for each column is '|'. Each row contains information about the customer, the meter, and 48 columns representing the QC code and value for 24 hours of data.

In addition to the meter reading file, there's a data file containing more information about each customer and meter.  This is called 'CustMeter.csv'.

A given Customer Account Number can have multiple Meter Numbers.  Each Meter Number may have multiple Data Types.  

The data is located in this repo in the \InputData folder.

## Assignment Part 1 - Azure set up (15 points)
The following resources should be set up in an Azure resource group:
- A storage account
- A key vault
- A Databricks instance

The storage account should contain a blob storage container for the assignment with /input and /output 'folders'. The input data should be uploaded to the /input folder.

The key vault should have two secrets. One secret will contain the storage account access key that Databricks will use to access the storage account. The second secret will contain the GitHub Personal Access Token (PAT) used to integrate your instance of this repo with Databricks.

The Databricks instance should use a secret scope to connect with the key vault. It also should be set up to integrate with your instance of this repo using the PAT.

HandIn - Take the required screenshots to provide evidence of the setup in your Azure subscription.  Save the screenshots in the \AzureSetup folder of this repo.

## Assignment Part 2 - Clean data (39 points)
Write the PySpark code using a Databricks notebook named 'CleanMeterData.py' that will take the input data and transform it to a cleaned result data with the following characteristics:
- Conversion from wide format to long format. Each long format row will contain the columns not associated with the meter readings (all columns up to QC#1), a single hour (integer from 1-24), and associated QC code and value.  The names of the new columns should be 'IntervalHour', 'QCCode', and 'IntervalValue'.  To be clear, each input row of 24 hours should be converted to 24 separate rows.
- All readings with "Data Type" values of "Reverse Energy in Wh" or "Net Energy in WH" should be deleted.
- All bad readings (a QC code other that '3') should be deleted.  This should include empty values.
- Any duplicate readings should be deleted.
- Data should be sorted in ascending customer, meter, datatype, date, and interval hour (note - interval hour should be an integer and the other columns should be strings).
- The long form cleaned file should be saved in two formats:
    - A single CSV file in /output/CSV in the storage account.
    - A single Parquet file in /Output/Parquet in the storage account.
- The customer data file should be directly copied from the input file to /Output/CustomerData in CSV format.
- HandIn - Download the CSV and Parquet files and save in the \CleanedData folder of this repo.
- HandIn - Screenshot the storage account with the output data files and save in the \CleanedData folder of this repo.
- HandIn - Save the notebook in the \Src folder of this repo.

## Assignment Part 3 - Analysis (21 points)
In a new PySpark notebook named 'AnalyzeData.py', write the code to answer the questions in \Analysis\Questions.txt file. Read the Parquet version of the cleaned data for analysis. Each question should have its own notebook cell.
- HandIn - Questions.txt with the answers to the questions (submitted via GitHub)
- HandIn - Save the notebook in the \Src folder of this repo.

Remember to commit all repo changes and push the repo!





