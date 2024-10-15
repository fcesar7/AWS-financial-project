# End-to-end Hybrid Data Architecture

## Project Description

This project consists on building an end-to-end data project. The data architecture is hybrid, meaning it is partly hosted on cloud services provider AWS and partly locally. It focus on extracting financial data via API services from Alpha Vantage and St. Louis Fed Web Services (FRED API), applying an XGBoost machine learning algorithm to evaluate the price of gold. Data will be stored in AWS S3 and the scripts will be computed in AWS Lambda. The results and curated data will be queried from AWS to Power BI, resulting in an interactive dashboard.

## Procedure
### 1. Extraction
#### 1.1 Get API keys
API keys need to be obtained from Alpha Vantage and FRED websites. They are required in order to make API requests to the respective services.
#### 1.2 Extraction Script
On folder AWS-financial-project-1/aws_files, there is a file named 'extraction_lambda.py'. This is the python script we will upload to AWS Lambda.
#### 1.3 IAM Permissions
We need to make sure that our Lambda function is allowed to interact with S3 and EventBridge. S3 is the AWS object storage service where we will store our extracted data in this data staging phase. We will use EventBridge to orchestrate our data pipeline.
Go to IAM, click 'Roles' under 'Access management'.

![image info](./images/Picture1.png)

Select "Create Role". Then click on 'AWS Service' as the 'Trusted entity type'.

![image info](./images/Picture2.png)

Select that you want to give permissions to Lambda. Then choose “AmazonS3FullAccess” and “AmazonEventBridgeFullAccess”

#### 1.4 S3 Bucket
Create an S3 bucket and name it as you like. The bucket should be in the same region as the Lambda function. Create a folder in the bucket named 'extraction-staging' that will contain our extracted data.


