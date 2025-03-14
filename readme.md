# Work Log

## 2/3/25
* Used this ETL pipeline tutorial to get started - https://www.youtube.com/watch?v=goSGk2VwVcM
* Set up AWS root user and IAM user, granted permissions on IAM user
* Wrote the code for lambda function
* Created python layer for lambda, referencing this: https://docs.aws.amazon.com/lambda/latest/dg/python-layers.html
* Set up EventBridge scheduled trigger/rule. Pending to shift this from root account to IAM account
* Set up secrets, referencing this: https://www.youtube.com/watch?v=mNwWpW7cZo4

## 14/3/202
* Created pyspark scripts, will not use due to pandas being a better choice for smaller data
* Created transformation portion of pipeline

# Tasks

- [x] fix file name
- [x] make apikey a secret
- [x] clean and document initial pipelines
- [x] recommend storage solution
- [] operationalize deployment of code, layer, etc.
- [] shift trigger to IAM account

