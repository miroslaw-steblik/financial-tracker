#import boto3


#--------------------- s3 ----------------------------------------------------#
# bucket_name = 'finance-a'
# key = 'monthly-budget/financial_tracker.csv'

# """ config details for S3 are in ~/.aws/credentials """

# def load_s3():
#     s3 = boto3.client('s3')

#     s3.upload_file(combined_file_path, bucket_name, key)
#     print(f"File uploaded to s3://{bucket_name}/{key}")
#     return all_df