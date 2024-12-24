import json
import time

import boto3


def start_job(client, s3_bucket_name, object_name):
    response = None
    response = client.start_document_text_detection(
        DocumentLocation={
            'S3Object': {
                'Bucket': s3_bucket_name,
                'Name': object_name
            }}
    )

    return response["JobId"]

def is_job_complete(client, job_id):
    time.sleep(1)
    response = client.get_document_text_detection(JobId=job_id)
    status = response["JobStatus"]
    print("Job status: {}".format(status))

    while(status == "IN_PROGRESS"):
        time.sleep(1)
        response = client.get_document_text_detection(JobId=job_id)
        status = response["JobStatus"]
        print("Job status: {}".format(status))

    return status

def get_job_results(client, job_id):
    pages = []
    time.sleep(1)
    response = client.get_document_text_detection(JobId=job_id)
    pages.append(response)
    print("Resultset page received: {}".format(len(pages)))
    next_token = None
    if 'NextToken' in response:
        next_token = response['NextToken']

    while next_token:
        time.sleep(1)
        response = client.get_document_text_detection(JobId=job_id, NextToken=next_token)
        pages.append(response)
        print("Resultset page received: {}".format(len(pages)))
        next_token = None
        if 'NextToken' in response:
            next_token = response['NextToken']

    return pages

# def is_pdf_openable(bucket,key):
#     try:
#         response=s3_client.get_object(Bucket=bucket,Key=key)
#         pdf_content=response['Body'].read()
#         doc=fitz.open(stream=BytesIO(pdf_content))
#         doc.close()
#         return True
#     except Exception as e:
#         print(f"error opening {key}: {str(e)}")
#         return False
    


def lambda_handler(event, context):
    # TODO implement
    print(event)
    filetexts=dict()
    s3_bucket_name=event['Records'][0]['s3']['bucket']['name']
    document_name=event['Records'][0]['s3']['object']['key']
    
    print(s3_bucket_name,document_name)
    

    file=document_name.split('/')[-1]
    client = boto3.client('textract',region_name=boto3.Session().region_name,aws_access_key_id='AKIARU3MIISFVHPQBHLV',aws_secret_access_key='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')

    job_id = start_job(client, s3_bucket_name, document_name)
    print("Started job with id: {}".format(job_id))
    if is_job_complete(client, job_id):
        response = get_job_results(client, job_id)
    
    lines=[]
    for result_page in response:
        for item in result_page["Blocks"]:
            if item["BlockType"] == "LINE":
                print(item["Text"])
                lines.append(item["Text"])
    filetexts[file] = lines  
    
    for file, text in filetexts.items():
        file2 = file.split('.')[0]
        file2 = file.replace('.pdf','').replace('.','')
        with open(f'/tmp/{file2}.txt', 'w', encoding = "utf-8") as outfile:
            for line in lines:
                outfile.write(line + '\n')
            outfile.close()
        
    
    s3 = boto3.client("s3")
    s3.upload_file(Filename=f"/tmp/{file2}.txt", Bucket=s3_bucket_name, Key=f"sagemaker-pipelines-nlp-demo/code/{file2}.txt")
        
    return {
        'statusCode': 200,
        'body': json.dumps('text file written successfully!!')
    }
