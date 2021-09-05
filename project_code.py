# MAJOR PROJECT: AUTOMATION OF AWS S3 OBJECT STORAGE WITH PYTHON AND BOTO3

import boto3
import logging
import requests
import traceback
import botocore
from botocore.client import Config
import csv
import os
from botocore.exceptions import ClientError, ParamValidationError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def send_to_s3(target_dir, aws_region, bucket_name):     
    if not os.path.isdir(target_dir):
        raise ValueError('target_dir %r not found.' % target_dir)

    for filename in os.listdir(target_dir):
        logger.warning('Uploading %s to Amazon S3 bucket %s' % (filename, bucket_name))
        s3.Object(bucket_name, filename).put(Body=open(os.path.join(target_dir, filename), 'rb'))
        logger.info('FILE SUCCESSFULLY UPLOADED to https://s3.%s.amazonaws.com/%s/%s' % ( aws_region, bucket_name, filename))

def presigned_url(bucket_name, key_name):    
    client = boto3.client('s3',aws_access_key_id=data[0][1],aws_secret_access_key=data[1][1],region_name='ap-south-1',config=Config(signature_version='s3v4'))
    url = client.generate_presigned_url('get_object',{'Bucket' : bucket_name, 'Key' : key_name})
    response = requests.get(url)
    print("PRE-SIGNED URL GENERATED SUCCESSFULLY....... \n")
    print(response)
    print(url)


print("TAKING CREDENTIALS FROM rootkey file for CONNECTION.......... \n")
with open(r'C:\Users\koushik\Desktop\AWS_S3\access.csv','r') as f:                 
    data = list(csv.reader(f, delimiter = "="))
print("CONNECTING TO AMAZON WEB SERVICES AND GATHERING ALL RESOURCES............... \n")

s3 = boto3.resource('s3',aws_access_key_id = data[0][1],aws_secret_access_key = data[1][1],config = Config(signature_version='s3v4'))
for buckets in s3.buckets.all():
    print("*****CREDENTIALS VERIFIED....CONNECTION SUCCESSFULLY TO AWS S3*****\n")
    break
con = input("DO YOU WANT TO PERFORM OPERATIONS  (y/n) \n")     
while con != 'n' and con == 'y':
    x = input("1.CREATE A NEW BUCKET \n2.INSERT A FILE(OBJECT) IN THE BUCKET \n3.DISPLAY LIST OF DATA BUCKETS,FOLDERS AND SUBFOLDERS \n4.DELETE BUCKET  \n5.DELETE CONTENTS \n6.DOWNLOAD CONTENTS \n7.MULTIPLE FILES UPLOAD \n8.READ FILE FROM S3 \n9.GENERATE PRE-SIGNED URL FOR DOWNLOAD\n(PLEASE ENTER YOUR CHOICE....)\n")
    if x == '1':
        print("LIST OF S3 DATA BUCKETS PRESENT \n")
        for bucket_list in s3.buckets.all():
            print("--> "+bucket_list.name)
        BUCKET_NAME = input("ENTER THE BUCKET NAME TO BE CREATED \n")
        s3.create_bucket(ACL='public-read',Bucket = BUCKET_NAME,CreateBucketConfiguration = {'LocationConstraint':'ap-south-1'})
        print("DATA BUCKET "+BUCKET_NAME+" created")
        print("LIST OF S3 DATA BUCKETS ASSOCIATED WITH AWS ACCOUNT....")
        for bucket in s3.buckets.all():
            print("--------------------------")
            print("*** "+ bucket.name)
            print("--------------------------")
            for my_bucket_object in bucket.objects.all():
                print(my_bucket_object.key)
            print("--------------------------")
    elif x == '2':
        try:
            print("LIST OF BUCKETS PRESENT \n")
            bl = [bucket.name for bucket in s3.buckets.all()]
            for i in range(0,len(bl)):
                print(i,"-->"+bl[i])
            ch = int(input("ENTER THE CHOICE OF THE DATA BUCKET THAT THE OPERATION SHOULD BE PERFORMED WITH \n"))
            path = input("ENTER THE PATH OF THE FILE YOU WANT TO UPLOAD TO THE BUCKET \n")
            data = open(path,'rb')
            name = input("Enter the name that you want to save the file with(ALONG WITH EXTENSION) \n")
            s3.Bucket(bl[ch]).put_object(Key= name, Body=data)
            print("FILE SUCCESSFULLY SAVED TO" +bl[ch]+" LOCATION WITH NAME "+name)
            print("LIST OF S3 DATA BUCKETS and OBJECTS ASSOCIATED WITH AWS S3 ACCOUNT \n")
            for bucket in s3.buckets.all():
                print("--------------------------")
                print("*** "+ bucket.name)
                print("--------------------------")
                for my_bucket_object in bucket.objects.all():
                    print(my_bucket_object.key)
                print("--------------------------")
        except:
            print("ERROR")
            traceback.print_exc()
    elif x == '3':
        try:
            print("LIST OF S3 DATA BUCKETS and OBJECTS ASSOCIATED WITH AWS ACCOUNT....")
            for bucket in s3.buckets.all():
                print("--------------------------")
                print("*** "+ bucket.name)
                print("--------------------------")
                for my_bucket_object in bucket.objects.all():
                    print(my_bucket_object.key)
                print("--------------------------")
        except:
            print("ERROR")
            traceback.print_exc()
    elif x == '4':
        try:
            ch1 = input("DO YOU WANT TO:..\n1.DELETE ENTIRE DATA BUCKET ALONG WITH CONTENTS \n2.DELETE CONTENTS AND RETAIN BUCKET \n")
            if ch1 == '1':
                print("LIST OF BUCKETS PRESENT \n")
                bl = [bucket.name for bucket in s3.buckets.all()]
                for i in range(0,len(bl)):
                    print(i,"-->"+bl[i])
                ch = int(input("ENTER THE CHOICE OF THE DATA BUCKET THAT THE OPERATION SHOULD BE PERFORMED WITH \n"))
                bucket = s3.Bucket(bl[ch])
                for key in bucket.objects.all():
                    key.delete()
                bucket.delete()
                print("S3 DATA BUCKET "+bl[ch]+" SUCCESSFULLY REMOVED \n")
                print("LIST OF S3 DATA BUCKETS and OBJECTS ASSOCIATED WITH AWS S3 ACCOUNT \n")
                for bucket in s3.buckets.all():
                    print("--------------------------")
                    print("*** "+ bucket.name)
                    print("--------------------------")
                    for my_bucket_object in bucket.objects.all():
                        print(my_bucket_object.key)
                    print("--------------------------")
                bl.clear()
            elif ch1 == '2':
                print("LIST OF BUCKETS PRESENT \n")
                bl = [bucket.name for bucket in s3.buckets.all()]
                for i in range(0,len(bl)):
                    print(i,"-->"+bl[i])
                ch = int(input("ENTER THE CHOICE OF THE DATA BUCKET THAT THE OPERATION SHOULD BE PERFORMED WITH \n"))
                bucket = s3.Bucket(bl[ch])
                bucket.objects.filter(Prefix="").delete()
                print("DATA BUCKET "+bl[ch]+" DATA CLEARED SUCCESSFULLY \n")
                print("AFTER SUCCESSFULL OPERATION, THE LIST OF DATA OBJECTS ARE AS FOLLOWS: \n")
                print("LIST OF S3 DATA BUCKETS and OBJECTS ASSOCIATED WITH AWS S3 ACCOUNT \n")
                for bucket in s3.buckets.all():
                    print("--------------------------")
                    print("*** "+ bucket.name)
                    print("--------------------------")
                    for my_bucket_object in bucket.objects.all():
                        print(my_bucket_object.key)
                bl.clear()
            else:
                print("PLEASE ENTER VALID OPTION \n")
        except:
            print("ERROR")
            traceback.print_exc()
    elif x == '5':
        try:
            bl = [bucket.name for bucket in s3.buckets.all()]
            print("LIST OF S3 DATA BUCKETS and OBJECTS ASSOCIATED WITH AWS S3 ACCOUNT \n")
            for i in range(0,len(bl)):
                print("-----------------------")
                print(i,"-->"+bl[i])
                print("-----------------------")
                for bucket in s3.buckets.all():
                    for my_bucket_object in bucket.objects.all():
                        if my_bucket_object.bucket_name == bl[i]:
                            print(my_bucket_object.key)
            ch = int(input("ENTER THE CHOICE OF THE DATA BUCKET THAT THE OPERATION SHOULD BE PERFORMED WITH \n"))
            ch1 = input("PLEASE ENTER YOUR CHOICE \n1.OBJECT FILE DELETION \n2.SUBFOLDER DELETION \n")
            if ch1 == '1':
                obj_name = input("Please enter object file name(WITH EXTENSION) \n")
                obj = s3.Object(bl[ch],obj_name)
                obj.delete()
                print(obj_name+" DELETED SUCCESSFULLY \n")
                print("LIST OF S3 DATA BUCKETS and OBJECTS ASSOCIATED WITH AWS S3 ACCOUNT \n")
                for bucket in s3.buckets.all():
                    print("--------------------------")
                    print("*** "+ bucket.name)
                    print("--------------------------")
                    for my_bucket_object in bucket.objects.all():
                        print(my_bucket_object.key)
                bl.clear()
            elif ch1 == '2':
                sub_folder = input("Please enter subfolder name ending with / \n")
                bucket = s3.Bucket(bl[ch])
                bucket.objects.filter(Prefix = sub_folder).delete()
                print(sub_folder+" AND CONTENTS SUCCESSFULLY DELETED FROM DATA BUCKET "+bl[ch])
                print("LIST OF S3 DATA BUCKETS and OBJECTS ASSOCIATED WITH AWS S3 ACCOUNT \n")
                for bucket in s3.buckets.all():
                    print("--------------------------")
                    print("*** "+ bucket.name)
                    print("--------------------------")
                    for my_bucket_object in bucket.objects.all():
                        print(my_bucket_object.key)
                bl.clear()
        except:
            print("ERROR")
            traceback.print_exc()
    elif x == '6':
        try:
            print("LIST OF S3 DATA BUCKETS and OBJECTS ASSOCIATED WITH AWS S3 ACCOUNT \n")
            bl = [bucket.name for bucket in s3.buckets.all()]
            for i in range(0,len(bl)):
                print(i,"-->"+bl[i])
                for bucket in s3.buckets.all():
                    for my_bucket_object in bucket.objects.all():
                        if my_bucket_object.bucket_name == bl[i]:
                            print(my_bucket_object.key)
            ch = int(input("ENTER THE CHOICE OF THE DATA BUCKET THAT THE OPERATION SHOULD BE PERFORMED WITH \n"))                                
            ch1 = input("PLEASE ENTER YOUR CHOICE \n1.SINGLE FILE DOWNLOAD \n2.MULTIPLE FILES DOWNLOAD \n")
            if ch1 == '1':
                file_name = input("ENTER the name of the file to be downloaded (with extension) \n")
                saved_name = input("ENTER THE NAME OF THE FILE YOU WANT TO SAVE WITH(WITH EXTENSION) \n")
                destination = input("ENTER the download destination path \n")
                KEY = file_name
                s3.Bucket(bl[ch]).download_file(KEY,destination+"/"+saved_name)
                print(file_name+" SUCCESSFULLY SAVED TO "+destination)
                bl.clear()
            elif ch1 == '2':                
                path = input("Please enter the destination path \n")
                bucky = s3.Bucket(bl[ch])
                for objs in bucky.objects.all():
                    try:	
                        bucky.download_file(objs.key,path + objs.key)
                        print(objs.key+" DOWNLOADED")
                    except:
                        print("ERROR")
                        traceback.print_exc()
                bl.clear()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise
    elif x == '7':
        bl = [bucket.name for bucket in s3.buckets.all()]
        for i in range(0,len(bl)):
            print(i,"-->"+bl[i])
        ch = int(input("ENTER THE CHOICE OF THE DATA BUCKET THAT THE OPERATION SHOULD BE PERFORMED WITH \n"))
        path = input("ENTER THE SYSTEM-PATH OF THE FILES YOU WANT TO UPLOAD \n")
        send_to_s3(path,'ap-south-1',bl[ch])
        bl.clear()
    elif x == '8':
        bl = [bucket.name for bucket in s3.buckets.all()]
        for i in range(0,len(bl)):
            print(i,"-->"+bl[i])
            for bucket in s3.buckets.all():
                for my_bucket_object in bucket.objects.all():
                    if my_bucket_object.bucket_name == bl[i]:
                        print(my_bucket_object.key)
        ch = int(input("ENTER THE CHOICE OF THE DATA BUCKET THAT THE OPERATION SHOULD BE PERFORMED WITH \n"))
        path_key = input("ENTER THE PATH OF THE FILE FROM THE BUCKET THAT YOU WANT TO READ FROM S3 \n")
        obj = s3.Object(bl[ch], path_key)
        print("-------------------------------------------------------------------------------------------")
        for line in obj.get()['Body']._raw_stream:
            print(line.decode("UTF8"))
        print("-------------------------------------------------------------------------------------------")
        bl.clear()
    elif x == '9':
        bl = [bucket.name for bucket in s3.buckets.all()]
        for i in range(0,len(bl)):
            print("------------------------------------------")
            print(i,"-->"+bl[i])
            print("------------------------------------------")
            for bucket in s3.buckets.all():
                for my_bucket_object in bucket.objects.all():
                    if my_bucket_object.bucket_name == bl[i]:
                        print(my_bucket_object.key)
        ch = int(input("ENTER THE CHOICE OF THE DATA BUCKET THAT THE OPERATION SHOULD BE PERFORMED WITH \n"))
        key = input("ENTER THE PATH OF THE OBJECT FILE \n")
        presigned_url(bl[ch],key)
        bl.clear()
    else:
        print("PLEASE ENTER A VALID OPTION...")
    con = input("DO YOU WANT TO CONTINUE(y/n) \n")
    
    
    

