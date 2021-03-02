from aws_services import delete_IAM_role, delete_ec2_security_group, delete_cluster
import boto3

def delete_services():

    redshift = boto3.client('redshift')
    iam = boto3.client('iam')
    ec2 = boto3.client('ec2')

    delete_cluster(redshift)
    delete_IAM_role(iam)
    delete_ec2_security_group(ec2)

    print('All services has been deleted')
