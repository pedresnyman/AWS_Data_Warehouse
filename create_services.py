from aws_services import create_IAM_role, create_ec2_security_group, create_cluster, get_cluster_status
import configparser
import boto3
import time

def create_services():
    # CONFIG
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    redshift = boto3.client('redshift')
    iam = boto3.client('iam')
    ec2 = boto3.client('ec2')

    create_IAM_role(iam)
    create_ec2_security_group(ec2)

    iam_role_arn = iam.get_role(RoleName=config.get('IAM_ROLE', 'NAME'))['Role']['Arn']

    for security_group in ec2.describe_security_groups()['SecurityGroups']:
        if security_group['GroupName'] == config.get('SECURITY_GROUP', 'NAME'):
            vpc_security_group_id = security_group['GroupId']

    create_cluster(redshift, iam_role_arn, vpc_security_group_id)

    while get_cluster_status(redshift) != 'AVAILABLE':
        time.sleep(5)

    print('All services has been created and redshift cluster is active')