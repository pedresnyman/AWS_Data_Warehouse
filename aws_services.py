import configparser
import json
import time
import psycopg2
import sqlparse
import pandas as pd

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

def create_IAM_role(iam):
    """
    Create IAM Role and attachment of policy.
    :param iam: an IAM service client instance
    :return: True if IAM role created and policy applied successfully.
             False if IAM role or policy has bad requests.
    """

    role_name = config.get('IAM_ROLE', 'NAME')
    role_description = config.get('IAM_ROLE', 'DESCRIPTION')
    role_policy_arn = config.get('IAM_ROLE','POLICY_ARN')

    print(f"Creating IAM role with name : {role_name}, description : {role_description} and policy : {role_policy_arn}")

    # IAM Policy for role.
    role_policy_document = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": { "Service": [ "redshift.amazonaws.com" ] },
                    "Action": [ "sts:AssumeRole" ]
                }
            ]
        }
    )
    # Check if role exists
    for roles in iam.list_roles()['Roles']:
        if roles['RoleName'] == role_name:
            print(f'Role {role_name} already exist!')
            return True

    # Creation of IAM role.
    try:
        create_response = iam.create_role(
                    Path='/',
                    RoleName=role_name,
                    Description=role_description,
                    AssumeRolePolicyDocument = role_policy_document
        )
        print(f'Got response from IAM client for creating role : {create_response}')
        if create_response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise Exception(f'Bad Response! Response Code:{create_response["ResponseMetadata"]["HTTPStatusCode"]}')
    except Exception as e:
        print(f'Error occured while creating role : {e}')
        return False

    # Attach IAM policy to IAM created role
    try:
        policy_response = iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn=role_policy_arn
        )
        print(f'Got response from IAM client for applying policy to role : {policy_response}')
        if policy_response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise Exception(f'Bad Response! Response Code:{policy_response["ResponseMetadata"]["HTTPStatusCode"]}')
    except Exception as e:
        print(f'Error occured while applying policy : {e}')
        return False

    return True if(
            (create_response['ResponseMetadata']['HTTPStatusCode'] == 200) and (policy_response['ResponseMetadata']['HTTPStatusCode'] == 200)
    ) else False

def delete_IAM_role(iam):
    """
    Detach IAM role policy and delete IAM role.
    :param iam_client: an IAM service client instance
    :return: True if role deleted successfully.
    """

    role_name = config.get('IAM_ROLE', 'NAME')
    existing_roles = [role['RoleName'] for role in iam.list_roles()['Roles']]
    if (role_name not in existing_roles):
        print(f"Role {role_name} does not exist.")
        return True

    print(f'Processing deleting IAM role : {role_name}')
    try:
        detach_response = iam.detach_role_policy(
            RoleName=role_name,
            PolicyArn=config.get('IAM_ROLE', 'POLICY_ARN')
        )
        print(f'Response for policy detach from IAM role : {detach_response}')
        if detach_response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise Exception(f'Bad Response! Response Code:{detach_response["ResponseMetadata"]["HTTPStatusCode"]}')
        delete_response = iam.delete_role(
            RoleName=role_name
        )
        print(f'Response for deleting IAM role : {delete_response}')
        if delete_response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise Exception(f'Bad Response! Response Code:{delete_response["ResponseMetadata"]["HTTPStatusCode"]}')
    except Exception as e:
        print(f'Exception occured while deleting role : {e}')
        return False

    return True if (
            (detach_response['ResponseMetadata']['HTTPStatusCode'] == 200) and (
                delete_response['ResponseMetadata']['HTTPStatusCode'] == 200)
    ) else False

def create_ec2_security_group(ec2):
    """
    Create EC2 security group.
    :param ec2: an EC2 service client instance
    :return: True if IAM role created and policy applied successfully.
             False if IAM role or policy has bad requests.
    """
    # Check if group exists.
    group_name = config.get('SECURITY_GROUP','NAME')
    group = ec2.describe_security_groups(Filters=[{'Name': 'group-name', 'Values': [group_name]}])['SecurityGroups']
    if group:
        print('Group already exists!')
        return True

    # Get VPC ID
    vpc_id = ec2.describe_security_groups()['SecurityGroups'][0]['VpcId']

    response = ec2.create_security_group(
        Description=config.get('SECURITY_GROUP','DESCRIPTION'),
        GroupName=config.get('SECURITY_GROUP','NAME'),
        VpcId=vpc_id,
        DryRun=False # Checks whether you have the required permissions for the action, without actually making the request, and provides an error response
    )
    print(f'Security group creation response : {response}')
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        raise Exception(f'Bad Response! Response Code:{response["ResponseMetadata"]["HTTPStatusCode"]}')

    print("Authorizing security group ingress")
    ec2.authorize_security_group_ingress(
                GroupId=response['GroupId'],
                GroupName=config.get('SECURITY_GROUP','NAME'),
                FromPort=int(config.get('INBOUND_RULE','PORT_RANGE')),
                ToPort=int(config.get('INBOUND_RULE', 'PORT_RANGE')),
                CidrIp=config.get('INBOUND_RULE','CIDRIP'),
                IpProtocol=config.get('INBOUND_RULE','PROTOCOL'),
                DryRun=False
    )
    print(f'Security group {group_name} created successful!')
    return (response['ResponseMetadata']['HTTPStatusCode'] == 200)

def delete_ec2_security_group(ec2):
    """
    Delete a security group
    :param ec2: ec2 client instance
    :return: True if security group deleted successfully
    """

    # Check if group exists.
    group_name = config.get('SECURITY_GROUP','NAME')
    group = ec2.describe_security_groups(Filters=[{'Name': 'group-name', 'Values': [group_name]}])['SecurityGroups']

    if not group:
        print(f'Group {group_name} does not exist')
        return True

    for group_properties in group:
        group_id = group_properties['GroupId']

    try:
        response = ec2.delete_security_group(
            GroupId=group_id,
            GroupName=group_name,
            DryRun=False
        )
        print(f'Deleting security group response : {response}')
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise Exception(f'Bad Response! Response Code:{response["ResponseMetadata"]["HTTPStatusCode"]}')
    except Exception as e:
        print(f'Error occured while deleting group : {e}')
        return False

    print(f'Security group {group_name} deleted successful!')
    return (response['ResponseMetadata']['HTTPStatusCode'] == 200)

def create_cluster(redshift, iam_role_arn, vpc_security_group_id):
    """
    Create a Redshift cluster using the IAM role and security group created.
    :param redshift_client: a redshift client instance
    :param iam_role_arn: IAM role arn to give permission to cluster to communicate with other AWS service
    :param vpc_security_group_id: vpc group for network setting for cluster
    :return: True if cluster created successfully.
    """

    # Cluster Hardware config
    cluster_type = config.get('DWH', 'DWH_CLUSTER_TYPE')
    node_type = config.get('DWH', 'DWH_NODE_TYPE')
    num_nodes = int(config.get('DWH', 'DWH_NUM_NODES'))

    # Cluster identifiers and credentials
    cluster_identifier = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
    db_name = config.get('DWH', 'DWH_DB')
    database_port = int(config.get('DWH', 'DWH_PORT'))
    master_username = config.get('DWH', 'DWH_DB_USER')
    master_user_password = config.get('DWH', 'DWH_DB_PASSWORD')

    # Cluster adding IAM role
    iam_role = None

    # Security settings
    security_group = config.get('SECURITY_GROUP', 'NAME')

    try:
        response = redshift.create_cluster(
            DBName=db_name,
            ClusterIdentifier=cluster_identifier,
            ClusterType=cluster_type,
            NodeType=node_type,
            NumberOfNodes=num_nodes,
            MasterUsername=master_username,
            MasterUserPassword=master_user_password,
            VpcSecurityGroupIds=[vpc_security_group_id],
            IamRoles=[iam_role_arn]
        )
        print(f"Cluster creation response : {response}")
        print(f"Cluster creation response code : {response['ResponseMetadata']['HTTPStatusCode']} ")
    except Exception as e:
        print(f"Exception occured while creating cluster : {e}")
        return False

    return (response['ResponseMetadata']['HTTPStatusCode'] == 200)

def get_cluster_status(redshift):
    """
    Check Redshift cluster status
    :param redshift: a redshift client instance
    :param cluster_identifier: Cluster unique identifier
    :return: Cluster status
    """
    cluster_identifier = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
    response = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)
    cluster_status = response['Clusters'][0]['ClusterStatus']
    print(f'Cluster status : {cluster_status.upper()}')
    return cluster_status.upper()

def delete_cluster(redshift):
    """
    Deleting the redshift cluster
    :param redshift: a redshift client instance
    :return: True if cluster deleted successfully.
    """

    cluster_identifier = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
    if(len(redshift.describe_clusters()['Clusters']) == 0):
        print(f'Cluster {cluster_identifier} does not exist.')
        return True

    try:
        while(not get_cluster_status(redshift)) == 'ACTIVE':
            print("Can't delete cluster. Waiting for cluster to become ACTIVE")
            time.sleep(10)
        response = redshift.delete_cluster(
            ClusterIdentifier=cluster_identifier,
            SkipFinalClusterSnapshot=True
        )
        print(f'Cluster deleted with response : {response}')
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise Exception(f'Bad Response! Response Code:{response["ResponseMetadata"]["HTTPStatusCode"]}')
    except Exception as e:
        print(f"Exception occured while deleting cluster : {e}")
        return False

    clusters = redshift.describe_clusters()['Clusters']
    while cluster_identifier.lower() in [cluster['ClusterIdentifier'] for cluster in clusters]:
        for cluster in clusters:
            if cluster['ClusterIdentifier'] == cluster_identifier.lower():
                print(f'Cluster {cluster_identifier} in state: {cluster["ClusterStatus"]}')
                clusters = redshift.describe_clusters()['Clusters']
                time.sleep(5)

    print(f'Cluster {cluster_identifier} deleted successful!')
    return response['ResponseMetadata']['HTTPStatusCode']


class DB:
    """
    Database class. Class creates a DB connection which has two functions to benefit from

    Functions:
    1.) _get_db_sql
        - retrieves the output of your sql query in the form of a dataframe
    2.) _run_db_sql
        - executes your sql query without returning results.

    Paramaters:
        server - the server you want to connect to
        db - the database you want to connect to
    """

    def __init__(self,):
        keys = {
            'host': config.get('DWH', 'DWH_ENDPOINT'),
            'database': config.get('DWH', 'DWH_DB'),
            'username': config.get('DWH', 'DWH_DB_USER'),
            'password': config.get('DWH', 'DWH_DB_PASSWORD'),
            'port': config.get('DWH', 'DWH_PORT')
        }
        # Connection to db
        conn_string = f"postgresql://{keys['username']}:{keys['password']}@{keys['host']}:{keys['port']}/{keys['database']}"
        try:
            self.conn = psycopg2.connect(
                conn_string
            )
            self.conn.set_session(autocommit=True)
        except Exception as ex:
            raise ValueError(ex)

    def _get_db_sql(self, sql):
        """
        Get function to retrieve data in a dataframe
        """
        try:
            conn = self.conn
            with conn:
                df = pd.read_sql(sql, conn)
            return df
        except Exception as ex:
            print(ex)
            return

    def _run_db_sql(self, sql):
        """
        Run function to execute queries without returning results
        """
        stmts = sqlparse.split(sql)
        try:
            conn = self.conn
            with conn:
                cur = conn.cursor()
                try:
                    for stmt in stmts:
                        if stmt != '':
                            cur.execute(stmt)
                except Exception as ex_stmt:
                    print(ex_stmt)
                    print(stmt)
        except Exception as ex:
            print(ex)