from __future__ import print_function
import json
import re
import boto3
import botocore
import base64
import requests
from datetime import datetime
from operator import itemgetter

SOURCE_REGION = 'us-east-1'
TARGET_REGION = 'us-west-2'
RDS_EVENT_PATTERN = re.compile(ur'RDS-EVENT-(\d.*)')
ACCOUNT_ID_PATTERN = re.compile(ur'(arn:aws:.*::)([0-9]+)')
DB_INSTANCE_CLASS = '.micro'
RDS_DB_SUBNET_GROUP = 'backup-verification'
DB_CHECK_SNS_TOPIC_NAME = "rds-mysql-db-check"
JIRA_ISSUE_KEY = 'PROJ'
JIRA_URL = 'https://rest/api/latest/issue/'
JIRA_USER_NAME = ''
JIRA_USER_PASSWORD = ''
DATADOG_API_KEY = ''

print('Loading function ' + datetime.now().time().isoformat())

ec2 = boto3.client('ec2', region_name=TARGET_REGION)
rds = boto3.client('rds', region_name=TARGET_REGION)
rds_source = boto3.client('rds', region_name=SOURCE_REGION)
sns = boto3.client('sns', region_name=TARGET_REGION)

def publish_event_to_datadog(identifier, description, alert_type):
    # Send backup verification event to datadog
    print(description)
    data = json.dumps({
                      'title': 'RDS backup verification event',
                      'text': description,
                      'priority': 'normal',
                      'tags': ['environment:prod','name:' + identifier],
                      'alert_type': alert_type
                       })
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    try:
        response = requests.post('https://app.datadoghq.com/api/v1/events',
                                 params={'api_key': DATADOG_API_KEY},
                                 data=data,
                                 headers=headers
                                 )
        print(response.content)
    except requests.exceptions.RequestException as e:
        # Can not send data to DataDog.
        # A serious problem happened, like an SSLError or InvalidURL
        print('Exception during sending event to Datadog: {0}'.format(str(e)))
    if alert_type == 'error':
        publish_event_to_jira(description)

def publish_event_to_jira(description):
    # Create jira incident
    data = json.dumps({
                        'fields': {
                            'project': {'key': JIRA_ISSUE_KEY },
                            'summary': 'DB backup verification has error',
                            'description': ("\n".join(map(str, description))),
                            'issuetype': {'name': 'Incident'}
                        }
                     })
    base64string = base64.encodestring('%s:%s' % (JIRA_USER_NAME, JIRA_USER_PASSWORD)).replace('\n', '')
    jiraHeaders = {
        'content-type': 'application/json',
        'Authorization': 'Basic %s' % base64string
        }
    try:
        response = requests.post(JIRA_URL,
                                 data=data,
                                 headers=jiraHeaders
                                 )
        print(response.content)
    except requests.exceptions.RequestException as e:
        # Can not send data to JIRA.
        # A serious problem happened, like an SSLError or InvalidURL
        print('Exception during sending event to JIRA: {0}'.format(str(e)))

def get_account_id():
    account_id = None
    try:
        iam = boto3.client('iam')
        response = iam.get_user()
        arn = response['User']['Arn']
        print('Lambda IAM user arn: {0}'.format(arn))
        account_id = re.search(ACCOUNT_ID_PATTERN, arn).groups()[1]
        print('AWS account id: {0}'.format(account_id))
    except botocore.exceptions.ClientError as e:
        account_id = re.search(r'(arn:aws:sts::)([0-9]+)', str(e)).groups()[1]
    return account_id

def get_first_az():
    response = ec2.describe_availability_zones()
    return response['AvailabilityZones'][0]['ZoneName']

def restore_db_instance(az, snapshot_name):
    account_id = get_account_id()
    try:
    	response = rds.restore_db_instance_from_db_snapshot(
    	    DBInstanceIdentifier=snapshot_name,
    	    DBSnapshotIdentifier=snapshot_name,
    	    DBInstanceClass=DB_INSTANCE_CLASS,
    	    AvailabilityZone=az,
    	    DBSubnetGroupName=RDS_DB_SUBNET_GROUP,
    	    MultiAZ=False,
    	    PubliclyAccessible=False,
    	    Tags=[
    		{
    		    'Key': 'environment',
    		    'Value': 'ov'
    		},
    		{
    		    'Key': 'service_name',
    		    'Value': 'backup'
    		},
    		{
    		    'Key': 'service_tier',
    		    'Value': 'Database'
    		},
    		{
    		    'Key': 'Name',
    		    'Value': snapshot_name
    		},
    		{
    		    'Key': 'purpose',
    		    'Value': 'backup_verification'
    		},
    	    ],
    	    CopyTagsToSnapshot=True
    	)
    	print('Restore DB instance response: {0}'.format(response))
        publish_event_to_datadog(snapshot_name,
            'Start restore DB instance "{0}"'.format(snapshot_name),
            'info')
    except botocore.exceptions.ClientError as e:
        publish_event_to_datadog(snapshot_name,
            'The DB instance restore "{0}" failed. '
            'Accound Id: "{1}". '.format(snapshot_name, account_id) +
            'Message error: ' + str(e),
            'error')

def handle_db_snapshot_has_been_copied(snapshot_name):
    # Restore RDS DB Instance
    print("Handler: handle_db_snapshot_has_been_copied")
    publish_event_to_datadog(snapshot_name,
        'Snapshot "{0}" has been copied'.format(snapshot_name),
        'success')
    az = get_first_az()
    restore_db_instance(az, snapshot_name)

def gatekeeper_allow_automation_instance_only(snapshot_name):
    account_id = get_account_id()
    resource_name = "arn:aws:rds:{region}:{account_id}:db:{name}".format(
        region=TARGET_REGION,
        account_id=account_id,
        name=snapshot_name
    )
    respone = rds.list_tags_for_resource(ResourceName=resource_name)
    tag_list = respone['TagList']
    result = len(filter(lambda tag_item: tag_item['Key'] == 'purpose' and tag_item['Value'] == 'backup_verification', tag_list)) == 1
    return result

def handle_db_instance_has_been_restored(snapshot_name):
    # Resets Maser user password
    account_id = get_account_id()
    print('Handler: handle_db_instance_has_been_restored')
    if gatekeeper_allow_automation_instance_only(snapshot_name):
        waiter = rds.get_waiter('db_instance_available')
        waiter.wait(DBInstanceIdentifier=snapshot_name)
        publish_event_to_datadog(snapshot_name,
            'DB instance {0} has been restored.'.format(snapshot_name),
            'success')
        response = rds.describe_db_instances(DBInstanceIdentifier=snapshot_name)
        dbi_resource_id = response['DBInstances'][0]['DbiResourceId']
        try:
            response = rds.modify_db_instance(DBInstanceIdentifier=snapshot_name,
                BackupRetentionPeriod=0,
                MasterUserPassword=dbi_resource_id,
                ApplyImmediately=True)
            print('Modify Db instance response: {0}'.format(response))
            publish_event_to_datadog(snapshot_name,
                'Start modify Db instance "{0}"'.format(snapshot_name),
                'info')
        except botocore.exceptions.ClientError as e:
            publish_event_to_datadog(snapshot_name,
                'The DB instance modification "{0}" failed. '
                'Accound Id: "{1}". '.format(snapshot_name, account_id) +
                'Message error: ' + str(e),
                'error')
        waiter = rds.get_waiter('db_instance_available')
        waiter.wait(DBInstanceIdentifier=snapshot_name)

def copy_db_snapshot(source_snapshot_name, target_snapshot_name):
    # Copy snapshot to the another region
    account_id = get_account_id()
    source_snap_arn = 'arn:aws:rds:{0}:{1}:snapshot:{2}'.format(SOURCE_REGION, account_id, source_snapshot_name)
    try:
        response = rds.copy_db_snapshot(SourceDBSnapshotIdentifier=source_snap_arn,
            TargetDBSnapshotIdentifier=target_snapshot_name,
            CopyTags=True)
        print('Copy Db snapshot response: {0}'.format(response))
        publish_event_to_datadog(target_snapshot_name,
            'Start copy snapshot {0} => {1}'.format(source_snapshot_name, target_snapshot_name),
            'info')
    except botocore.exceptions.ClientError as e:
        publish_event_to_datadog(target_snapshot_name,
            'The copy of a cross region DB snapshot "{0}" copied to "{1}" failed. '
            'Accound Id: "{3}". '.format(source_snapshot_name, target_snapshot_name, account_id) +
            'Message error: ' + str(e),
            'error')

def handle_db_master_password_has_been_reseted(snapshot_name):
    # Initiate a database checks
    print('Handler: handle_db_master_password_has_been_reseted')
    waiter = rds.get_waiter('db_instance_available')
    waiter.wait(DBInstanceIdentifier=snapshot_name)
    publish_event_to_datadog(snapshot_name,
        'The Master user password for RDS DB instance "{0}" has been reseted'.format(snapshot_name),
        'success')
    account_id = get_account_id()
    sns_topic_arn = "arn:aws:sns:{region}:{account_id}:{topic_name}".format(
        region=TARGET_REGION,
        account_id=account_id,
        topic_name=DB_CHECK_SNS_TOPIC_NAME
    )
    message = json.dumps({'db_instance_identifier': snapshot_name})
    sns.publish(TopicArn=sns_topic_arn, Message=message)
    print('Sent notification message "{0}" to the DB check nofication topic: "{1}"'.format(message, sns_topic_arn))

def handler_backup_is_completed(db_instance_identifier):
    # Handles automatic and manual backups
    response = rds_source.describe_db_snapshots(DBInstanceIdentifier=db_instance_identifier)
    print('RDS Describe Snapshots response: {0}'.format(response))
    snapshots = response['DBSnapshots']
    if len(snapshots) > 0:
        snapshots = sorted(snapshots, key=itemgetter('SnapshotCreateTime'), reverse=True)
        latest_snapshot = snapshots[0]
        if latest_snapshot['Engine'] == 'mysql':
            source_snapshot_name = latest_snapshot['DBSnapshotIdentifier'].strip()
            print('Has found snapshot: {0}'.format(source_snapshot_name))
            if source_snapshot_name.startswith("rds:"):
                target_snapshot_name = source_snapshot_name[4:]
            else:
                target_snapshot_name = source_snapshot_name
            print('Target snapshot: "{0}"'.format(target_snapshot_name))
            copy_db_snapshot(source_snapshot_name, target_snapshot_name)
        else:
            raise Exception('Snapshots made for an unsupported engine: {0}'.format(latest_snapshot['Engine']))
    else:
        raise Exception('Snapshots were not found for the DB Instance: {0}'.format(db_instance_identifier))

def handler_start_delete_instance(db_instance_identifier):
    # Completed delete RDS DB Instance
    print('Handler: handler_start_delete_instance')
    waiter = rds.get_waiter('db_instance_deleted')
    waiter.wait(DBInstanceIdentifier=db_instance_identifier)
    publish_event_to_datadog(db_instance_identifier,
        'DB instance "{0}" was successfully deleted'.format(db_instance_identifier),
        'success')

def handle_recovery_db_instance_is_complete(db_instance_identifier):
    # Handles recovery RDS DB Instance
    # handler 0021-event(Recovery of the DB instance is complete), and checks,
    # the db password wasn't reset, yes or no, which will be the signal that the 0043-event(A DB instance is being
    # restored from a DB snapshot) was successfully done otherwise continue to perform the check functionality
    print('Handler: handle_recovery_db_instance_is_complete')
    response = rds.describe_events(
       Duration=180,
       SourceType='db-instance',
       SourceIdentifier=db_instance_identifier,
       EventCategories=['configuration change']
    )
    if gatekeeper_allow_automation_instance_only(db_instance_identifier):
        if re.search('Reset master credentials', str(response)):
            print('RDS-EVENT-0021 is ignored for "{0}" because password was reset'.format(db_instance_identifier))
        else:
            publish_event_to_datadog(db_instance_identifier,
                'DB instance "{0}" was successfully recovery'.format(db_instance_identifier),
                'success')
            handle_db_instance_has_been_restored(db_instance_identifier)

def handler_db_error(db_instance_identifier, event_id):
    print('Handler: handle_db_error')
    account_id = get_account_id()
    messages = {
        'RDS-EVENT-0061' : 'The copy of a cross region DB snapshot failed.',
        'RDS-EVENT-0031' : 'The DB instance has failed. We recommend that '
                           'you begin a point-in-time-restore for the DB instance.',
        'RDS-EVENT-0067' : 'An attempt to reset the master password for the DB instance has failed.'
     }
    publish_event_to_datadog(db_instance_identifier,
        'Got RDS error event: "{0}". ID Snaphost or DB instance: "{1}". '
        'Accound Id: "{2}". '
        'Message error: "{3}"'.format(event_id, db_instance_identifier, account_id, messages[event_id]),
        'error')

handlers = {
    'RDS-EVENT-0060' : handle_db_snapshot_has_been_copied,
    'RDS-EVENT-0043' : handle_db_instance_has_been_restored,
    'RDS-EVENT-0016' : handle_db_master_password_has_been_reseted,
    'RDS-EVENT-0002' : handler_backup_is_completed,
    'RDS-EVENT-0003' : handler_start_delete_instance,
    'RDS-EVENT-0021' : handle_recovery_db_instance_is_complete
}

handlers_err = {
    'RDS-EVENT-0061',
    'RDS-EVENT-0031',
    'RDS-EVENT-0067'
}

def lambda_handler(event2, context):
    for record in event['Records']:
        sns_message = json.loads(record['Sns']['Message'])
        raw_identifier_link = sns_message['Identifier Link']
        lines = raw_identifier_link.split('\n')
        snapshot_name = None
        if 'Source ID' in sns_message:
            snapshot_name = sns_message['Source ID']
        else:
            if len(lines) == 2:
                for line in lines:
                    if 'SourceId' in line:
                        snapshot_name = line[len('SourceId: '):].strip()
                        break
            else:
                raise Exception('Expected 2 lines in the "Identifier Link", but got {0}'.format(len(lines)))
        if not snapshot_name:
            Exception('Can not obtain snapshot name')
        raw_event_id = sns_message['Event ID'].strip()
        event_id = re.search(RDS_EVENT_PATTERN, raw_event_id).group()
        print("Got RDS event:{0} for the snapshot:{1}".format(event_id, snapshot_name))
        topic_arn = record['Sns']['TopicArn']
        if event_id in handlers.keys():
            print('Hanlde event {0}'.format(event_id))
            if event_id == 'RDS-EVENT-0002' and TARGET_REGION in topic_arn:
                print('{0} is ignored for target region: {1}'.format(event_id, TARGET_REGION))
                break
            if event_id == 'RDS-EVENT-0043' and SOURCE_REGION in topic_arn:
                print('{0} is ignored for source region: {1}'.format(event_id, TARGET_REGION))
                break
            if event_id == 'RDS-EVENT-0021' and SOURCE_REGION in topic_arn:
                print('{0} is ignored for source region: {1}'.format(event_id, TARGET_REGION))
                break
            handlers[event_id](snapshot_name)
        elif event_id in handlers_err:
            print('Hanlde_err event {0}'.format(event_id))
            handler_db_error(snapshot_name, event_id)
        else:
            print("Event {0} is ignored".format(event_id2))
