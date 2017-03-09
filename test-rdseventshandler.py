#!/usr/bin/env python

import boto3
import boto
import datetime
import unittest
import json
import lambda_function
from mock import Mock, patch
from moto import mock_sns


# sample event
describe_db_snapshots = {
   u'DBSnapshots':[
      {
         u'Engine':'mysql',
         u'SnapshotCreateTime':datetime.datetime(2017, 2, 14, 17, 42, 3, 79000),
         u'AvailabilityZone':'us-east-1e',
         u'DBSnapshotArn':'arn:aws:rds:us-east-1:123456789012:snapshot:test-snapshot-delete-me',
         u'PercentProgress':100,
         u'Encrypted':False,
         u'LicenseModel':'general-public-license',
         u'StorageType':'gp2',
         u'Status':'available',
         u'DBSnapshotIdentifier':'backup-testing-2017-02-14-07',
         u'InstanceCreateTime':datetime.datetime(2016, 4, 27, 17, 30, 18, 565000),
         u'OptionGroupName':'default:mysql-5-6',
         u'AllocatedStorage':5,
         u'EngineVersion':'5.6.19b',
         u'SnapshotType':'manual',
         u'Port':3306,
         u'DBInstanceIdentifier':'backup-testing'
      }
   ]
}

copy_db_snapshot = {
   u'DBSnapshot':{
        u'Engine':'mysql',
        u'Status':'pending',
        u'SourceRegion':'us-east-1',
        u'MasterUsername':'hipchat',
        u'Encrypted':False,
        u'LicenseModel':'general-public-license',
        u'StorageType':'gp2',
        u'PercentProgress':0,
        u'SourceDBSnapshotIdentifier':'arn:aws:rds:us-east-1:123456789012:snapshot:test-snapshot-delete-me',
        u'DBSnapshotIdentifier':'test-snapshot-delete-me',
        u'InstanceCreateTime':datetime.datetime(2016, 4, 27, 17, 30, 18, 565000),
        u'AllocatedStorage':5,
        u'EngineVersion':'5.6.19b',
        u'SnapshotType':'manual',
        u'DBSnapshotArn':'arn:aws:rds:us-west-2:123456789012:snapshot:test-snapshot-delete-me',
        u'Port':3306,
        u'DBInstanceIdentifier':'backup-testing'
    }
}

modify_db_instance = {
   u'DBInstance':{
      u'PubliclyAccessible':False,
      u'MasterUsername':'XXXXXXXX',
      u'MonitoringInterval':0,
      u'LicenseModel':'general-public-license',
      u'VpcSecurityGroups':[
         {
            u'Status':'active',
            u'VpcSecurityGroupId':'sg-XXXXXXXX'
         }
      ],
      u'InstanceCreateTime':datetime.datetime(2017, 2, 15, 14, 13, 18, 219000),
      u'CopyTagsToSnapshot':True,
      u'OptionGroupMemberships':[
         {
            u'Status':'in-sync',
            u'OptionGroupName':'default:mysql-5-6'
         }
      ],
      u'PendingModifiedValues':{
         u'MasterUserPassword':'****'
      },
      u'Engine':'mysql',
      u'MultiAZ':False,
      u'DBSecurityGroups':[],
      u'DBParameterGroups':[
         {
            u'DBParameterGroupName':'default.mysql5.6',
            u'ParameterApplyStatus':'in-sync'
         }
      ],
      u'AutoMinorVersionUpgrade':False,
      u'PreferredBackupWindow':'19:20-19:50',
      u'DBSubnetGroup':{
         u'Subnets':[
            {
               u'SubnetStatus':'Active',
               u'SubnetIdentifier':'subnet-XXXXXXXX',
               u'SubnetAvailabilityZone':{
                  u'Name':'us-west-2b'
               }
            },
            {
               u'SubnetStatus':'Active',
               u'SubnetIdentifier':'subnet-XXXXXXXX',
               u'SubnetAvailabilityZone':{
                  u'Name':'us-west-2a'
               }
            }
         ],
         u'DBSubnetGroupName':'backup-verification',
         u'VpcId':'vpc-XXXXXXXX',
         u'DBSubnetGroupDescription':'The group for deployment of the DB instances for backup verification',
         u'SubnetGroupStatus':'Complete'
      },
      u'ReadReplicaDBInstanceIdentifiers':[],
      u'AllocatedStorage':5,
      u'DBInstanceArn':'arn:aws:rds:us-west-2:123456789012:db:test-snapshot-delete-me',
      u'BackupRetentionPeriod':0,
      u'DBName':'hipchat',
      u'PreferredMaintenanceWindow':'sun:23:00-mon:00:00',
      u'Endpoint':{
         u'HostedZoneId':'XXXXXXXXXXXXXXX',
         u'Port':3306,
         u'Address':'test-snapshot-delete-me.XXXXXXXX.us-west-2.rds.amazonaws.com'
      },
      u'DBInstanceStatus':'available',
      u'EngineVersion':'5.6.19b',
      u'AvailabilityZone':'us-west-2a',
      u'DomainMemberships':[],
      u'StorageType':'gp2',
      u'DbiResourceId':'db-XXXXXXXX',
      u'CACertificateIdentifier':'rds-ca-2015',
      u'StorageEncrypted':False,
      u'DBInstanceClass':'db.t2.micro',
      u'DbInstancePort':0,
      u'DBInstanceIdentifier':'test-snapshot-delete-me'
   }
}

describe_db_instances = {
   u'DBInstances':[
      {
         u'SnapshotCreateTime':datetime.datetime(2017, 2, 14, 17, 42, 3, 79000),
         u'Engine':'mysql',
         u'AvailabilityZone':'us-east-1e',
         u'DbiResourceId':'XXXXXXXX-1d',
         u'Endpoint': {
		    "Port": 3306, "Address": "test-snapshot-delete-me.XXXXXXXX.us-east-1.rds.amazonaws.com"},
         u'PercentProgress':100,
         u'MasterUsername':'XXXXXXXX',
         u'Encrypted':False,
         u'LicenseModel':'general-public-license',
         u'StorageType':'gp2',
         u'Status':'available',
         u'VpcId':'vpc-XXXXXXXX',
         u'InstanceCreateTime':datetime.datetime(2016, 4, 27, 17, 30, 18, 565000),
         u'OptionGroupName':'default:mysql-5-6',
         u'AllocatedStorage':5,
         u'EngineVersion':'5.6.19b',
         u'SnapshotType':'manual',
         u'Port':3306,
         u'DBInstanceIdentifier':'test-snapshot-delete-me'
      }
   ]
}

describe_events = {
    'Reset master credentials'
}


class TestRDSBackUpVerification(unittest.TestCase):

    def setUp(self):
        # setUp function called before each test method.
        conn_constructor = patch("lambda_function.boto3.client").start()
        get_account = patch("lambda_function.get_account_id").start()
        get_first_az = patch("lambda_function.get_first_az").start()
        get_waiter = patch("lambda_function.rds.get_waiter").start()
        gatekeeper_allow_automation_instance_only = patch(
              "lambda_function.gatekeeper_allow_automation_instance_only").start()
        patch('lambda_function.requests.post').start()
        self.rds_source = conn_constructor.return_value

    def tearDown(self):
        # to destroy patch of a structure
        patch.stopall()

    def side_describe_db_snapshots(self, DBInstanceIdentifier):
        # Helper function to return response describe_db_snapshot.
        return describe_db_snapshots

    def side_snapshot_copy(self,
                           SourceDBSnapshotIdentifier,
                           TargetDBSnapshotIdentifier,
                           CopyTags):
        # Helper function to return response copy_db_snapshot.
        return copy_db_snapshot

    def side_modify_db_instance(self,
                                DBInstanceIdentifier,
                                BackupRetentionPeriod,
                                MasterUserPassword,
                                ApplyImmediately):
        # Helper function to return response modify_db_instance.
        return modify_db_instance

    def side_describe_db_instances(self, DBInstanceIdentifier):
        # Helper function to return response describe_db_instances.
        return describe_db_instances

    def side_describe_events(self,
                             Duration,
                             SourceType,
                             SourceIdentifier,
                             EventCategories):
        # Helper function to return response describe_events.
        return describe_events

    def side_restore_db_instance_from_db_snapshot(self,
                                                  DBInstanceIdentifier,
                                                  DBSnapshotIdentifier,
                                                  DBInstanceClass,
                                                  AvailabilityZone,
                                                  DBSubnetGroupName,
                                                  MultiAZ,
                                                  PubliclyAccessible,
                                                  Tags,
                                                  CopyTagsToSnapshot):
        # Helper function to return response describe_db_instances.
        return describe_db_instances

    def side_events(self, Event_ID):
        # Helper function to return json data from files in the events dir.
        with open('events/' + Event_ID + '.json') as f:
            data = json.load(f)
        return data

    @mock_sns
    def test_backup_rds_verification(self):
        # Replace response of describe_db_snapshots api call
        lambda_function.rds_source.describe_db_snapshots = self.side_describe_db_snapshots
        # Replace response of describe_db_instances api call
        lambda_function.rds.describe_db_instances = self.side_describe_db_instances
        # Replace response of restore_db_instance_from_db_snapshot api call
        lambda_function.rds.restore_db_instance_from_db_snapshot = self.side_restore_db_instance_from_db_snapshot
        # Replace response of describe_events api call
        lambda_function.rds.describe_events = self.side_describe_events
        # Replace response of copy_db_snapshot api call
        lambda_function.rds.copy_db_snapshot = self.side_snapshot_copy
        # Replace response of modify_db_instance api call
        lambda_function.rds.modify_db_instance = self.side_modify_db_instance
        # Replace return value of get_account_id function
        lambda_function.get_account_id.return_value = "123456789012"
        # Replace return value of get_first_az function
        lambda_function.get_first_az.return_value = "us-west-2"
        # Replace response publish event data to Datadog and Jira
        lambda_function.requests.post.return_value.content = "==== Publish event ===="
        # Create sns topic
        sns = boto.sns.connect_to_region("us-west-2")
        sns.create_topic("rds-mysql-db-check")
        # Replace return value of gatekeeper_allow_automation_instance_only function
        lambda_function.gatekeeper_allow_automation_instance_only.return_value = True
        print('==== Start testing with an event id {0} ====').format(self.EVENT_ID)
        lambda_function.lambda_handler(self.side_events(self.EVENT_ID), '')


class TestRDSBackUpVerification1(TestRDSBackUpVerification):
    # A DB instance is being restored from a DB snapshot.
    EVENT_ID = 'RDS-EVENT-0043'


class TestRDSBackUpVerification2(TestRDSBackUpVerification):
    # The master password for the DB instance has been reset.
    EVENT_ID = 'RDS-EVENT-0016'


class TestRDSBackUpVerification3(TestRDSBackUpVerification):
    # Recovery of the DB instance is complete.
    EVENT_ID = 'RDS-EVENT-0021'


class TestRDSBackUpVerification4(TestRDSBackUpVerification):
    # The DB instance is being deleted.
    EVENT_ID = 'RDS-EVENT-0003'


class TestRDSBackUpVerification5(TestRDSBackUpVerification):
    # The copy of a cross region DB snapshot failed.
    EVENT_ID = 'RDS-EVENT-0060'


class TestRDSBackUpVerification6(TestRDSBackUpVerification):
    # The copy of a cross region DB snapshot failed.
    EVENT_ID = 'RDS-EVENT-0061'


class TestRDSBackUpVerification7(TestRDSBackUpVerification):
    # The DB instance has failed. We recommend that you begin
    # a point-in-time-restore for the DB instance.
    EVENT_ID = 'RDS-EVENT-0031'


class TestRDSBackUpVerification8(TestRDSBackUpVerification):
    # An attempt to reset the master password for the DB instance has failed.
    EVENT_ID = 'RDS-EVENT-0067'


if __name__ == '__main__':
    # A backup of the DB instance is complete.
    TestRDSBackUpVerification.EVENT_ID = 'RDS-EVENT-0002'
    unittest.main()
