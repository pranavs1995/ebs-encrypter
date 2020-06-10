import boto3
from configparser import ConfigParser
from botocore.exceptions import ClientError, EndpointConnectionError
import logging

logging_level=10 #DEBUG

logger = logging.getLogger()
logger.setLevel(int(logging_level))

MAX_RETRIES = 360
DELAY_RETRY = 5

class EBSEncryptor:

    def __init__(self, instance_id, config):
        self._aws_access_key_id = config['default']['aws_access_key_id']
        self._aws_secret_access_key = config['default']['aws_secret_access_key']
        self._region = config['default']['region']
        self._instance_id = config['default']['instance_id']
        self._kms_key = config['default']['kms_key']
        self._discard_source=config['default']['discard_source']

        self._tags = [
                {
                    'Key': 'created_by',
                    'Value': 'ebs_encryptor'
                }
            ]

        self._ec2_client = boto3.client('ec2',region_name=self._region,aws_access_key_id=self._aws_access_key_id,aws_secret_access_key=self._aws_secret_access_key)
        self._ec2_resource = boto3.resource('ec2',region_name=self._region,aws_access_key_id=self._aws_access_key_id,aws_secret_access_key=self._aws_secret_access_key)

        self._instance = self._ec2_resource.Instance(id=self._instance_id)

        self._volume = None
        self._snapshot = None

        self._wait_snapshot = self._ec2_client.get_waiter('snapshot_completed')
        self._wait_volume = self._ec2_client.get_waiter('volume_available')

        self._wait_snapshot.config.max_attempts = MAX_RETRIES
        self._wait_volume.config.max_attempts = MAX_RETRIES
        self._wait_snapshot.config.delay = DELAY_RETRY
        self._wait_volume.config.delay = DELAY_RETRY

    def _is_instance_exists(self):
        try:
            response = self._ec2_client.describe_instances(InstanceIds=[self._instance.id])
            return response
        except ClientError:
            raise

    def _is_instance_running(self):
        pass

    # take snapshot of volume
    def _take_snapshot(self, volume):
        print("--STARTING CREATE SNAPSHOT STAGE--")
        snapshot = volume.create_snapshot(
                TagSpecifications = [
                    {
                        'ResourceType': 'snapshot',
                        'Tags': self._tags
                    }
                ]
            )
        self._wait_snapshot.wait(SnapshotIds=[snapshot.id])
        return snapshot

    # create encrypted volume from snapshot
    def _create_volume(self, snapshot, original_volume):
        print("--STARTING CREATE VOLUME STAGE--")
        vol_args = {
                'SnapshotId': snapshot.id,
                'VolumeType': original_volume.volume_type,
                'AvailabilityZone': original_volume.availability_zone,
                'Encrypted': True,
                'KmsKeyId': self._kms_key
            }
        
        if original_volume.volume_type.startswith('io'):
            print(f'-- Provisioned IOPS volume detected (with 'f'{original_volume.iops} IOPS)')
            vol_args['Iops'] = original_volume.iops
        print("Creating encrypted volume from {}".format(snapshot.id))

        volume = self._ec2_resource.create_volume(**vol_args)
        self._wait_volume.wait(VolumeIds=[volume.id])

        volume.create_tags(Tags=self._tags)
        return volume

    def _swap_volumes(self, old_volume, new_volume):
        print('--SWAP THE OLD VOLUME AND THE NEW ONE--')
        device = old_volume.attachments[0]['Device']
        self._instance.detach_volume(Device=device, VolumeId=old_volume.id)
        self._wait_volume.wait(VolumeIds=[old_volume.id])
        self._instance.attach_volume(Device=device, VolumeId=new_volume.id)

    def _cleanup(self, old_volume, discard_source):
        pass

    def _start_encryption(self):
        print("Starting Encryption for instance {} volumes".format(self._instance.id))
        for device in self._instance.block_device_mappings:
            if 'Ebs' not in device:
                print('{}: SKIP Volume {} is not EBS volume'.format(self._instance_id, device['VolumeId']))
                continue
        
        for volume in self._instance.volumes.all():
            if volume.encrypted:
                print('Volume {} already encrypted in instance {}'.format(volume.volume_id,self._instance_id))
                continue
        
            self._snapshot = self._take_snapshot(volume)

            self._volume = self._create_volume(self._snapshot, volume)

            self._swap_volumes(old_volume=volume, new_volume=self._volume)

            self._cleanup(volume,discard_source=True)
            # add delete on termination feature here


def main():
    config = ConfigParser()
    config.read('config_file.ini')

    for instance_id in config['default']['instance_id'].split(','):
        ebs_encryptor_obj = EBSEncryptor(instance_id,config)
        ebs_encryptor_obj._start_encryption()

main()