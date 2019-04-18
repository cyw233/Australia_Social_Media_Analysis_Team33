import boto
from boto.ec2.regioninfo import RegionInfo

region = RegionInfo(name='melbourne', endpoint='nova.rc.nectar.org.au')
ec2_conn = boto.connect_ec2(aws_access_key_id='56ee4481633b492484c4e8aa5c9b492c',
                            aws_secret_access_key='126094b8a1c6486daf568208e0a3143c',
                            is_secure=True,
                            region=region,
                            port=8773,
                            path='/services/Cloud',
                            validate_certs=False)
print('connection established')
