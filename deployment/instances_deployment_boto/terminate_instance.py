from nectar import ec2_conn

# put the ID of the instance you want to delete in 'INSTANCE_ID' and run
# the program to terminate the instance
INSTANCE_ID = ''
ec2_conn.terminate_instances(instance_ids=[INSTANCE_ID])
print('Instance {} has been terminated'.format(INSTANCE_ID))