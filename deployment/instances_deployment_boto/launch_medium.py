from nectar import ec2_conn
import time

reservation = ec2_conn.run_instances('ami-00003ad8',
                                     placement='melbourne-qh2',
                                     key_name='cluster1',
                                     instance_type='m1.medium',
                                     security_groups=['default', 'ssh', 'couchdb'])
instance = reservation.instances[0]
print('New instance {} has been created'.format(instance.id))


# create a volume
vol_req = ec2_conn.create_volume(50, 'melbourne-qh2')
print('New volume {} has been created'.format(vol_req.id))


# check the state of the volume
curr_vols = ec2_conn.get_all_volumes([vol_req.id])
print('Volume status: {}, volume AZ: {}'.format(curr_vols[0].status,
      curr_vols[0].zone))


# wait until the instance is running before attaching volume to it
while instance.state != 'running':
    print('Instance {} is {}'.format(instance.id, instance.state))
    time.sleep(5)
    instance.update()


# attach the new volume to the new instance
ec2_conn.attach_volume(vol_req.id, instance.id, '/dev/vdc')
print('Volume {} has been attached to {} at /dev/vdc'.format(vol_req.id, instance.id))


# create a snapshot.
print('Creating snapshot...')
snapshot = ec2_conn.create_snapshot(vol_req.id, 'Snapshot for '+str(vol_req.id))
time.sleep(10)
print('Snapshot successfully created!')
