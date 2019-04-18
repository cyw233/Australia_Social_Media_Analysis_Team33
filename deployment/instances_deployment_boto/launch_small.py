from nectar import ec2_conn
import time

INSTANCES_NUM = 3


# create instances
def create_instances(num=1):
    reservation = ec2_conn.run_instances('ami-00003ad8',
                                         placement='melbourne-qh2',
                                         key_name='cluster1',
                                         min_count=1,
                                         max_count=num,
                                         instance_type='m1.small',
                                         security_groups=['default', 'ssh', 'couchdb'])
    print('New instances have been created')
    return reservation


# create volumes
def create_volumes(num):
    volumes = []
    for _ in range(num):
        vol_req = ec2_conn.create_volume(50, 'melbourne-qh2')

        curr_vols = ec2_conn.get_all_volumes([vol_req.id])
        print('Volume status: {}, volume AZ: {}'.format(curr_vols[0].status,
                                                        curr_vols[0].zone))

        volumes.append(vol_req)

    print('All new volumes have been created')
    return volumes


def wait_instances_running(reservation):
    for instance in reservation:
        while instance.state != 'running':
            print('Instance {} is {}'.format(instance.id, instance.state))
            time.sleep(5)
            instance.update()

    print('All instances are running now!')


def attach_volumes(reservation, volumes):
    for i in range(len(reservation.instances)):
        ec2_conn.attach_volume(volumes[i].id, reservation.instances[i].id, '/dev/vdc')
        print('Volume {} has been attached to {} at /dev/vdc'.format(volumes[i].id, reservation.instances[i].id))

    print("All volumes have been successfully attached")


reserv = create_instances(INSTANCES_NUM)
vols = create_volumes(INSTANCES_NUM)
wait_instances_running(reserv)
attach_volumes(reserv, vols)
# list all available instances
for inst in reserv:
    print('\nID: {}\tIP: {}\tPlacement: {}'.format(inst.id,
                                                   inst.private_ip_address,
                                                   inst.placement))
