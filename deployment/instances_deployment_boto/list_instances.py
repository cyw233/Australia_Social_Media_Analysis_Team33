from nectar import ec2_conn

# list all available instances
reservations = ec2_conn.get_all_reservations()
print('\nID: {}\tIP: {}\tPlacement: {}'.format(reservations[0].id,
                            reservations[0].instances[0].private_ip_address,
                            reservations[0].instances[0].placement))