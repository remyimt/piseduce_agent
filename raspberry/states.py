# The index of the last state executed from the NFS filesystem
# After this state, we use the environment property 'ssh_user' to define the user account to use in SSH connections
# See raspberry.exec.ssh_test()
SSH_IDX = 12

# Add the environment names to the 'environments' array to limit the process to specific environments
PROCESS = {
    'deploy': [
        {
            'environments': [],
            'states': [
                'boot_conf', 'turn_off', 'turn_on', 'ssh_test', 'env_copy', 'env_check', 
                'delete_partition', 'create_partition', 'mount_partition', 'resize_partition',
                'wait_resizing', 'system_conf', 'boot_files', 'ssh_test', 'system_update',
                'boot_update', 'user_conf', 'deployed'
            ]
        }
    ],
    'destroy': [
        {
            'environments': [],
            'states': [
                'destroying', 'turn_off', 'destroyed'
            ]
        }
    ],
    'reboot': [
        {
            'environments': [],
            'states': [
                'turn_off', 'turn_on', 'rebooted'
            ]
        }
    ],
    'boot_test': [
        {
            'environments': [],
            'states': [
                'boot_conf', 'turn_off', 'turn_on', 'ssh_test', 'booted'
            ]
        }
    ],
    'reg_env': [
        {
            'environments': [],
            'states': [
                'uncompress', 'read_info', 'img_upload', 'update_db', 'deployed'
            ]
        }
    ]
}


# State names must NOT include '_exec' or '_post'
# 'lost' timeouts must be greater then 'before_reboot' timeouts
# 0: infinite timeouts
# The states must be ordered according to the process values
STATE_DESC = {
    'boot_conf': { 'exec': True, 'post': False, 'before_reboot': 0, 'lost': 5 },
    'turn_off': { 'exec': True, 'post': False, 'before_reboot': 0, 'lost': 5 },
    'turn_on': { 'exec': True, 'post': True, 'before_reboot': 60, 'lost': 90 },
    # First boot of tinycore system can be very long
    'ssh_test': { 'exec': False, 'post': True, 'before_reboot': 300, 'lost': 330 },
    'env_copy': { 'exec': True, 'post': True, 'before_reboot': 0, 'lost': 30 },
    'env_check': { 'exec': True, 'post': False, 'before_reboot': 0, 'lost': 400 },
    'delete_partition': { 'exec': True, 'post': False, 'before_reboot': 0, 'lost': 5 },
    'create_partition': { 'exec': True, 'post': False, 'before_reboot': 0, 'lost': 5 },
    'mount_partition': { 'exec': True, 'post': True, 'before_reboot': 0, 'lost': 5 },
    'resize_partition': { 'exec': True, 'post': True, 'before_reboot': 0, 'lost': 5 },
    'wait_resizing': { 'exec': True, 'post': False, 'before_reboot': 0, 'lost': 90 },
    'system_conf': { 'exec': True, 'post': False, 'before_reboot': 0, 'lost': 5 },
    'boot_files': { 'exec': True, 'post': False, 'before_reboot': 20, 'lost': 5 },
    'system_update': { 'exec': True, 'post': True, 'before_reboot': 0, 'lost': 0 },
    'boot_update': { 'exec': True, 'post': False, 'before_reboot': 0, 'lost': 5 },
    'user_conf': { 'exec': True, 'post': False, 'before_reboot': 0, 'lost': 1 },
    # Final state: exec is False, post is False and the 2 timeouts are infinite (0)
    'deployed': { 'exec': False, 'post': False, 'before_reboot': 0, 'lost': 0 },

    # Final state: exec is False, post is False and the 2 timeouts are infinite (0)
    'rebooted': { 'exec': False, 'post': False, 'before_reboot': 0, 'lost': 0 },

    'destroying': { 'exec': True, 'post': False, 'before_reboot': 0, 'lost': 30 },
    # Final state: exec is False, post is False and the 2 timeouts are infinite (0)
    'destroyed': { 'exec': False, 'post': False, 'before_reboot': 0, 'lost': 0 },

    # Final state: exec is False, post is False and the 2 timeouts are infinite (0)
    'booted': { 'exec': False, 'post': False, 'before_reboot': 0, 'lost': 0 },

    'uncompress': { 'exec': True, 'post': True, 'before_reboot': 0, 'lost': 0 },
    'read_info': { 'exec': True, 'post': False, 'before_reboot': 0, 'lost': 0 },
    'img_upload': { 'exec': True, 'post': True, 'before_reboot': 0, 'lost': 0 },
    'update_db': { 'exec': True, 'post': False, 'before_reboot': 0, 'lost': 0 }
}
