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
                'launching', 'running', 'finishing', 'terminated', 'stopped'
            ]
        }
    ],
    'destroy': [
        {
            'environments': [],
            'states': [
                'destroying', 'destroyed'
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
    'save_env': [
        {
            'environments': [],
            'states': [
                'img_part', 'img_format', 'img_copy', 'img_copy_check', 'img_customize',
                'img_compress', 'img_compress_check', 'upload', 'upload_check', 'deployed'
            ]
        }
    ]
}


# State names must NOT include '_exec' or '_post'
# 'lost' timeouts must be greater then 'before_reboot' timeouts
# 0: infinite timeouts
# The states must be ordered according to the process values
STATE_DESC = {
    'wait_running': { 'exec': False, 'post': True, 'before_reboot': 0, 'lost': 0 },
    'deploy': { 'exec': True, 'post': False, 'before_reboot': 0, 'lost': 0 },
    'wait_deploying': { 'exec': False, 'post': True, 'before_reboot': 0, 'lost': 0 },
    # Final state: exec is False, post is False and the 2 timeouts are infinite (0)
    'deployed': { 'exec': False, 'post': False, 'before_reboot': 0, 'lost': 0 },

    # Final state: exec is False, post is False and the 2 timeouts are infinite (0)
    'rebooted': { 'exec': False, 'post': False, 'before_reboot': 0, 'lost': 0 },

    'destroying': { 'exec': True, 'post': False, 'before_reboot': 0, 'lost': 30 },
    # Final state: exec is False, post is False and the 2 timeouts are infinite (0)
    'destroyed': { 'exec': False, 'post': False, 'before_reboot': 0, 'lost': 0 },

    # Final state: exec is False, post is False and the 2 timeouts are infinite (0)
    'booted': { 'exec': False, 'post': False, 'before_reboot': 0, 'lost': 0 },

    'img_part': { 'exec': True, 'post': False, 'before_reboot': 0, 'lost': 0 },
    'img_format': { 'exec': True, 'post': False, 'before_reboot': 0, 'lost': 0 },
    'img_copy': { 'exec': True, 'post': False, 'before_reboot': 0, 'lost': 0 },
    'img_copy_check': { 'exec': True, 'post': False, 'before_reboot': 0, 'lost': 0 },
    'img_customize': { 'exec': True, 'post': False, 'before_reboot': 0, 'lost': 0 },
    'img_compress': { 'exec': True, 'post': False, 'before_reboot': 0, 'lost': 0 },
    'img_compress_check': { 'exec': True, 'post': False, 'before_reboot': 0, 'lost': 0 },
    'upload': { 'exec': True, 'post': False, 'before_reboot': 0, 'lost': 0 },
    'upload_check': { 'exec': True, 'post': False, 'before_reboot': 0, 'lost': 0 }
}
