# Add the environment names to the 'environments' array to limit the process to specific environments
PROCESS = {
    'deploy': [
        {
            'environments': [],
            'states': [
                'deploy1', 'deploy2', 'deploy3', 'deployed'
            ]
        }
    ],
    'destroy': [
        {
            'environments': [],
            'states': [
                'destroy1', 'destroy2', 'destroyed'
            ]
        }
    ],
    'custom': [
        {
            'environments': [],
            'states': [
                'custom1', 'custom2', 'customed'
            ]
        }
    ],
    'reboot': [
        {
            'environments': [],
            'states': [
                'reboot1', 'reboot2', 'rebooted'
            ]
        }
    ]
}


# State names must NOT include '_exec' or '_post'
# 'lost' timeouts must be greater then 'before_reboot' timeouts
# 0: infinite timeouts
# The states must be ordered according to the process values
STATE_DESC = {
    'deploy1': { 'exec': True, 'post': False, 'before_reboot': 0, 'lost': 12 },
    'deploy2': { 'exec': True, 'post': True, 'before_reboot': 0, 'lost': 12 },
    'deploy3': { 'exec': True, 'post': False, 'before_reboot': 8, 'lost': 12 },
    # Final state: exec is False, post is False and the 2 timeouts are infinite (0)
    'deployed': { 'exec': False, 'post': False, 'before_reboot': 0, 'lost': 0 },

    'destroy1': { 'exec': True, 'post': False, 'before_reboot': 0, 'lost': 12 },
    'destroy2': { 'exec': True, 'post': True, 'before_reboot': 0, 'lost': 12 },
    # Final state: exec is False, post is False and the 2 timeouts are infinite (0)
    'destroyed': { 'exec': False, 'post': False, 'before_reboot': 0, 'lost': 0 },

    'custom1': { 'exec': True, 'post': False, 'before_reboot': 0, 'lost': 12 },
    'custom2': { 'exec': True, 'post': True, 'before_reboot': 0, 'lost': 12 },
    # Final state: exec is False, post is False and the 2 timeouts are infinite (0)
    'customed': { 'exec': False, 'post': False, 'before_reboot': 0, 'lost': 0 },

    'reboot1': { 'exec': True, 'post': False, 'before_reboot': 0, 'lost': 12 },
    'reboot2': { 'exec': True, 'post': True, 'before_reboot': 0, 'lost': 12 },
    # Final state: exec is False, post is False and the 2 timeouts are infinite (0)
    'rebooted': { 'exec': False, 'post': False, 'before_reboot': 0, 'lost': 0 }
}
