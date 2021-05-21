# Load the configuration file
import sys
from lib.config_loader import load_config, get_config

if len(sys.argv) != 2:
    print("The configuration file is required in parameter.")
    print("For example, 'python3 %s config.json'" % sys.argv[0])
    sys.exit(2)
load_config(sys.argv[1])

from database.connector import open_session, close_session
from database.tables import Schedule
import json, requests, traceback

# The API port (see files/config_agent.json)
api_port = 1234
# The API token (see files/config_agent.json)
api_token = "123456789"

url = []
error_url = []
idx_list = []

def test_last_url(post_args, expected_json):
    test_ok = False
    test_nb = len(url) - 1
    if len(idx_list) == 0 or test_nb in idx_list:
        print("______________________________________________________")
        print("%d: Testing %s" % (test_nb, url[test_nb]))
        try:
            if len(post_args) == 0:
                r = requests.get(url = "http://localhost:%d/v1/%s" % (api_port, url[test_nb]),
                    timeout = 6)
            else:
                r = requests.post(url = "http://localhost:%d/v1/%s" % (api_port, url[test_nb]),
                    timeout = 6, json = post_args)
            if r.status_code == 200:
                r_json = r.json()
                print(json.dumps(r_json, indent = 4))
                first = True
                for expected in expected_json:
                    colon_idx = expected.index(":")
                    key_path = expected[:colon_idx].strip()
                    expected_value = expected[colon_idx + 1:].strip()
                    value = r_json
                    if "." in key_path:
                        for key in key_path.split("."):
                            value = value.get(key)
                    elif len(key_path) > 0:
                        value = r_json[key_path]
                    length = None
                    if expected_value[0] == "/":
                        length = int(expected_value[1:])
                    if first:
                        first = False
                        if length is None:
                            print("%s <> %s" % (value, expected_value))
                            test_ok = value == expected_value
                        else:
                            print("%d <> %d" % (len(value), length))
                            test_ok = len(value) == length
                    else:
                        if length is None:
                            print("%s <> %s" % (value, expected_value))
                            test_ok &= value == expected_value
                        else:
                            print("%d <> %d" % (len(value), length))
                            test_ok &= len(value) == length
            else:
                print("Wrong status code: %d" % r.status_code)
            if not test_ok:
                error_url.append("%d: %s" % (test_nb, url[test_nb]))
        except:
            traceback.print_exc()
            print("Something bad happened...")
            error_url.append("%d: %s" % (test_nb, url[test_nb]))


print("######################################################")
### Read properties from the database
# Test
url.append("debug/state")
try:
    r_json = test_last_url(
            {},
            [ "state: running", "type: raspberry" ])
except:
    print("Something bad happened again...")
    traceback.print_exc()
    error_url.append("%d: %s" % (len(url) - 1, url[-1]))

# Test
url.append("debug/auth")
try:
    r_json = test_last_url(
        { "token": api_token },
        [ "auth: success" ])
except:
    print("Something bad happened again...")
    error_url.append("%d: %s" % (len(url) - 1, url[-1]))

# Test (10 lines)
url.append("user/switch/list")
try:
    r_json = test_last_url(
        { "token": api_token },
        [ ": /2", "24port_RPI3: /7", "24port_RPI4.master_port: 8" ])
except:
    print("Something bad happened again...")
    error_url.append("%d: %s" % (len(url) - 1, url[-1]))

# Test (10 lines)
url.append("user/environment/list")
try:
    r_json = test_last_url(
        { "token": api_token },
        [ ": /7", "raspbian_buster_32bit.img_size: 1845493760" ])
except:
    print("Something bad happened again...")
    error_url.append("%d: %s" % (len(url) - 1, url[-1]))

# Test (10 lines)
url.append("user/node/state")
try:
    r_json = test_last_url(
        { "token": api_token, "nodes": [ "imt-1", "imt-2", "imt-3", "imt-4", "imt-37" ]},
        [ "nodes: /4", "nodes.imt-1.bin: admin_job", "nodes.imt-3.state: deployed", "nodes.imt-37.os_password: toto" ])
except:
    print("Something bad happened again...")
    error_url.append("%d: %s" % (len(url) - 1, url[-1]))
try:
    r_json = test_last_url(
        { "token": api_token, "user": "admin@piseduce" },
        [ "nodes: /2", "nodes.imt-1.bin: admin_job", "nodes.imt-2.name: imt-2" ])
except:
    print("Something bad happened again...")
    error_url.append("%d: %s" % (len(url) - 1, url[-1]))

# Test (15 lines)
url.append("user/node/schedule")
try:
    r_json = test_last_url(
        { "token": api_token },
        [ 
            "nodes: /6", "nodes.imt-36: /3",
            "nodes.imt-37.2021-05-17.toto@piseduce: /4",
            "nodes.imt-37.2021-05-17.toto@piseduce.hours: /11"
        ])
except:
    print("Something bad happened again...")
    error_url.append("%d: %s" % (len(url) - 1, url[-1]))

# Test (15 lines)
url.append("user/node/list")
try:
    r_json = test_last_url(
        { "token": api_token },
        [ 
            ": /35", "imt-36: /5",
            "imt-2.port_number: 2",
            "imt-10.switch: 24port_RPI3",
            "imt-14.model: RPI3B+1G"
        ])
except:
    print("Something bad happened again...")
    error_url.append("%d: %s" % (len(url) - 1, url[-1]))

# Test (15 lines)
url.append("user/node/mine")
try:
    r_json = test_last_url(
        { "token": api_token, "user": "admin@piseduce" },
        [ 
            "states: /18", "nodes: /2",
            "nodes.imt-2.action_state: deployed",
            "nodes.imt-1.start_date: 2021-05-17 13:57:00",
            "nodes.imt-1.model: RPI3B+1G"
        ])
except:
    print("Something bad happened again...")
    error_url.append("%d: %s" % (len(url) - 1, url[-1]))
try:
    r_json = test_last_url(
        { "token": api_token, "user": "toto@piseduce" },
        [ 
            "states: /18", "nodes: /3",
            "nodes.imt-36.environment: ubuntu_20.04_64bit",
            "nodes.imt-37.os_password: toto"
        ])
except:
    print("Something bad happened again...")
    error_url.append("%d: %s" % (len(url) - 1, url[-1]))

### Write properties to the database
# Test (17 lines)
url.append("user/reserve")
try:
    r_json = test_last_url(
        {
            "token": api_token,
            "user": "testing@piseduce",
            "filter": { "model": "RPI4B8G", "nb_nodes": 3 },
            "start_date": "2021-05-17 13:57:00",
            "duration": 3
        },
        [ 
            "nodes: /3"
        ])
except:
    print("Something bad happened again...")
    error_url.append("%d: %s" % (len(url) - 1, url[-1]))
# Test (17 lines)
url.append("user/reserve")
try:
    r_json = test_last_url(
        {
            "token": api_token,
            "user": "testing@piseduce",
            "filter": { "name": "imt-27", "nb_nodes": 1 },
            "start_date": "2021-05-17 13:57:00",
            "duration": 3
        },
        [ 
            "nodes: /1"
        ])
except:
    print("Something bad happened again...")
    error_url.append("%d: %s" % (len(url) - 1, url[-1]))
# Test (17 lines)
url.append("user/reserve")
try:
    r_json = test_last_url(
        {
            "token": api_token,
            "user": "testing@piseduce",
            "filter": { "name": "imt-3", "nb_nodes": 1 },
            "start_date": "2021-05-17 13:57:00",
            "duration": 1
        },
        [ 
            "nodes: /0"
        ])
except:
    print("Something bad happened again...")
    error_url.append("%d: %s" % (len(url) - 1, url[-1]))
# Test (17 lines)
url.append("user/reserve")
try:
    r_json = test_last_url(
        {
            "token": api_token,
            "user": "testing@piseduce",
            "filter": { "name": "imt-3", "nb_nodes": 1 },
            "start_date": "2021-05-17 11:57:00",
            "duration": 3
        },
        [ 
            "nodes: /0"
        ])
except:
    print("Something bad happened again...")
    error_url.append("%d: %s" % (len(url) - 1, url[-1]))
# Test (17 lines)
url.append("user/reserve")
try:
    r_json = test_last_url(
        {
            "token": api_token,
            "user": "testing@piseduce",
            "filter": { "name": "imt-3", "nb_nodes": 1 },
            "start_date": "2021-05-20 13:57:00",
            "duration": 4
        },
        [ 
            "nodes: /0"
        ])
except:
    print("Something bad happened again...")
    error_url.append("%d: %s" % (len(url) - 1, url[-1]))
# Test (17 lines)
url.append("user/reserve")
try:
    r_json = test_last_url(
        {
            "token": api_token,
            "user": "testing@piseduce",
            "filter": { "name": "imt-1", "nb_nodes": 1 },
            "start_date": "2021-05-17 10:57:00",
            "duration": 72
        },
        [ 
            "nodes: /0"
        ])
except:
    print("Something bad happened again...")
    error_url.append("%d: %s" % (len(url) - 1, url[-1]))
# Test (17 lines)
url.append("user/configure")
try:
    r_json = test_last_url(
        {
            "token": api_token,
            "user": "admin@piseduce"
        },
        [ 
            ": /2",
            "imt-4: /8",
            "imt-3.end_date: 2021-05-21 11:57:00"
        ])
except:
    print("Something bad happened again...")
    error_url.append("%d: %s" % (len(url) - 1, url[-1]))
# Test (17 lines)
url.append("user/configure")
try:
    r_json = test_last_url(
        {
            "token": api_token,
            "user": "admin@piseduce"
        },
        [ 
            ": /2",
            "imt-4: /8",
            "imt-3.end_date: 2021-05-21 11:57:00"
        ])
except:
    print("Something bad happened again...")
    error_url.append("%d: %s" % (len(url) - 1, url[-1]))
# Test (17 lines)
url.append("user/deploy")
try:
    r_json = test_last_url(
        {
            "token": api_token,
            "user": "testing@piseduce",
            "nodes": {
                "imt-29": {
                    "node_bin": "test"
                }
            }
        },
        [ 
            "imt-29.missing: /3"
        ])
except:
    print("Something bad happened again...")
    error_url.append("%d: %s" % (len(url) - 1, url[-1]))
# Test (22 lines)
url.append("user/deploy")
try:
    r_json = test_last_url(
        {
            "token": api_token,
            "user": "testing@piseduce",
            "nodes": {
                "imt-27": {
                    "node_bin": "test",
                    "update_os": "test",
                    "part_size": "test",
                    "environment": "test"
                }
            }
        },
        [ 
            "imt-27.state: ready"
        ])
except:
    print("Something bad happened again...")
    error_url.append("%d: %s" % (len(url) - 1, url[-1]))
# Test (22 lines)
url.append("user/destroy")
try:
    r_json = test_last_url(
        {
            "token": api_token,
            "user": "testing@piseduce",
            "nodes": [ "imt-27", "imt-20", "imt-36" ]
        },
        [ 
            ": /3",
            "imt-27: success",
            "imt-20: failure",
            "imt-36: failure"
        ])
except:
    print("Something bad happened again...")
    error_url.append("%d: %s" % (len(url) - 1, url[-1]))

# Clean the database
db = open_session()
for s in db.query(Schedule).filter(Schedule.owner == "testing@piseduce").all():
    db.delete(s)
close_session(db)
# Display the results
print("######################################################")
if len(error_url) == 0:
    print("Tests are successfully passed")
else:
    print("%d detected errors from the following URL:" % len(error_url))
    for url in error_url:
        print("  %s" % url)
