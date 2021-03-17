from database.connector import open_session, close_session, row2props
from database.tables import Switch
import subprocess


def switch_test(ip, community, oid):
    nb_port = 0
    cmd = "snmpwalk -v2c -c %s %s %s" % (community, ip, oid[:oid.rindex(".")])
    process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, universal_newlines=True)
    power_state = process.stdout.split("\n")
    power_state = [p.split(" = ")[0] for p in power_state if len(p) > 0]
    if process.returncode == 0 and len(power_state) > 0:
        oid_first_port = power_state[0]
        offset = int(oid_first_port[oid_first_port.rindex("."):][1:]) - 1
        detected_oid = oid_first_port[:oid_first_port.rindex(".")]
        return {
            "success": True,
            "oid": detected_oid,
            "port_nb": len(power_state),
            "offset":  offset
        }
    else:
        return {
            "success": False,
            "oid": "useless",
            "port_nb": 0,
            "offset":  -1
        }


def switch_props(sw_name):
    # Get the information about the switch
    db = open_session()
    props = row2props(db.query(Switch).filter(Switch.name == sw_name).all())
    close_session(db)
    return props


def get_poe_status(switch_name):
    props = switch_props(switch_name)
    oid = props["oid"]
    cmd = "snmpwalk -v2c -c %s %s %s" % (props["community"], props["ip"], oid[:oid.rindex(".")])
    process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, universal_newlines=True)
    power_state = process.stdout.split("\n")
    return [p[-1] for p in power_state if len(p) > 0]


def set_power_port(switch_name, port, value):
    props = switch_props(switch_name)
    snmp_address = "%s.%d" % (props["oid"], int(props["oid_offset"]) + int(port))
    cmd = "snmpset -v2c -c %s %s %s i %s" % (props["community"], props["ip"], snmp_address, value)
    subprocess.run(cmd.split(), check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return True


def turn_on_port(switch_name, port):
    set_power_port(switch_name, port, 1)
    return True


def turn_off_port(switch_name, port):
    set_power_port(switch_name, port, 2)
    return True
