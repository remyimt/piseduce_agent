from database.connector import open_session, close_session
from database.tables import RaspSwitch
import subprocess, traceback


def switch_cons(ip, community, oid):
    result = []
    try:
        cmd = "snmpwalk -v2c -c %s %s %s" % (community, ip, oid[:oid.rindex(".")])
        process = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            universal_newlines=True, timeout=30)
        power_state = process.stdout.split("\n")
        power_state = [p.split(" = ") for p in power_state if len(p) > 0]
        if process.returncode == 0 and len(power_state) > 0:
            for p in power_state:
                result.append(float(p[1].split()[-1].replace('"', '')))
    except:
        traceback.print_exc()
    return result


def switch_test(ip, community, oid):
    try:
        nb_port = 0
        cmd = "snmpwalk -v2c -c %s %s %s" % (community, ip, oid[:oid.rindex(".")])
        process = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            universal_newlines=True, timeout=30)
        power_state = process.stdout.split("\n")
        power_state = [p.split(" = ")[0] for p in power_state if len(p) > 0]
        if process.returncode == 0 and len(power_state) > 0:
            oid_first_port = power_state[0]
            offset = int(oid_first_port[oid_first_port.rindex("."):][1:]) - 1
            detected_oid = oid_first_port[:oid_first_port.rindex(".")]
            return {
                "success": True,
                "poe_oid": detected_oid,
                "port_number": len(power_state),
                "offset":  offset
            }
        else:
            return {
                "success": False,
                "poe_oid": "useless",
                "port_number": 0,
                "offset":  -1
            }
    except:
        return {
            "success": False,
            "poe_oid": "useless",
            "port_number": 0,
            "offset":  -1
        }


def get_poe_status(switch_name):
    db = open_session()
    sw = db.query(RaspSwitch).filter(RaspSwitch.name == switch_name).first()
    oid = sw.poe_oid
    cmd = "snmpwalk -v2c -c %s %s %s" % (sw.community, sw.ip, oid[:oid.rindex(".")])
    close_session(db)
    process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, universal_newlines=True)
    power_state = process.stdout.split("\n")
    return [p[-1] for p in power_state if len(p) > 0]


def set_power_port(switch_name, port, value):
    db = open_session()
    sw = db.query(RaspSwitch).filter(RaspSwitch.name == switch_name).first()
    snmp_address = "%s.%d" % (sw.poe_oid, sw.oid_offset + int(port))
    cmd = "snmpset -v2c -c %s %s %s i %s" % (sw.community, sw.ip, snmp_address, value)
    close_session(db)
    subprocess.run(cmd.split(), check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return True


def turn_on_port(switch_name, port):
    set_power_port(switch_name, port, 1)
    return True


def turn_off_port(switch_name, port):
    set_power_port(switch_name, port, 2)
    return True
