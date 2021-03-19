from api.auth import auth
from database.connector import open_session, close_session
from database.tables import Environment, Node, NodeProperty, Switch 
from datetime import datetime
from glob import glob
from lib.config_loader import get_config
from lib.switch_snmp import get_poe_status, switch_test, turn_on_port, turn_off_port
from paramiko.ssh_exception import BadHostKeyException, AuthenticationException, SSHException
from sqlalchemy import distinct
import flask, json, logging, os, paramiko, shutil, subprocess, time


admin_v1 = flask.Blueprint("admin_v1", __name__)


def new_switch_prop(switch_name, prop_name, prop_value):
    new_prop = Switch()
    new_prop.name = switch_name
    new_prop.prop_name = prop_name
    new_prop.prop_value = prop_value
    return new_prop


@admin_v1.route("/add/switch", methods=["POST"])
@auth
def add_switch():
    switch_data = flask.request.json
    del switch_data["token"]
    switch_props = get_config()["switch_prop"]
    # Check if all properties belong to the POST data
    missing_data = dict([ (key_data, []) for key_data in switch_props if key_data not in switch_data.keys()])
    if len(missing_data) == 0:
        checks = {}
        for data in switch_data:
            checks[data] = { "value": switch_data[data] }
        db = open_session()
        existing = db.query(Switch).filter_by(name = switch_data["name"]).all()
        for to_del in existing:
            db.delete(to_del)
        close_session(db)
        # Check the IP
        ip_check = False
        cmd = 'ping -c 1 -W 1 %s' % switch_data['ip']
        process = subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        ip_check = process.returncode == 0
        checks["ip"]["check"] = ip_check
        if ip_check:
            # Remove the last digit of the OID
            root_oid = switch_data["oid_first_port"]
            root_oid = root_oid[:root_oid.rindex(".")]
            switch_info = switch_test(switch_data["ip"], switch_data["community"], root_oid)
            # Check the SNMP connection
            snmp_check = switch_info["success"]
            checks["community"]["check"] = snmp_check
            checks["oid_first_port"]["check"] = snmp_check
        if ip_check and snmp_check:
            # Add the switch
            db = open_session()
            db.add(new_switch_prop(switch_data["name"], "ip", switch_data["ip"]))
            db.add(new_switch_prop(switch_data["name"], "community", switch_data["community"]))
            db.add(new_switch_prop(switch_data["name"], "port_nb", switch_info["port_nb"]))
            db.add(new_switch_prop(switch_data["name"], "master_port", switch_data["master_port"]))
            db.add(new_switch_prop(switch_data["name"], "oid", switch_info["oid"]))
            db.add(new_switch_prop(switch_data["name"], "oid_offset", switch_info["offset"]))
            close_session(db)
            return json.dumps({ "switch": switch_data["name"] })
        else:
            return json.dumps({"check": checks})
    else:
        return json.dumps({"missing": missing_data })


@admin_v1.route("/switch/ports/<string:switch_name>", methods=["POST"])
@auth
def port_status(switch_name):
    status = get_poe_status(switch_name)
    result = { switch_name: [] }
    for port in range(0, len(status)):
        if status[port] == '1':
            result[switch_name].append('on')
        elif status[port] == '2':
            result[switch_name].append('off')
        else:
            result[switch_name].append('unknown')
    return json.dumps(result)


@admin_v1.route("/switch/nodes/<string:switch_name>", methods=["POST"])
@auth
def switch_nodes(switch_name):
    result = { "errors": [], "nodes": {}}
    db = open_session()
    switch_info = db.query(Switch).filter(Switch.name == switch_name).filter(Switch.prop_name == "master_port").first()
    node_info = db.query(NodeProperty).filter(NodeProperty.prop_name.in_(["switch", "port_number"])).all()
    if switch_info is not None:
        result["nodes"][switch_info.prop_value] = "pimaster"
    # Build the node information
    nodes = {}
    for info in node_info:
        if info.name not in nodes:
            nodes[info.name] = {}
        nodes[info.name][info.prop_name] = info.prop_value
    close_session(db)
    for n in nodes:
        if nodes[n]["switch"] == switch_name:
            result["nodes"][str(nodes[n]["port_number"])] = n
    return json.dumps(result)


@admin_v1.route("/switch/turn_on/<string:switch_name>", methods=["POST"])
@auth
def turn_on(switch_name):
    if "ports" in flask.request.json:
        for port in flask.request.json["ports"]:
            turn_on_port(switch_name, port)
    return json.dumps({})


@admin_v1.route("/switch/turn_off/<string:switch_name>", methods=["POST"])
@auth
def turn_off(switch_name):
    result = {"errors": [] }
    if "ports" not in flask.request.json:
        result["errors"].append("Required parameters: 'ports'")
    db = open_session()
    master_port = db.query(Switch).filter(Switch.name == switch_name
        ).filter(Switch.prop_name == "master_port").first().prop_value
    close_session(db)
    for port in flask.request.json["ports"]:
        if port == master_port:
            result["errors"].append("can not turn off the pimaster")
            logging.error("can not turn off the pimaster on the port  %s of the switch '%s'" % (
                port, switch_name))
        else:
            turn_off_port(switch_name, port)
    return json.dumps(result)


@admin_v1.route("/switch/init_detect", methods=["POST"])
@auth
def init_detect():
    result = { "errors": [], "network": "", "macs": [] }
    if "ports" not in flask.request.json:
        result["errors"].append("Required parameters: 'ports'")
        return json.dumps(result)
    # Get the network IP from the dnsmasq configuration
    cmd = "grep listen-address /etc/dnsmasq.conf"
    process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        universal_newlines=True)
    network_ip = process.stdout.split("=")[1]
    result["network"] = network_ip[:network_ip.rindex(".")]
    if len(result["network"].split(".")) != 3:
        logging.error("Wrong network IP from the dnsmasq configuration: %s" % result["network"])
        result["errors"].append("Wrong network IP from the dnsmasq configuration")
    # Get existing static IP from the dnsmasq configuration
    existing_ips = []
    existing_macs = []
    cmd = "grep ^dhcp-host /etc/dnsmasq.conf"
    process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        universal_newlines=True)
    for line in process.stdout.split('\n'):
        if "," in line and not line.startswith("#"):
            existing_ips.append(line.split(",")[2])
            existing_macs.append(line.split(",")[0][-17:])
            result["macs"].append(line.split(",")[0][-17:])
    logging.info("existing ips: %s" % existing_ips)
    logging.info("existing macs: %s" % existing_macs)
    # Check the node IP is available
    for port in flask.request.json["ports"]:
        node_ip = "%s.%s" % (result["network"], port)
        if  node_ip in existing_ips:
            result["errors"].append("%s already exists in the DHCP configuration!" % node_ip)
            return json.dumps(result)
    # Expose TFTP files to all nodes (boot from the NFS server)
    tftp_files = glob('/tftpboot/rpiboot_uboot/*')
    for f in tftp_files:
        if os.path.isdir(f):
            new_f = '/tftpboot/%s' % os.path.basename(f)
            if not os.path.isdir(new_f):
                shutil.copytree(f, new_f)
        else:
            shutil.copy(f, '/tftpboot/%s' % os.path.basename(f))
    return json.dumps(result)


@admin_v1.route("/switch/dhcp_conf/<string:switch_name>", methods=["POST"])
@auth
def dhcp_conf(switch_name):
    result = { "errors": [], "node_ip": "" }
    if "port" not in flask.request.json or "macs" not in flask.request.json or "network" not in flask.request.json:
        result["errors"].append("Required parameters: 'port', 'macs' and 'network'")
        return json.dumps(result)
    node_port = flask.request.json["port"]
    known_macs = flask.request.json["macs"]
    network_ip = flask.request.json["network"]
    # Detect MAC address by sniffing DHCP requests
    logging.info('Reading system logs to get failed DHCP requests')
    # Reading system logs to retrieve failed DHCP requests
    cmd = "grep DHCPDISCOVER /var/log/syslog | grep \"no address\" | tail -n 1"
    process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            universal_newlines=True)
    node_mac = ""
    for line in process.stdout.split('\n'):
        if len(line) > 0:
            now = datetime.now()
            # Hour with the format "%H:%M:%S"
            hour = line.split(" ")[2].split(":")
            log_date = now.replace(hour = int(hour[0]), minute = int(hour[1]), second = int(hour[2]))
            logging.info("Last DHCP request at %s" % log_date)
            if (now - log_date).seconds < 10:
                mac = line.split(" ")[7]
                if len(mac) == 17 and (mac.startswith("dc:a6:32") or mac.startswith("b8:27:eb")):
                    if mac in known_macs:
                        logging.error("[node-%s] MAC '%s' already exists in the DHCP configuration" % (node_port, mac))
                    node_mac = mac
    if len(node_mac) > 0:
        logging.info("[node-%s] new node with the MAC '%s'" % (node_port, node_mac))
        node_ip = "%s.%s" % (network_ip, node_port)
        # Configure the node IP according to the MAC address
        cmd = "echo 'dhcp-host=%s,node-%s,%s' >> /etc/dnsmasq.conf" % (node_mac, node_port, node_ip)
        process = subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        logging.info("[node-%s] MAC: '%s', IP: '%s'" % (node_port, node_mac, node_ip))
        # Restart dnsmasq
        cmd = "service dnsmasq restart"
        process = subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # Reboot the node
        turn_off_port(switch_name, node_port)
        time.sleep(1)
        turn_on_port(switch_name, node_port)
        # Fill the result
        result["node_ip"] = node_ip
    else:
        result["errors"].append("[node-%s] No detected MAC" % node_port)
        logging.error("[node-%s] no detected MAC" % node_port)
    return json.dumps(result)


@admin_v1.route("/switch/dhcp_conf/<string:switch_name>/del", methods=["POST"])
@auth
def dhcp_conf_del(switch_name):
    result = { "errors": [] }
    flask_data = flask.request.json
    if "ip" not in flask_data or "mac" not in flask_data:
        result["errors"].append("Required parameters: 'ip' and 'mac'")
        return json.dumps(result)
    # Delete records in dnsmasq configuration using IP
    if len(flask_data["ip"]) > 0:
        cmd = "sed -i '/%s/d' /etc/dnsmasq.conf" % flask_data["ip"]
        process = subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    # Delete records in dnsmasq configuration using MAC
    if len(flask_data["mac"]) > 0:
        cmd = "sed -i '/%s/d' /etc/dnsmasq.conf" % flask_data["mac"]
        process = subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    cmd = "service dnsmasq restart"
    process = subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return json.dumps(result)


@admin_v1.route("/switch/node_conf/<string:switch_name>", methods=["POST"])
@auth
def node_conf(switch_name):
    result = { "errors": [] }
    if "node_ip" not in flask.request.json or "port" not in flask.request.json:
        result["errors"].append("Required parameters: 'node_ip', 'port'")
        return json.dumps(result)
    node_ip = flask.request.json["node_ip"]
    node_port = flask.request.json["port"]
    node_name = "%s-%s" % (switch_name, node_port)
    node_model = ""
    node_serial = ""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(node_ip, username="root", timeout=1.0)
        (stdin, stdout, stderr) = ssh.exec_command("cat /proc/cpuinfo")
        return_code = stdout.channel.recv_exit_status()
        for line in  stdout.readlines():
            output = line.strip()
            if "Revision" in output:
                rev = output.split()[-1]
                if rev == "c03111":
                    node_model = "RPI4B"
                if rev == "a020d3":
                    node_model = "RPI3Bplus"
                if len(node_model) == 0:
                    node_model = "unknown"
            if "Serial" in output:
                node_serial = output.split()[-1][-8:]
        ssh.close()
        # End of the configuration, turn off the node
        turn_off_port(switch_name, node_port)
        # Write the node information to the database
        if len(node_serial) > 0 and len(node_model) > 0:
            db = open_session()
            existing = db.query(Node).filter(Node.name == node_name).all()
            for to_del in existing:
                db.delete(to_del)
            existing = db.query(NodeProperty).filter(NodeProperty.name == node_name).all()
            for to_del in existing:
                db.delete(to_del)
            node_db = Node()
            node_db.type = get_config()["node_type"]
            node_db.name = node_name
            node_db.ip = node_ip
            node_db.status = "available"
            node_db.owner = None
            db.add(node_db)
            # add 'switch' property
            prop_db = NodeProperty()
            prop_db.name = node_name
            prop_db.prop_name = "port_number"
            prop_db.prop_value = node_port
            db.add(prop_db)
            # add 'port_number' property
            prop_db = NodeProperty()
            prop_db.name = node_name
            prop_db.prop_name = "switch"
            prop_db.prop_value = switch_name
            db.add(prop_db)
            # add 'ip' property
            prop_db = NodeProperty()
            prop_db.name = node_name
            prop_db.prop_name = "ip"
            prop_db.prop_value = node_ip
            db.add(prop_db)
            # add 'model' property
            prop_db = NodeProperty()
            prop_db.name = node_name
            prop_db.prop_name = "model"
            prop_db.prop_value = node_model
            db.add(prop_db)
            # add 'serial' property
            prop_db = NodeProperty()
            prop_db.name = node_name
            prop_db.prop_name = "serial"
            prop_db.prop_value = node_serial
            db.add(prop_db)
            close_session(db)
    except (AuthenticationException, SSHException, socket.error):
        result["errors"].append("[node-%s] can not connect via SSH to %s" % (node_port, node_ip))
        logging.warn("[node-%s] can not connect via SSH to %s" % (node_port, node_ip))
    return json.dumps(result)


@admin_v1.route("/switch/clean_detect/<string:switch_name>", methods=["POST"])
@auth
def clean_detect(switch_name):
    result = { "errors": [] }
    # Turn off the ports
    if "ports" in flask.request.json:
        logging.info("clean")
    # Delete the files in the tftpboot directory
    return json.dumps(result)


@admin_v1.route("/add/node", methods=["POST"])
@auth
def add_node():
    json_data = flask.request.json
    node_props = get_config()["node_prop"].copy()
    worker_type = get_config()["node_type"]
    node_props += get_config()[worker_type + "_prop"]
    missing_data = {}
    for prop in node_props:
        if prop not in json_data:
            # Create a missing prop without default values
            missing_data[prop] = []
            if prop == "switch":
                db = open_session()
                switches = db.query(distinct(Switch.name)).all()
                if len(switches) == 0:
                    missing_data[prop].append("no_values")
                else:
                    for sw in switches:
                        missing_data[prop].append(sw[0])
                close_session(db)
    # Check if all properties belong to the POST data
    if len(missing_data) == 0:
        db = open_session()
        existing = db.query(Node).filter(Node.name == json_data["name"]).all()
        for to_del in existing:
            db.delete(to_del)
        existing = db.query(NodeProperty).filter(NodeProperty.name == json_data["name"]).all()
        for to_del in existing:
            db.delete(to_del)
        node_db = Node()
        node_db.type = worker_type
        node_db.name = json_data["name"]
        node_db.ip = json_data["ip"]
        node_db.status = "available"
        node_db.owner = None
        db.add(node_db)
        for prop in node_props:
            if prop != "name":
                prop_db = NodeProperty()
                prop_db.name = json_data["name"]
                prop_db.prop_name = prop
                prop_db.prop_value = json_data[prop]
                db.add(prop_db)
        close_session(db)
        return { "node": json_data["name"] }
    else:
        return {"missing": missing_data }


@admin_v1.route("/add/environment", methods=["POST"])
@auth
def add_environment():
    env_data = flask.request.json
    env_props = get_config()["env_prop"]
    # Check if all properties belong to the POST data
    missing_data = dict([(key_data, []) for key_data in env_props if key_data not in env_data.keys()])
    if len(missing_data) == 0:
        db = open_session()
        existing = db.query(Environment).filter(Environment.name == env_data["name"]).all()
        for to_del in existing:
            db.delete(to_del)
        for prop in env_props:
            if prop != "name":
                if prop == "desc":
                    for d in env_data["desc"]:
                        env_db = Environment()
                        env_db.name = env_data["name"]
                        env_db.prop_name = prop
                        env_db.prop_value = d
                        db.add(env_db)
                else:
                    env_db = Environment()
                    env_db.name = env_data["name"]
                    env_db.prop_name = prop
                    env_db.prop_value = env_data[prop]
                    db.add(env_db)
        close_session(db)
        return { "environment": env_data["name"] }
    else:
        return {"missing": missing_data }


@admin_v1.route("/delete/<el_type>", methods=["POST"])
@auth
def delete(el_type):
    data = flask.request.json
    props = [ "name" ]
    # Check if all properties belong to the POST data
    missing_data = [key_data for key_data in props if key_data not in data.keys()]
    if len(missing_data) == 0:
        if el_type == "node":
            db = open_session()
            existing = db.query(Node).filter(Node.name == data["name"]).all()
            for to_del in existing:
                db.delete(to_del)
            existing = db.query(NodeProperty).filter(NodeProperty.name == data["name"]).all()
            for to_del in existing:
                db.delete(to_del)
            close_session(db)
        elif el_type == "switch":
            db = open_session()
            existing = db.query(Switch).filter(Switch.name == data["name"]).all()
            for to_del in existing:
                db.delete(to_del)
            close_session(db)
        elif el_type == "environment":
            db = open_session()
            existing = db.query(Environment).filter_by(name = data["name"]).all()
            for to_del in existing:
                db.delete(to_del)
            close_session(db)
        else:
            return {"type_error": data["type"] }
        return { "delete": len(existing) }
    else:
        return {"missing": missing_data }
