from api.auth import auth
from api.tool import safe_string
from database.connector import open_session, close_session
from database.tables import Action, ActionProperty, RaspEnvironment, RaspNode, RaspSwitch 
from datetime import datetime
from glob import glob
from lib.config_loader import get_config
from lib.switch_snmp import get_poe_status, switch_test, turn_on_port, turn_off_port
from paramiko.ssh_exception import BadHostKeyException, AuthenticationException, SSHException
from sqlalchemy import distinct
import flask, json, logging, os, paramiko, shutil, socket, subprocess, time


admin_v1 = flask.Blueprint("admin_v1", __name__)


def new_switch_prop(switch_name, prop_name, prop_value):
    new_prop = RaspSwitch()
    new_prop.name = switch_name
    new_prop.prop_name = prop_name
    new_prop.prop_value = prop_value
    return new_prop


@admin_v1.route("/node/pimaster", methods=["POST"])
@auth
def pimaster_node():
    db = open_session()
    pimaster_ip = db.query(RaspNode).filter(RaspNode.node_name == "pimaster"
        ).filter(RaspNode.prop_name == "master_ip").first().prop_value
    close_session(db)
    return json.dumps({ "ip": pimaster_ip })


@admin_v1.route("/pimaster/changeip", methods=["POST"])
@auth
def pimaster_changeip():
    new_ip = flask.request.json["new_ip"]
    new_network = new_ip[:new_ip.rindex(".")]
    # Check the static IP configuration
    cmd = "grep '^static ip_address' /etc/dhcpcd.conf"
    process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        universal_newlines=True)
    static_conf = process.stdout.strip()
    if len(static_conf) > 0:
        ip = static_conf.split("=")[1].replace("/24", "")
        network = ip[:ip.rindex(".")]
        # Change the static IP
        cmd = "sed -i 's:=%s/:=%s/:g' /etc/dhcpcd.conf" % (ip, new_ip)
        process = subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # Change the IP of the DHCP server
        cmd = "sed -i 's:=%s:=%s:g' /etc/dnsmasq.conf" % (ip, new_ip)
        process = subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # Change the IP of the DHCP clients
        cmd = "sed -i 's:,%s:,%s:g' /etc/dnsmasq.conf" % (network, new_network)
        process = subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # Delete the DHCP leases
        cmd = "rm /var/lib/misc/dnsmasq.leases"
        process = subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return json.dumps({ "msg": "IP configuration is successfully changed. You need to reboot pimaster!" })
    else:
        return json.dumps({ "msg": "The agent is not configured with a static IP" })


@admin_v1.route("/node/rename", methods=["POST"])
@auth
def rename_nodes():
    # Received data
    rename_data = flask.request.json
    if "base_name" not in rename_data:
        return json.dumps({ "error": "'base_name' parameter is required" })
    nodes = []
    error = ""
    db = open_session()
    if len(db.query(Action).all()) > 0:
        error = "can not rename the nodes: actions in progress"
    else:
        # Rename all nodes
        for node in db.query(Schedule).all():
            # We assume the node name looks like 'base_name-number'
            current = node.node_name.split("-")[0]
            node.name = node.node_name.replace(current, rename_data["base_name"])
            nodes.append(node.node_name)
        for node in db.query(RaspNode).all():
            # We assume the node name looks like 'base_name-number'
            current = node.node_name.split("-")[0]
            node.name = node.node_name.replace(current, rename_data["base_name"])
        for node in db.query(ActionProperty).all():
            # We assume the node name looks like 'base_name-number'
            current = node.node_name.split("-")[0]
            node.node_name = node.node_name.replace(current, rename_data["base_name"])
    close_session(db)
    if len(error) == 0:
        return json.dumps({ "nodes": nodes })
    else:
        return json.dumps({ "error": error })


# Add DHCP clients to the dnsmasq configuration
@admin_v1.route("/add/client", methods=["POST"])
@auth
def add_client():
    # Received data
    dhcp_data = flask.request.json
    del dhcp_data["token"]
    dhcp_props = get_config()["client_prop"]
    # Check if all properties belong to the POST data
    missing_data = dict([ (key_data, []) for key_data in dhcp_props if key_data not in dhcp_data.keys()])
    if len(missing_data) == 0:
        # Check the parameters of the DHCP client
        checks = {}
        for data in dhcp_data:
            checks[data] = { "value": dhcp_data[data] }
        # Get the network IP from the dnsmasq configuration
        cmd = "grep listen-address /etc/dnsmasq.conf"
        process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            universal_newlines=True)
        network_ip = process.stdout.split("=")[1]
        network_ip = network_ip[:network_ip.rindex(".")]
        # Get the existing IP addresses
        existing_ips = []
        cmd = "grep ^dhcp-host /etc/dnsmasq.conf"
        process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            universal_newlines=True)
        for line in process.stdout.split('\n'):
            if "," in line and not line.startswith("#"):
                existing_ips.append(line.split(",")[2])
        # Check the provided IP
        ip_check = dhcp_data["ip"].startswith(network_ip) and dhcp_data["ip"] not in existing_ips
        checks["ip"]["check"] = ip_check
        # Check the value looks like a MAC address
        mac_check = len(dhcp_data["mac_address"]) == 17 and len(dhcp_data["mac_address"].split(":")) == 6
        checks["mac_address"]["check"] = mac_check
        # Remove unwanted characters from the name
        dhcp_data["name"] = safe_string(dhcp_data["name"])
        if ip_check and mac_check:
            # Add the DHCP client
            cmd = "echo 'dhcp-host=%s,%s,%s' >> /etc/dnsmasq.conf" % (
                    dhcp_data["mac_address"], dhcp_data["name"], dhcp_data["ip"])
            process = subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            logging.info("[%s] MAC: '%s', IP: '%s'" % (dhcp_data["name"], dhcp_data["mac_address"], dhcp_data["ip"]))
            # Restart dnsmasq
            cmd = "service dnsmasq restart"
            process = subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return json.dumps({"client": { "name": dhcp_data["name"] } })
        else:
            return json.dumps({"check": checks})
    else:
        return json.dumps({"missing": missing_data })


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
        existing = db.query(RaspSwitch).filter(RaspSwitch.name == switch_data["name"]).all()
        for to_del in existing:
            db.delete(to_del)
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
            db = open_session()
            # Get information about existing switches to reserve the IP range for the nodes connected to the new switch
            all_switches = db.query(RaspSwitch).filter(RaspSwitch.prop_name.in_(["port_nb", "first_ip"])).all()
            existing_info = {}
            for sw in all_switches:
                if sw.name not in existing_info:
                    existing_info[sw.name] = {}
                existing_info[sw.name][sw.prop_name] = int(sw.prop_value)
            # Sort the switch information on the 'first_ip' property
            existing_info = { k: v for k, v in sorted(existing_info.items(), key = lambda item: item[1]["first_ip"]) }
            # Choose the last digit of the first IP such as [last_digit, last_digit + port_nb] is available
            last_digit = 1
            for sw in existing_info.values():
                new_last = last_digit + switch_info["port_nb"] - 1
                if new_last < sw["first_ip"]:
                    # We found the last_digit value
                    break
                else:
                    last_digit = sw["first_ip"] + sw["port_nb"]
            if last_digit + switch_info["port_nb"] - 1 > 250:
                close_session(db)
                msg = "No IP range available for the switch '%s' with %d ports" % (
                        switch_data["name"], switch_info["port_nb"])
                logging.error(msg)
                return json.dumps({ "error": msg })
            # Add the switch
            db.add(new_switch_prop(switch_data["name"], "ip", switch_data["ip"]))
            db.add(new_switch_prop(switch_data["name"], "community", switch_data["community"]))
            db.add(new_switch_prop(switch_data["name"], "port_nb", switch_info["port_nb"]))
            db.add(new_switch_prop(switch_data["name"], "first_ip", last_digit))
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
    switch_info = db.query(RaspSwitch).filter(RaspSwitch.name == switch_name).filter(RaspSwitch.prop_name == "master_port").first()
    node_info = db.query(RaspNode).filter(RaspNode.prop_name.in_(["switch", "port_number"])).all()
    if switch_info is not None:
        result["nodes"][switch_info.prop_value] = "pimaster"
    # Build the node information
    nodes = {}
    for info in node_info:
        if info.node_name not in nodes:
            nodes[info.node_name] = {}
        nodes[info.node_name][info.prop_name] = info.prop_value
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
    master_port = db.query(RaspSwitch).filter(RaspSwitch.name == switch_name
        ).filter(RaspSwitch.prop_name == "master_port").first().prop_value
    close_session(db)
    for port in flask.request.json["ports"]:
        if port == master_port:
            result["errors"].append("can not turn off the pimaster")
            logging.error("can not turn off the pimaster on the port  %s of the switch '%s'" % (
                port, switch_name))
        else:
            turn_off_port(switch_name, port)
    return json.dumps(result)


@admin_v1.route("/switch/init_detect/<string:switch_name>", methods=["POST"])
@auth
def init_detect(switch_name):
    result = { "errors": [], "network": "", "ip_offset": 0, "macs": [] }
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
    db = open_session()
    myswitch = db.query(RaspSwitch).filter(RaspSwitch.name == switch_name).filter(RaspSwitch.prop_name == "first_ip").first()
    ip_offset = int(myswitch.prop_value) - 1
    close_session(db)
    result["ip_offset"] = ip_offset
    for port in flask.request.json["ports"]:
        node_ip = "%s.%d" % (result["network"], (ip_offset + int(port)))
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
    if "port" not in flask.request.json or "macs" not in flask.request.json or \
        "base_name" not in flask.request.json or \
        "network" not in flask.request.json or "ip_offset" not in flask.request.json:
        result["errors"].append("Required parameters: 'port', 'macs', 'network', 'base_name' and 'ip_offset'")
        return json.dumps(result)
    known_macs = flask.request.json["macs"]
    node_port = int(flask.request.json["port"])
    last_digit = int(flask.request.json["ip_offset"]) + node_port
    node_name = "%s-%d" % (flask.request.json["base_name"], last_digit)
    node_ip = "%s.%d" % (flask.request.json["network"], last_digit)
    # Detect MAC address by sniffing DHCP requests
    logging.info('Reading system logs to get failed DHCP requests')
    # Reading system logs to retrieve failed DHCP requests
    cmd = "grep -a DHCPDISCOVER /var/log/syslog | grep \"no address\" | tail -n 1"
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
                        logging.error("[%s] MAC '%s' already exists in the DHCP configuration" % (node_name, mac))
                        result["errors"].append("%s already exists in the DHCP configuration!" % mac)
                        return json.dumps(result)
                    node_mac = mac
    if len(node_mac) > 0:
        logging.info("[%s] new node with the MAC '%s'" % (node_name, node_mac))
        # Configure the node IP according to the MAC address
        cmd = "echo 'dhcp-host=%s,%s,%s' >> /etc/dnsmasq.conf" % (node_mac, node_name, node_ip)
        process = subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        logging.info("[%s] MAC: '%s', IP: '%s'" % (node_name, node_mac, node_ip))
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
        logging.warning("[%s] no detected MAC" % node_name)
    return json.dumps(result)


def delele_dhcp_ip(client_ip):
    cmd = "sed -i '/%s$/d' /etc/dnsmasq.conf" % client_ip
    process = subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    cmd = "service dnsmasq restart"
    process = subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    
def delele_dhcp_mac(client_mac):
    cmd = "sed -i '/%s/d' /etc/dnsmasq.conf" % client_mac
    process = subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    cmd = "service dnsmasq restart"
    process = subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


@admin_v1.route("/switch/dhcp_conf/<string:switch_name>/del", methods=["POST"])
@auth
def dhcp_conf_del(switch_name):
    result = { "errors": [] }
    flask_data = flask.request.json
    if "ip" not in flask_data or "mac" not in flask_data:
        result["errors"].append("Required parameters: 'ip' or 'mac'")
        return json.dumps(result)
    # Delete records in dnsmasq configuration using IP
    if len(flask_data["ip"]) > 0:
        delele_dhcp_ip(flask_data["ip"])
    # Delete records in dnsmasq configuration using MAC
    if len(flask_data["mac"]) > 0:
        delele_dhcp_mac(flask_data["mac"])
    return json.dumps(result)


@admin_v1.route("/switch/node_conf/<string:switch_name>", methods=["POST"])
@auth
def node_conf(switch_name):
    result = { "errors": [], "serial": "" }
    if "node_ip" not in flask.request.json or "port" not in flask.request.json \
        or "base_name" not in flask.request.json:
        result["errors"].append("Required parameters: 'node_ip', 'base_name', 'port'")
        return json.dumps(result)
    node_ip = flask.request.json["node_ip"]
    node_port = flask.request.json["port"]
    node_name = "%s-%s" % (flask.request.json["base_name"], node_ip.split(".")[-1])
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
                if rev == "a020d3":
                    node_model = "RPI3B+1G"
                if rev == "a03111":
                    node_model = "RPI4B1G"
                if rev in ["b03111", "b03112" , "b03114"]:
                    node_model = "RPI4B2G"
                if rev in ["c03111", "c03112" , "c03114"]:
                    node_model = "RPI4B4G"
                if rev == "d03114":
                    node_model = "RPI4B8G"
                if len(node_model) == 0:
                    node_model = "unknown"
            if "Serial" in output:
                node_serial = output.split()[-1][-8:]
                result["serial"] = node_serial
        ssh.close()
        # End of the configuration, turn off the node
        turn_off_port(switch_name, node_port)
        # Write the node information to the database
        if len(node_serial) > 0 and len(node_model) > 0:
            db = open_session()
            existing = db.query(RaspNode).filter(RaspNode.node_name == node_name).all()
            for to_del in existing:
                db.delete(to_del)
            # add 'switch' property
            prop_db = RaspNode()
            prop_db.node_name = node_name
            prop_db.prop_name = "port_number"
            prop_db.prop_value = node_port
            db.add(prop_db)
            # add 'port_number' property
            prop_db = RaspNode()
            prop_db.node_name = node_name
            prop_db.prop_name = "switch"
            prop_db.prop_value = switch_name
            db.add(prop_db)
            # add 'ip' property
            prop_db = RaspNode()
            prop_db.node_name = node_name
            prop_db.prop_name = "ip"
            prop_db.prop_value = node_ip
            db.add(prop_db)
            # add 'model' property
            prop_db = RaspNode()
            prop_db.node_name = node_name
            prop_db.prop_name = "model"
            prop_db.prop_value = node_model
            db.add(prop_db)
            # add 'serial' property
            prop_db = RaspNode()
            prop_db.node_name = node_name
            prop_db.prop_name = "serial"
            prop_db.prop_value = node_serial
            db.add(prop_db)
            close_session(db)
    except (AuthenticationException, SSHException, socket.error):
        logging.warn("[node-%s] can not connect via SSH to %s" % (node_port, node_ip))
    return json.dumps(result)


@admin_v1.route("/switch/clean_detect", methods=["POST"])
@auth
def clean_detect():
    result = { "errors": [] }
    # Delete the files in the tftpboot directory
    tftp_files = glob('/tftpboot/rpiboot_uboot/*')
    for f in tftp_files:
        new_f = f.replace('/rpiboot_uboot','')
        if os.path.isdir(new_f):
            shutil.rmtree(new_f)
        else:
            if not 'bootcode.bin' in new_f:
                os.remove(new_f)
    return json.dumps(result)


@admin_v1.route("/add/node", methods=["POST"])
@auth
def add_node():
    json_data = flask.request.json
    node_props = get_config()["node_prop"].copy()
    agent_type = get_config()["node_type"]
    node_props += get_config()[agent_type + "_prop"]
    missing_data = {}
    for prop in node_props:
        if prop not in json_data:
            # Create a missing prop without default values
            missing_data[prop] = []
            if prop == "switch":
                db = open_session()
                switches = db.query(distinct(RaspSwitch.name)).all()
                if len(switches) == 0:
                    missing_data[prop].append("no_values")
                else:
                    for sw in switches:
                        missing_data[prop].append(sw[0])
                close_session(db)
    # Check if all properties belong to the POST data
    if len(missing_data) == 0:
        db = open_session()
        existing = db.query(RaspNode).filter(RaspNode.node_name == json_data["name"]).all()
        for to_del in existing:
            db.delete(to_del)
        for prop in node_props:
            if prop != "name":
                prop_db = RaspNode()
                prop_db.node_name = json_data["name"]
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
        existing = db.query(RaspEnvironment).filter(RaspEnvironment.name == env_data["name"]).all()
        for to_del in existing:
            db.delete(to_del)
        for prop in env_props:
            if prop != "name":
                if prop == "desc":
                    for d in env_data["desc"]:
                        env_db = RaspEnvironment()
                        env_db.name = env_data["name"]
                        env_db.prop_name = prop
                        env_db.prop_value = d
                        db.add(env_db)
                else:
                    env_db = RaspEnvironment()
                    env_db.name = env_data["name"]
                    env_db.prop_name = prop
                    env_db.prop_value = env_data[prop]
                    db.add(env_db)
        close_session(db)
        #TODO Reload the list of the environment from the DB
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
            existing = db.query(RaspNode).filter(RaspNode.node_name == data["name"]).all()
            for to_del in existing:
                db.delete(to_del)
            close_session(db)
        elif el_type == "switch":
            db = open_session()
            existing = db.query(RaspSwitch).filter(RaspSwitch.name == data["name"]).all()
            for to_del in existing:
                db.delete(to_del)
            close_session(db)
        elif el_type == "environment":
            db = open_session()
            existing = db.query(RaspEnvironment).filter_by(name = data["name"]).all()
            for to_del in existing:
                db.delete(to_del)
            close_session(db)
        elif el_type == "client":
            # Delete a DHCP client (IP address is in the 'name' attribute)
            delele_dhcp_ip(data["name"])
            existing = [ data["name"] ]
        else:
            return {"type_error": data["type"] }
        return { "delete": len(existing) }
    else:
        return {"missing": missing_data }
