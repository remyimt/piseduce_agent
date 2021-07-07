from api.tool import safe_string, decrypt_password
from database.connector import open_session, close_session
from database.tables import IotNodes, IotSelection, Schedule
from datetime import datetime
from glob import glob
from importlib import import_module
from lib.config_loader import get_config
import json, logging, os, pytz, subprocess, time


def experiment_to_reservation():
    reservations = {}
    # Get the reservations from the IoT-Lab plateform
    process = subprocess.run("iotlab-status -er", shell=True,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, universal_newlines=True)
    json_data = json.loads(process.stdout)["items"]
    iot_site = get_config()["iot_site"]
    for resa in json_data:
        for node_name in resa["nodes"]:
            if iot_site in node_name:
                name_short = node_name.split(".")[0]
                if name_short not in reservations:
                    reservations[name_short] = []
                info_res = {}
                start_date = datetime.strptime(resa["start_date"], "%Y-%m-%dT%H:%M:%SZ")
                start_date = start_date.replace(tzinfo=pytz.UTC)
                info_res["start_date"] = start_date.timestamp()
                info_res["end_date"] = start_date.timestamp() + resa["submitted_duration"] * 60
                info_res["owner"] = resa["user"]
                reservations[name_short].append(info_res)
    # Get the reservations from the database
    db = open_session()
    db_selection = db.query(IotSelection).filter(IotSelection.node_ids != "").all()
    # Check the IoT existing selections
    for selection in db_selection:
        for node_id in selection.node_ids.split("+"):
            name = "%s-%s" % (selection.archi.split(":")[0], node_id)
            if name not in reservations:
                reservations[name] = []
            reservations[name].append({
                "start_date": selection.start_date,
                "end_date": selection.end_date,
                "owner": selection.owner
            })
    close_session(db)
    return reservations


def build_name(db_sel):
    selection_name = "undefined"
    if db_sel.node_ids == "" or db_sel.node_ids is None:
        selection_name = "%d-%s-nodes" % (db_sel.node_nb, db_sel.archi.split(":")[0])
    else:
        selection_name = [ "%s-%s" % (db_sel.archi.split(":")[0], node_id)
            for node_id in db_sel.node_ids.split("+")]
        selection_name = ",".join(selection_name)
    return selection_name


def build_server_list():
    # The file to build the server list without querying the grid5000 API
    server_file = "node-iot.json"
    # Get all nodes in the default queue of this site
    servers = {}
    if os.path.isfile(server_file):
        with open(server_file, "r") as f:
            servers = json.load(f)
    else:
        # List all nodes of the site
        logging.info("Query the API to build the server list")
        process = subprocess.run("iotlab-status --nodes --site %s" % get_config()["iot_site"],
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            universal_newlines=True)
        json_data = json.loads(process.stdout)["items"]
        for node in json_data:
            if node["state"] == "Alive" or node["state"] == "Busy":
                node_name = node["network_address"].split(".")[0]
                servers[node_name] = {
                    "name": node_name,
                    "archi": node["archi"],
                    "site": node["site"],
                    "coords (x,y,z)": "%s, %s, %s" % (node["x"], node["y"], node["z"])
                }
        with open(server_file, "w") as f:
            f.write(json.dumps(servers, indent = 4))
    return servers


# Add the environments to the configuration
def load_environments():
    # No environment for iot-lab agents
    pass


def client_list(arg_dict):
    return json.dumps({ "error": "DHCP client list is not available from Iot-Lab agents" })


def environment_list(arg_dict):
    return json.dumps({ "error": "Environment list is not available from Iot-Lab agents" })


def node_configure(arg_dict):
    if "user" not in arg_dict or "@" not in arg_dict["user"]:
        return json.dumps({
            "parameters": {
                "user": "email@is.fr"
            }
        })
    result = {}
    # Common properties to every kind of nodes
    config = get_config()
    conf_prop = {
        "node_bin": { "values": [], "mandatory": True },
    }
    conf_prop.update(config["configure_prop"][config["node_type"]])
    # Get the available firmwares
    conf_prop["firmware"]["values"] = []
    conf_prop["firmware"]["values"].append("")
    for firmware in glob("iot-lab/firmware/*"):
        conf_prop["firmware"]["values"].append(os.path.basename(firmware))
    # Get the available profiles
    conf_prop["profile"]["values"] = []
    conf_prop["profile"]["values"].append("")
    cmd = "iotlab-profile -u %s -p %s get -l" % (
        arg_dict["iot_user"], decrypt_password(arg_dict["iot_password"]))
    process = subprocess.run(cmd, shell=True,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, universal_newlines=True)
    json_data = json.loads(process.stdout)
    conf_prop["profile"]["values"] += [
        "%s-%s" % (p["nodearch"], p["profilename"]) for p in json_data ]
    db = open_session()
    selections = db.query(IotSelection).filter(IotSelection.owner == arg_dict["user"]).all()
    now = time.time()
    for s in selections:
        if s.end_date < now:
            db.delete(s)
        else:
            selection_name = build_name(s)
            result[selection_name] = conf_prop
            result[selection_name]["start_date"] = s.start_date
            result[selection_name]["end_date"] = s.end_date
    close_session(db)
    return json.dumps(result)


def node_deploy(arg_dict):
    # Check the parameters
    if "user" not in arg_dict or "@" not in arg_dict["user"] or "nodes" not in arg_dict:
        error_msg = {
            "parameters": {
                "user": "email@is.fr",
                "nodes": {
                    "node-3": { "node_bin": "my_bin", "environment": "my-env" }
                }
            }
        }
        return json.dumps(error_msg)
    # Check the nodes dictionnary
    node_prop = arg_dict["nodes"]
    if isinstance(node_prop, dict):
        for val in node_prop.values():
            if not isinstance(val, dict):
                return json.dumps(error_msg)
    else:
        return json.dumps(error_msg)
    result = {}
    # Get the iot selections from the DB
    db = open_session()
    selections = db.query(IotSelection).filter(IotSelection.owner == arg_dict["user"]).all()
    for sel in selections:
        sel_name = build_name(sel)
        if sel_name in node_prop:
            result[sel_name] = {}
            my_prop = node_prop[sel_name]
            if "node_bin" not in my_prop or len(my_prop["node_bin"]) == 0:
                result[sel_name]["missing"] = [ "node_bin" ]
            else:
                # Remove special characters from the node bin name
                node_bin = safe_string(my_prop["node_bin"])
                # Remove spaces from value
                node_bin = node_bin.replace(" ", "_")
                iot_list = sel.filter_str
                firmware_path = ""
                if len(my_prop["firmware"]) > 0:
                    firmware_path = "iot-lab/firmware/%s" % my_prop["firmware"]
                    if not os.path.isfile(firmware_path):
                        firmware_path = "iot-lab/firmware/%s/%s" % (
                            arg_dict["iot_user"], my_prop["firmware"])
                        if not os.path.isfile(firmware_path):
                            firmware_path = ""
                if len(firmware_path) > 0 and len(my_prop["profile"]) > 0:
                    iot_list += ",%s,%s" % (firmware_path, my_prop["profile"].split("-", 1)[1])
                else:
                    if len(firmware_path) > 0:
                        iot_list += ",%s" % firmware_path
                    if len(my_prop["profile"]) > 0:
                        iot_list += ",,%s" % my_prop["profile"].split("-", 1)[1]
                if sel.start_date > time.time() + 5 * 60:
                    cmd = "iotlab-experiment -u %s -p %s submit -n %s -r %d -d %d -l %s" % (
                        arg_dict["iot_user"],
                        decrypt_password(arg_dict["iot_password"]),
                        node_bin,
                        sel.start_date,
                        (sel.end_date - sel.start_date) / 60,
                        iot_list)
                else:
                    cmd = "iotlab-experiment -u %s -p %s submit -n %s -d %d -l %s" % (
                        arg_dict["iot_user"],
                        decrypt_password(arg_dict["iot_password"]),
                        node_bin,
                        (sel.end_date - sel.start_date) / 60,
                        iot_list)
                process = subprocess.run(cmd, shell=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
                if process.returncode > 0:
                    if len(process.stdout) > 0:
                        logging.error(process.stdout)
                        result["error"] = [ process.stdout[-1] ]
                    elif len(process.stderr) > 0:
                        logging.error(process.stderr)
                        result["error"] = [ process.stderr[-1] ]
                else:
                    json_data = json.loads(process.stdout)
                    # Delete the iot_selection entry and create the schedule entry
                    result[sel_name] = { "state": "ready" }
                    schedule = Schedule()
                    schedule.node_name = json_data["id"]
                    schedule.owner = sel.owner
                    schedule.bin = node_bin
                    schedule.start_date = sel.start_date
                    schedule.end_date = sel.end_date
                    schedule.state = "ready"
                    schedule.action_state = ""
                    db.add(schedule)
                    db.delete(sel)
    close_session(db)
    return json.dumps(result)


def node_deployagain(arg_dict):
    result = {}
    for job_id in arg_dict["nodes"]:
        cmd = "iotlab-experiment -u %s -p %s reload -i %s" % (
            arg_dict["iot_user"], decrypt_password(arg_dict["iot_password"]), job_id)
        process = subprocess.run(cmd, shell=True,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, universal_newlines=True)
        json_data = json.loads(process.stdout)
        if "id" in json_data:
            result[job_id] = "success"
            # Get information about the new job
            cmd = "iotlab-experiment -u %s -p %s get -i %d -p" % (
                arg_dict["iot_user"],
                decrypt_password(arg_dict["iot_password"]),
                json_data["id"])
            process = subprocess.run(cmd, shell=True,
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, universal_newlines=True)
            job_data = json.loads(process.stdout)
            # Compute the dates of the job
            start_date = datetime.strptime(job_data["start_date"], "%Y-%m-%dT%H:%M:%SZ")
            start_date = start_date.replace(tzinfo=pytz.UTC)
            start_time = start_date.timestamp()
            end_time = start_time + job_data["submitted_duration"] * 60
            # Add the new job to the schedule
            db = open_session()
            job_org = db.query(Schedule).filter(Schedule.node_name == job_id).first()
            schedule = Schedule()
            schedule.node_name = job_data["id"]
            schedule.owner = arg_dict["user"]
            schedule.bin = job_org.bin
            schedule.start_date = start_time
            schedule.end_date = end_time
            schedule.state = "ready"
            schedule.action_state = ""
            db.add(schedule)
            # Add the assigned nodes
            assigned_nodes = []
            for n in job_data["nodes"]:
                assigned_nodes.append(n.split(".")[0] + "@" + n.split(".")[1])
            nodes_str = ",".join(assigned_nodes)
            nodes_db = IotNodes()
            nodes_db.job_id = job_data["id"]
            nodes_db.assigned_nodes = nodes_str
            db.add(nodes_db)
            close_session(db)
    return json.dumps(result)


def node_destroy(arg_dict):
    # Check POST data
    if "nodes" not in arg_dict or "user" not in arg_dict:
        return json.dumps({
            "parameters": {
                "user": "email@is.fr",
                "nodes": ["name1", "name2" ]
            }
        })
    wanted = arg_dict["nodes"]
    user = arg_dict["user"]
    if len(user) == 0 or '@' not in  user:
        for n in wanted:
            result[n] = "no_email"
        return json.dumps(result)
    logging.info("Destroying the nodes: %s" % wanted)
    result = {}
    for job_id in arg_dict["nodes"]:
        cmd = "iotlab-experiment -u %s -p %s stop -i %s" % (
            arg_dict["iot_user"], decrypt_password(arg_dict["iot_password"]), job_id)
        process = subprocess.run(cmd, shell=True,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, universal_newlines=True)
        json_data = json.loads(process.stdout)
        if "id" in json_data:
            result[job_id] = "success"
    return json.dumps(result)


def node_extend(arg_dict):
    return json.dumps({ "error": "Extend is not available for Iot-Lab agents." })


def node_hardreboot(arg_dict):
    return json.dumps({
        "error": "Hard reboot is not available for Iot-Lab agents. Use deploy_again to restart experiments."
    })


def node_list(arg_dict):
    return json.dumps(build_server_list())


def node_mine(arg_dict):
    if "user" not in arg_dict or "@" not in arg_dict["user"] or \
        "iot_user" not in arg_dict or "iot_password" not in arg_dict:
            return json.dumps({
                "parameters": {
                    "user": "email@is.fr",
                    "iot_user": "my_user",
                    "iot_password": "encrypted_pwd"
                }
            })
    result = { "states": [], "nodes": {} }
    # Get the list of the states for the 'deploy' process
    py_module = import_module("%s.states" % get_config()["node_type"])
    PROCESS = getattr(py_module, "PROCESS")
    for p in PROCESS["deploy"]:
        if len(p["states"]) > len(result["states"]):
            result["states"] = p["states"]
    # Get the existing job for this user
    db = open_session()
    schedule = db.query(Schedule).filter(Schedule.owner == arg_dict["user"]).all()
    db_jobs = { sch.node_name: sch for sch in schedule }
    cmd = "iotlab-experiment -u %s -p %s get -l" % (
        arg_dict["iot_user"], decrypt_password(arg_dict["iot_password"]))
    process = subprocess.run(cmd, shell=True,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, universal_newlines=True)
    json_data = json.loads(process.stdout)["items"]
    now = time.time()
    for resa in json_data:
        id_str = str(resa["id"])
        # Manage the jobs registrered in the DB
        if id_str in db_jobs:
            # Delete from the DB the jobs that ends more than 24 hours ago
            if db_jobs[id_str].end_date < now - 24 * 3600:
                db.delete(db_jobs[id_str])
                nodes = db.query(IotNodes).filter(IotNodes.job_id).first()
                if nodes is not None:
                    db.delete(nodes)
                # Do not analyze this job
                my_sch = None
            else:
                my_sch = db_jobs[id_str]
        else:
            start_date = datetime.strptime(resa["start_date"], "%Y-%m-%dT%H:%M:%SZ")
            start_date = start_date.replace(tzinfo=pytz.UTC)
            start_time = start_date.timestamp()
            end_time = start_date.timestamp() + resa["submitted_duration"] * 60
            # Check if the job is terminated within the last 24 hours
            if end_time > now - 24 * 3600:
                # Register the job to the DB
                schedule = Schedule()
                schedule.node_name = resa["id"]
                schedule.owner = arg_dict["user"]
                schedule.bin = "autodetected-jobs"
                schedule.state = "ready"
                schedule.action_state = ""
                # Compute the dates
                start_date = datetime.strptime(resa["start_date"], "%Y-%m-%dT%H:%M:%SZ")
                start_date = start_date.replace(tzinfo=pytz.UTC)
                schedule.start_date = start_date.timestamp()
                schedule.end_date = start_date.timestamp() + resa["submitted_duration"] * 60
                db.add(schedule)
                my_sch = schedule
            else:
                # Do not analyze this job
                my_sch = None
        if my_sch is not None:
            # Get the list of the assigned nodes
            nodes = db.query(IotNodes).filter(IotNodes.job_id == resa["id"]).first()
            if nodes is None:
                cmd = "iotlab-experiment -u %s -p %s get -i %s -n" % (
                    arg_dict["iot_user"], decrypt_password(arg_dict["iot_password"]), id_str)
                process = subprocess.run(cmd, shell=True,
                    stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, universal_newlines=True)
                node_data = json.loads(process.stdout)["items"]
                assigned_nodes = []
                for n in node_data:
                    name = n["network_address"]
                    assigned_nodes.append(name.split(".")[0] + "@" + name.split(".")[1])
                nodes_str = ",".join(assigned_nodes)
                nodes_db = IotNodes()
                nodes_db.job_id = resa["id"]
                nodes_db.assigned_nodes = nodes_str
                db.add(nodes_db)
            else:
                nodes_str = nodes.assigned_nodes
            # Send job information
            result["nodes"][my_sch.node_name] = {
                "node_name": my_sch.node_name,
                "bin": my_sch.bin,
                "start_date": my_sch.start_date,
                "end_date": my_sch.end_date,
                "state": resa["state"].lower(),
                "assigned_nodes": nodes_str
            }
            result["nodes"][my_sch.node_name]["data_link"] = (
                resa["state"] == "Terminated" or resa["state"] == "Stopped")
    close_session(db)
    return json.dumps(result)


def node_reserve(arg_dict):
    # Check arguments
    if "filter" not in arg_dict or "user" not in arg_dict or \
        "start_date" not in arg_dict or "duration" not in arg_dict or \
        "iot_user" not in arg_dict or "iot_password" not in arg_dict:
            logging.error("Missing parameters: '%s'" % arg_dict)
            return json.dumps({
                "parameters": {
                    "user": "email@is.fr",
                    "filter": "{...}",
                    "start_date": 1623395254,
                    "duration": 3,
                    "iot_password": "my_encrypted_pwd",
                    "iot_user": "my_user"
                }
            })
    result = { "nodes": [] }
    user = arg_dict["user"]
    f = arg_dict["filter"]
    # f = {'nb_nodes': '3', 'model': 'RPI4B8G', 'switch': 'main_switch'}
    nb_nodes = int(f["nb_nodes"])
    del f["nb_nodes"]
    start_date = arg_dict["start_date"]
    end_date = start_date + arg_dict["duration"] * 3600
    # Get the node list
    servers = build_server_list()
    filtered_nodes = []
    if "name" in f:
        if f["name"] in servers:
            filtered_nodes.append(f["name"])
    else:
        # Get the node properties used in the filter
        node_props = {}
        if len(f) == 0:
            filtered_nodes += servers.keys()
        else:
            for node in servers.values():
                ok_filtered = True
                for prop in f:
                    if node[prop] != f[prop]:
                        ok_filtered = False
                if ok_filtered:
                    filtered_nodes.append(node["name"])
    # Check the availability of the filtered nodes
    logging.info("Filtered nodes: %s" % filtered_nodes)
    selected_nodes = []
    node_status = {}
    for node_name in filtered_nodes:
        ok_selected = True
        # Move the start date back 15 minutes to give the time for destroying the previous reservation
        back_date = start_date - 15 * 60
        # Check the running experiments of the IoT-Lab plateform
        for name, reservations in experiment_to_reservation().items():
            if name == node_name:
                for resa in reservations:
                    # Only one reservation for a specific node per user
                    if name == node_name and resa["owner"] == user:
                        ok_selected = False
                    # There is no reservation at the same date
                    if (back_date > resa["start_date"] and back_date < resa["end_date"]) or \
                        (back_date < resa["start_date"] and end_date > resa["start_date"]):
                        ok_selected = False
        if ok_selected:
            # Add the node to the reservation
            selected_nodes.append(node_name)
            if len(selected_nodes) == nb_nodes:
                # Exit when the required number of nodes is reached
                break;
    logging.info("Selected nodes: %s" % selected_nodes)
    if len(selected_nodes) > 0:
        archi = servers[selected_nodes[0]]["archi"]
        db = open_session()
        if "name" in f:
            node_id = selected_nodes[0].split("-")[1]
            selection = db.query(IotSelection
                ).filter(IotSelection.owner == user
                ).filter(IotSelection.archi == archi
                ).filter(IotSelection.start_date == start_date
                ).filter(IotSelection.node_ids != ""
                ).first()
            if selection is None:
                iot_filter = "%s,%s,%s" % (get_config()["iot_site"], archi.split(":")[0], node_id)
                iot_selection = IotSelection()
                iot_selection.owner = user
                iot_selection.filter_str = iot_filter
                iot_selection.archi = archi
                iot_selection.node_ids = node_id
                iot_selection.node_nb = ""
                iot_selection.start_date = start_date
                iot_selection.end_date = end_date
                db.add(iot_selection)
            else:
                selection.filter_str += "+%s" % node_id
                selection.node_ids += "+%s" % node_id
        else:
            iot_filter = "%d,archi=%s+site=%s" % (
                    len(selected_nodes), archi, get_config()["iot_site"])
            iot_selection = IotSelection()
            iot_selection.owner = user
            iot_selection.filter_str = iot_filter
            iot_selection.archi = archi
            iot_selection.node_ids = ""
            iot_selection.node_nb = len(selected_nodes)
            iot_selection.start_date = start_date
            iot_selection.end_date = end_date
            db.add(iot_selection)
        # Store the iot-lab login/password to the DB in order to use it with agent_exec.py
        result["nodes"] = selected_nodes
        close_session(db)
    return json.dumps(result)


def node_schedule(arg_dict):
    result = { "nodes": {} }
    # Get the list of servers
    servers = build_server_list()
    # Get the list of the reservations
    for name, reservations in experiment_to_reservation().items():
        if name not in result["nodes"]:
            result["nodes"][name] = {}
        for resa in reservations:
            # Round the end_date up to the next hour
            remains = resa["end_date"] % 3600
            if remains == 0:
                end_date_comp = resa["end_date"]
            else:
                end_date_comp = resa["end_date"] - remains + 3600
            # Iterate over hours between start_date and end_date
            hours_added = 0
            while resa["start_date"] + hours_added * 3600 < end_date_comp:
                new_date = resa["start_date"] + hours_added * 3600
                result["nodes"][name][new_date] = {
                    "owner": resa["owner"],
                    "start_hour": resa["start_date"],
                    "end_hour": resa["end_date"]
                }
                hours_added += 1
    return json.dumps(result)


def node_state(arg_dict):
    result = { "nodes": {} }
    nodes = []
    # Get the existing job for this user
    db = open_session()
    schedule = db.query(Schedule).filter(Schedule.owner == arg_dict["user"]).all()
    db_jobs = { sch.node_name: sch for sch in schedule }
    if len(db_jobs) == 0:
        close_session(db)
        return json.dumps(result)
    cmd = "iotlab-experiment -u %s -p %s get -l" % (
        arg_dict["iot_user"], decrypt_password(arg_dict["iot_password"]))
    process = subprocess.run(cmd, shell=True,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, universal_newlines=True)
    json_data = json.loads(process.stdout)["items"]
    for resa in json_data:
        id_str = str(resa["id"])
        # Manage the jobs registrered in the DB
        if id_str in db_jobs:
            result["nodes"][resa["id"]] = {
                "name": resa["id"],
                "state": resa["state"].lower(),
                "bin": db_jobs[id_str].bin
            }
    close_session(db)
    return json.dumps(result)


def switch_list(arg_dict):
    return json.dumps({ "error": "Switch list is not available from Iot-Lab agents" })
