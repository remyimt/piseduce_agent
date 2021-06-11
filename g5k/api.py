from api.tool import safe_string
from database.connector import open_session, close_session, row2props
from database.tables import Action, ActionProperty, Environment, Node, Schedule, Switch
from datetime import datetime, timedelta, timezone
from grid5000 import Grid5000
from importlib import import_module
from lib.config_loader import DATE_FORMAT, get_config
from sqlalchemy import inspect, and_, or_
from agent_exec import free_reserved_node, new_action, init_action_process, save_reboot_state
import json, logging, os, pytz, time


G5K_SITE = Grid5000(
    username = get_config()["grid5000_user"],
    password = get_config()["grid5000_password"]
).sites[get_config()["grid5000_site"]]


# Delete the jobs existing in the schedule but not in the g5k API
def check_deleted_jobs(db_jobs, g5k_jobs, db):
    """
    db_jobs: { job_uid: db_schedule_obj }
    g5k_jobs: the return of G5K_SITE.jobs.list()
    """
    to_delete = []
    for job in db_jobs:
        found = False
        for g in g5k_jobs:
            if str(g.uid) == job:
                found = True
        if not found:
            to_delete.append(db_jobs[job])
    for job in to_delete:
        logging.warning("[%s] is deleted because it does not exist in g5k" % job.node_name)
        delete_job(job, db)


def delete_job(db_job, db):
    # Delete the actions associated to this job
    for a in db.query(Action).filter(Action.node_name == db_job.node_name).all():
        db.delete(a)
    # Delete the action properties associated to this job
    for a in db.query(ActionProperty).filter(ActionProperty.node_name == db_job.node_name).all():
        db.delete(a)
    # Delete the job from the Schedule table
    db.delete(db_job)


def build_server_list():
    # The file to build the server list without querying the grid5000 API
    server_file = "g5k-servers.json"
    # Get all nodes in the default queue of this site
    servers = {}
    if os.path.isfile(server_file):
        with open(server_file, "r") as f:
            servers = json.load(f)
    else:
        # List all nodes of the site
        logging.info("Query the API to build the server list")
        for cl in G5K_SITE.clusters.list():
            for node in G5K_SITE.clusters[cl.uid].nodes.list():
                if "default" in node.supported_job_types["queues"]:
                    servers[node.uid] = {
                        "name": node.uid,
                        "site": G5K_SITE.uid,
                        "cluster": cl.uid,
                        "cpu_nb": str(node.architecture["nb_threads"]),
                        "memoryMB": str(node.main_memory["ram_size"] / 1024 / 1024 / 1024),
                        "model": node.chassis["name"]
                    }
        # Remove dead servers
        nodes = G5K_SITE.status.list().nodes
        for node  in nodes:
            node_name = node.split(".")[0]
            if node_name in servers:
                if nodes[node]["hard"] == "dead":
                    del servers[node_name]
        with open(server_file, "w") as f:
            f.write(json.dumps(servers, indent = 4))
    return servers


# Convert a list of grid5000 status to a list of reservations
# Parameter: *.status.list().nodes
def status_to_reservations(node_status):
    result = {}
    for node in node_status:
        node_name = node.split(".")[0]
        result[node_name] = []
        for resa in node_status[node]["reservations"]:
            start_date = datetime.fromtimestamp(resa["scheduled_at"], timezone.utc)
            end_date = start_date + timedelta(seconds = resa["walltime"])
            result[node_name].append({ "owner": resa["user_uid"], "start_date": start_date, "end_date": end_date })
    return result


# Add the environments to the configuration
def load_environments():
    env_names = [
        "centos7-x64-min",
        "centos8-x64-min",
        "debian10-x64-base",
        "debian10-x64-big",
        "debian10-x64-min",
        "debian10-x64-nfs",
        "debian10-x64-std",
        "debian10-x64-xen",
        "debian9-x64-base",
        "debian9-x64-big",
        "debian9-x64-min",
        "debian9-x64-nfs",
        "debian9-x64-std",
        "debian9-x64-xen",
        "debiantesting-x64-min",
        "ubuntu1804-x64-min",
        "ubuntu2004-x64-min"
    ]
    config = get_config()
    config["configure_prop"][config["node_type"]]["environment"] = { "values": env_names, "mandatory": True }


def client_list(arg_dict):
    return json.dumps({ "error": "DHCP client list is not available from server agents" })


def environment_list(arg_dict):
    return json.dumps({ "error": "Environment list is not available from server agents" })


def node_configure(arg_dict):
    if "user" not in arg_dict or "@" not in arg_dict["user"]:
        return json.dumps({ "parameters": "user: 'email@is.fr'" })
    result = {}
    # Common properties to every kind of nodes
    config = get_config()
    env_names = config["configure_prop"][config["node_type"]]["environment"].keys()
    conf_prop = {
        "node_bin": { "values": [], "mandatory": True },
    }
    conf_prop.update(config["configure_prop"][config["node_type"]])
    # Load the job configuration file
    db = open_session()
    schedule = db.query(Schedule).filter(Schedule.owner == arg_dict["user"]).all()
    uids = { sch.node_name: sch for sch in schedule}
    # Get the grid5000 jobs for the grid5000 user
    user_jobs = G5K_SITE.jobs.list(state = "running", user = get_config()["grid5000_user"])
    user_jobs += G5K_SITE.jobs.list(state = "waiting", user = get_config()["grid5000_user"])
    # Deleted jobs that do not exist anymore
    check_deleted_jobs(uids, user_jobs, db)
    # Add the unregistered grid5000 jobs to the DB
    for j in user_jobs:
        while j.started_at == 0:
            j.refresh()
            time.sleep(1)
        job_id = str(j.uid)
        if job_id in uids:
            schedule = uids[job_id]
        else:
            if j.started_at is None:
                start_date = "undefined"
                end_date = "undefined"
            else:
                start_date = datetime.fromtimestamp(j.started_at)
                end_date = datetime.fromtimestamp(j.started_at + j.walltime)
            # Record the job properties to the database
            schedule = Schedule()
            schedule.node_name = str(j.uid)
            schedule.owner = arg_dict["user"]
            schedule.start_date = start_date
            schedule.end_date = end_date
            schedule.state = "configuring"
            schedule.action_state = ""
            db.add(schedule)
        # Send the job information about job in the 'configuring' state
        if schedule.state == "configuring":
            job_name = "job %s" % schedule.node_name
            result[job_name] = conf_prop
            result[job_name]["start_date"] = str(schedule.start_date)
            result[job_name]["end_date"] = str(schedule.end_date)
    close_session(db)
    return json.dumps(result)


def node_deploy(arg_dict):
    # Check the parameters
    error_msg = { "parameters": 
            "user: 'email@is.fr', 'nodes': {'node-3': { 'node_bin': 'my_bin', 'environment': 'my-env' }}" }
    if "user" not in arg_dict or "@" not in arg_dict["user"] or "nodes" not in arg_dict:
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
    # Add the properties to the job configuration
    for node_name in node_prop:
        result[node_name] = {}
        my_prop = node_prop[node_name]
        if "node_bin" not in my_prop or len(my_prop["node_bin"]) == 0:
            if "missing" not in result[node_name]:
                result[node_name]["missing"] = [ "node_bin" ]
            else:
                result[node_name]["missing"].append("node_bin")
        if "environment" not in my_prop or len(my_prop["environment"]) == 0:
            if "missing" not in result[node_name]:
                result[node_name]["missing"] = [ "environment" ]
            else:
                result[node_name]["missing"].append("environment")
        if len(result[node_name]) == 0:
            node_uid = node_name[4:]
            # Remove special characters from the node bin name
            node_bin = safe_string(my_prop["node_bin"])
            # Remove spaces from value
            node_bin = node_bin.replace(" ", "_")
            # Record the job configuration to the database
            db = open_session()
            my_job = db.query(Schedule).filter(Schedule.node_name == node_uid).first()
            if my_job is None:
                logging.error("job %s not found in the Schedule DB table" % node_uid)
            else:
                my_job.bin = node_bin
                my_job.state = "ready"
                env = ActionProperty()
                env.owner = arg_dict["user"]
                env.node_name = my_job.node_name
                env.prop_name = "environment"
                env.prop_value = my_prop["environment"]
                db.add(env)
                ssh_key = ActionProperty()
                ssh_key.owner = arg_dict["user"]
                ssh_key.node_name = my_job.node_name
                ssh_key.prop_name = "ssh_key"
                if "form_ssh_key" in my_prop and len(my_prop["form_ssh_key"]) > 0:
                    ssh_key.prop_value = my_prop["form_ssh_key"]
                    db.add(ssh_key)
                elif "account_ssh_key" in my_prop and len(my_prop["account_ssh_key"]) > 0:
                    ssh_key.prop_value = my_prop["account_ssh_key"]
                    db.add(ssh_key)
                close_session(db)
                result[node_name] = { "state": "ready" }
    return json.dumps(result)


def node_deployagain(arg_dict):
    result = {}
    # Check POST data
    if "nodes" not in arg_dict or "user" not in arg_dict:
        return json.dumps({ "parameters": "nodes: ['name1', 'name2' ], user: 'email@is.fr'" })
    wanted = arg_dict["nodes"]
    user = arg_dict["user"]
    if len(user) == 0 or '@' not in  user:
        for n in wanted:
            result[n] = "no_email"
        return json.dumps(result)
    # Get information about the requested nodes
    db = open_session()
    nodes = db.query(Schedule
            ).filter(Schedule.node_name.in_(wanted)
            ).filter(Schedule.owner == user
            ).all()
    for n in nodes:
        if n.state == "ready":
            node_action = db.query(Action).filter(Action.node_name == n.node_name).first()
            # The deployment is completed, add a new action
            node_action = new_action(n, db)
            # The deployment is completed, add a new action
            init_action_process(node_action, "deploy")
            db.add(node_action)
            result[n.node_name] = "success"
        else:
            result[n.node_name] = "failure: %s is not ready" % n.node_name
    close_session(db)
    # Build the result
    for n in wanted:
        if n not in result:
            result[n] = "failure"
    return json.dumps(result)


def node_destroy(arg_dict):
    # Check POST data
    if "nodes" not in arg_dict or "user" not in arg_dict:
        return json.dumps({ "parameters": "nodes: ['name1', 'name2' ], user: 'email@is.fr'" })
    wanted = arg_dict["nodes"]
    user = arg_dict["user"]
    if len(user) == 0 or '@' not in  user:
        for n in wanted:
            result[n] = "no_email"
        return json.dumps(result)
    logging.info("Destroying the nodes: %s" % wanted)
    result = {}
    db = open_session()
    # Delete actions in progress for the nodes to destroy
    actions = db.query(Action).filter(Action.node_name.in_(wanted)).all()
    for action in actions:
        db.delete(action)
    # Get the reservations to destroy
    nodes = db.query(Schedule
            ).filter(Schedule.node_name.in_(wanted)
            ).filter(Schedule.owner == user
            ).all()
    for n in nodes:
        if n.state == "configuring":
            # The node is not deployed, delete the reservation and the associated properties
            free_reserved_node(db, n.node_name)
            result[n.node_name] = "success"
        else:
            # Create a new action to start the destroy action
            node_action = new_action(n, db)
            init_action_process(node_action, "destroy")
            db.add(node_action)
            result[n.node_name] = "success"
    close_session(db)
    # Build the result
    for n in wanted:
        if n not in result:
            result[n] = "failure"
    return json.dumps(result)


def node_extend(arg_dict):
    result = {}
    # Check POST data
    if "nodes" not in arg_dict or "user" not in arg_dict:
        return json.dumps({ "parameters": "nodes: ['name1', 'name2' ], user: 'email@is.fr'" })
    wanted = arg_dict["nodes"]
    user = arg_dict["user"]
    if len(user) == 0 or '@' not in  user:
        for n in wanted:
            result[n] = "no_email"
        return json.dumps(result)
    # Get the current date
    now = datetime.now(timezone.utc)
    now = now.replace(tzinfo = None)
    # Get information about the requested nodes
    db = open_session()
    nodes = db.query(Schedule
            ).filter(Schedule.node_name.in_(wanted)
            ).filter(Schedule.owner == user
            ).all()
    for n in nodes:
        # Allow users to extend their reservation 4 hours before the end_date
        if (n.end_date - now).total_seconds() < 4 * 3600:
            new_end_date = n.end_date + (n.end_date - n.start_date)
            if (new_end_date - n.start_date).days > 7:
                new_end_date = n.start_date + timedelta(days=7)
            n.end_date = new_end_date
            result[n.node_name] = "success"
        else:
            result[n.node_name] = "failure: it is too early to extend the reservation"
    close_session(db)
    # Build the result
    for n in wanted:
        if n not in result:
            result[n] = "failure"
    return json.dumps(result)


def node_hardrebboot(arg_dict):
    result = {}
    # Check POST data
    if "nodes" not in arg_dict or "user" not in arg_dict:
        return json.dumps({ "parameters": "nodes: ['name1', 'name2' ], user: 'email@is.fr'" })
    wanted = arg_dict["nodes"]
    user = arg_dict["user"]
    if len(user) == 0 or '@' not in  user:
        for n in wanted:
            result[n] = "no_email"
        return json.dumps(result)
    # Get information about the requested nodes
    db = open_session()
    nodes = db.query(Schedule
            ).filter(Schedule.node_name.in_(wanted)
            ).filter(Schedule.owner == user
            ).all()
    for n in nodes:
        if n.state == "ready":
            # The deployment is completed, add a new action
            node_action = new_action(n, db)
            save_reboot_state(node_action, db)
            init_action_process(node_action, "reboot")
            db.add(node_action)
            result[n.node_name] = "success"
        else:
            logging.error("[%s] can not reboot because the state is not 'ready' (state: %s)" % (
                n.node_name, n.state))
            result[n.node_name] = "failure: %s is not ready" % n.node_name
    close_session(db)
    # Build the result
    for n in wanted:
        if n not in result:
            result[n] = "failure"
    return json.dumps(result)


def node_list(arg_dict):
    return json.dumps(build_server_list())


def node_mine(arg_dict):
    if "user" not in arg_dict or "@" not in arg_dict["user"]:
        return json.dumps({ "parameters": "user: 'email@is.fr'" })
    result = { "states": [], "nodes": {} }
    # Get the list of the states for the 'deploy' process
    py_module = import_module("%s.states" % get_config()["node_type"])
    PROCESS = getattr(py_module, "PROCESS")
    for p in PROCESS["deploy"]:
        if len(p["states"]) > len(result["states"]):
            result["states"] = p["states"]
    # Get the existing job for this user
    db = open_session()
    schedule = db.query(Schedule
        ).filter(Schedule.owner == arg_dict["user"]
        ).filter(Schedule.state != "configuring"
        ).all()
    db_jobs = { sch.node_name: sch for sch in schedule }
    if len(db_jobs) == 0:
        close_session(db)
        return json.dumps(result)
    user_jobs = G5K_SITE.jobs.list(state = "running", user = get_config()["grid5000_user"])
    user_jobs += G5K_SITE.jobs.list(state = "waiting", user = get_config()["grid5000_user"])
    check_deleted_jobs(db_jobs, user_jobs, db)
    for j in user_jobs:
        j.refresh()
        uid_str = str(j.uid)
        if uid_str in db_jobs:
            my_conf = db_jobs[uid_str]
            result["nodes"][uid_str] = {
                "node_name": uid_str,
                "bin": my_conf.bin,
                "start_date": str(my_conf.start_date),
                "end_date": str(my_conf.end_date),
                "state": my_conf.state,
                "job_state": j.state
            }
            assigned_nodes = db.query(ActionProperty
                ).filter(ActionProperty.node_name == my_conf.node_name
                ).filter(ActionProperty.prop_name == "assigned_nodes"
                ).first()
            if assigned_nodes is None:
                if len(j.assigned_nodes) > 0:
                    assigned_nodes = ActionProperty()
                    assigned_nodes.owner = arg_dict["user"]
                    assigned_nodes.node_name = my_conf.node_name
                    assigned_nodes.prop_name = "assigned_nodes"
                    assigned_nodes.prop_value = ",".join(j.assigned_nodes)
                    db.add(assigned_nodes)
                    result["nodes"][uid_str]["assigned_nodes"] = assigned_nodes.prop_value
            else:
                result["nodes"][uid_str]["assigned_nodes"] = assigned_nodes.prop_value
    close_session(db)
    return json.dumps(result)


def node_reserve(arg_dict):
    # Check arguments
    if "filter" not in arg_dict or "user" not in arg_dict or \
        "start_date" not in arg_dict or "duration" not in arg_dict:
            logging.error("Missing parameters: '%s'" % arg_dict)
            return json.dumps({
                "parameters": "filter: {...}, user: 'email@is.fr', start_date: '2021-06-21 14:36:32', 'duration': 3" })
    if len(arg_dict["start_date"]) != 19:
        logging.error("Wrong date format: '%s'" % arg_dict["start_date"])
        return json.dumps({"parameters": "Wrong date format (required: YYYY-MM-DD HH-MM)"})
    result = { "nodes": [] }
    user = arg_dict["user"]
    f = arg_dict["filter"]
    # f = {'nb_nodes': '3', 'model': 'RPI4B8G', 'switch': 'main_switch'}
    nb_nodes = int(f["nb_nodes"])
    del f["nb_nodes"]
    start_date = datetime.strptime(arg_dict["start_date"], DATE_FORMAT).replace(tzinfo=timezone.utc)
    hours_added = timedelta(hours = arg_dict["duration"])
    end_date = start_date + hours_added
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
        cluster_name = node_name.split("-")[0]
        if cluster_name not in node_status:
            node_status[cluster_name] = status_to_reservations(G5K_SITE.clusters[cluster_name].status.list().nodes)
        ok_selected = True
        # Move the start date back 15 minutes to give the time for destroying the previous reservation
        minutes_removed = timedelta(minutes = 15)
        back_date = start_date - minutes_removed
        # Check the schedule of the existing reservations
        for reservation in node_status[cluster_name][node_name]:
            # Only one reservation for a specific node per user
            if reservation["owner"] == user:
                ok_selected = False
            # There is no reservation at the same date
            if (back_date > reservation["start_date"] and back_date < reservation["end_date"]) or (
                back_date < reservation["start_date"] and end_date > reservation["start_date"]):
                ok_selected = False
        if ok_selected:
            # Add the node to the reservation
            selected_nodes.append(node_name)
            if len(selected_nodes) == nb_nodes:
                # Exit when the required number of nodes is reached
                break;
    logging.info("Selected nodes: %s" % selected_nodes)
    # Set the common properties of the grid5000 job
    command = "sleep %d" % (int(arg_dict["duration"]) * 3600)
    walltime = "%s:00" % arg_dict["duration"]
    job_conf = {
        "name": "piseduce %s" % datetime.now(),
        "resources": "nodes=%d,walltime=%s" % (len(selected_nodes), walltime),
        "command": command,
        "types": [ "deploy" ]
    }
    # Set the 'reservation' property to define the job's start date
    now = datetime.utcnow().replace(tzinfo = timezone.utc)
    delta_s = (start_date - now).total_seconds()
    if  delta_s > 5 * 60:
        # Only consider the start_date if this date is after the next 5 minutes
        local_date = start_date.astimezone(pytz.timezone("Europe/Paris"))
        job_conf["reservation"] = str(local_date)[:-6]
    if len(selected_nodes) == 1:
        # Reserve the node from its server name
        logging.info("Reservation the node '%s' with the walltime '%s'" %(
            selected_nodes[0], walltime))
        job_conf["properties"] = "(host in ('%s.%s.grid5000.fr'))" % (
            selected_nodes[0], G5K_SITE.uid)
    else:
        # Reserve the nodes from cluster names
        clusters = set()
        for node in selected_nodes:
            clusters.add(node.split("-")[0])
        logging.info("Reservation on the clusters '%s' with the walltime '%s'" %(
            clusters, walltime))
        job_conf["properties"] = "(cluster in (%s))" % ",".join(["'%s'" % c for c in clusters])
    try:
        job = G5K_SITE.jobs.create(job_conf)
        result["nodes"] = selected_nodes
    except:
        logging.exception("Creating job: ")
    return json.dumps(result)


def node_schedule(arg_dict):
    result = { "nodes": {} }
    servers = build_server_list()
    reservations = status_to_reservations(G5K_SITE.status.list().nodes)
    for node_name in reservations:
        if node_name in servers:
            if node_name not in result["nodes"]:
                result["nodes"][node_name] = {}
            for resa in reservations[node_name]:
                # Round the end_date up to the next hour
                remains = resa["end_date"].timestamp() % 3600
                if remains == 0:
                    end_date_comp = resa["end_date"]
                else:
                    end_date_comp = datetime.fromtimestamp(resa["end_date"].timestamp() - remains + 3600, timezone.utc)
                # Iterate over hours between start_date and end_date
                hours_added = 0
                delta = timedelta(hours = hours_added)
                while resa["start_date"] + delta < end_date_comp:
                    new_date = resa["start_date"] + delta
                    day_str = str(new_date).split()[0]
                    hour_str = str(new_date.hour)
                    if day_str not in result["nodes"][node_name]:
                        result["nodes"][node_name][day_str] = {}
                    if resa["owner"] not in result["nodes"][node_name][day_str]:
                        result["nodes"][node_name][day_str][resa["owner"]] = {
                            "hours": [],
                            "owner": resa["owner"],
                            "start_hour": str(resa["start_date"]).split()[1],
                            "end_hour": str(resa["end_date"]).split()[1]
                        }
                    result["nodes"][node_name][day_str][resa["owner"]]["hours"].append(hour_str)
                    hours_added += 1
                    delta = timedelta(hours = hours_added)
    return json.dumps(result)


def node_state(arg_dict):
    result = { "nodes": {} }
    db = open_session()
    nodes = []
    if "nodes" in arg_dict:
        nodes = db.query(Schedule
            ).filter(Schedule.node_name.in_(arg_dict["nodes"])
            ).filter(Schedule.state != "configuring"
            ).all()
    elif "user" in arg_dict:
        nodes = db.query(Schedule
            ).filter(Schedule.owner == arg_dict["user"]
            ).filter(Schedule.state != "configuring"
            ).all()
    # Get the state of the nodes
    for n in nodes:
        result["nodes"][n.node_name] = { "name": n.node_name, "state": n.state, "bin": n.bin }
        if n.state == "in_progress":
            # An action is in progress, get the state of this action
            action = db.query(Action.state).filter(Action.node_name == n.node_name).first()
            if action is None or action.state is None or len(action.state) == 0:
                result["nodes"][n.node_name]["state"] = n.state
            else:
                result["nodes"][n.node_name]["state"] = action.state.replace("_post", "").replace("_exec", "")
        if n.state == "ready":
            # There is no action associated to this node
            if n.action_state is not None and len(n.action_state) > 0:
                result["nodes"][n.node_name]["state"] = n.action_state
    # Get both the OS password and the environment copy progress of the nodes
    action_props = db.query(ActionProperty
            ).filter(ActionProperty.node_name.in_(result["nodes"].keys())
            ).filter(ActionProperty.prop_name.in_(["os_password", "percent"])).all()
    for prop in action_props:
        result["nodes"][prop.node_name][prop.prop_name] = prop.prop_value
    close_session(db)
    return json.dumps(result)


def switch_list(arg_dict):
    return json.dumps({ "error": "Switch list is not available from server agents" })
