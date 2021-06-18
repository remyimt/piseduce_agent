from api.tool import decrypt_password
from database.tables import ActionProperty
from grid5000 import Grid5000
from lib.config_loader import get_config
import logging


def g5k_connect(action, db):
    credential = db.query(ActionProperty
        ).filter(ActionProperty.node_name == action.node_name
        ).filter(ActionProperty.prop_name == "g5k").first()
    user = credential.prop_value.split("/", 1)[0]
    pwd = credential.prop_value.split("/", 1)[1]
    return (Grid5000(
        username = user,
        password = decrypt_password(pwd)
    ).sites[get_config()["grid5000_site"]], user)


def wait_running_post(action, db):
    g5k_info = g5k_connect(action, db)
    g5k_site = g5k_info[0]
    g5k_user = g5k_info[1]
    for j in g5k_site.jobs.list(state="running", user = g5k_user):
        if str(j.uid) == action.node_name:
            return True
    return False


def deploy_exec(action, db):
    g5k_info = g5k_connect(action, db)
    g5k_site = g5k_info[0]
    g5k_user = g5k_info[1]
    for j in g5k_site.jobs.list(state="running", user = g5k_user):
        if str(j.uid) == action.node_name:
            j.refresh()
            if len(j.assigned_nodes) > 0:
                env = db.query(ActionProperty
                    ).filter(ActionProperty.node_name == action.node_name
                    ).filter(ActionProperty.prop_name == "environment"
                    ).first()
                ssh_key = db.query(ActionProperty
                    ).filter(ActionProperty.node_name == action.node_name
                    ).filter(ActionProperty.prop_name == "ssh_key"
                    ).first()
                old_dep = db.query(ActionProperty
                    ).filter(ActionProperty.node_name == action.node_name
                    ).filter(ActionProperty.prop_name == "deployment"
                    ).first()
                logging.info("[%s] deploy the environment '%s'" % (action.node_name, env.prop_value))
                deployment_conf = {
                    "nodes": j.assigned_nodes,
                    "environment": env.prop_value
                }
                if ssh_key is not None and len(ssh_key.prop_value) > 0:
                    deployment_conf["key"] = ssh_key.prop_value
                try:
                    dep = G5K_SITE.deployments.create(deployment_conf)
                    if old_dep is None:
                        # Create an action property to register the deployment UID
                        uid_prop = ActionProperty()
                        uid_prop.node_name = action.node_name
                        uid_prop.prop_name = "deployment"
                        uid_prop.prop_value = dep.uid
                        uid_prop.owner = env.owner
                        db.add(uid_prop)
                    else:
                        # Update the deployment UID (node_deployagain probably happens)
                        old_dep.prop_value = dep.uid
                    return True
                except:
                    logging.exception("Deployment error: ")
                    return False
    return False


def wait_deploying_post(action, db):
    dep_uid = db.query(ActionProperty
        ).filter(ActionProperty.node_name == action.node_name
        ).filter(ActionProperty.prop_name == "deployment"
        ).first()
    if dep_uid is None:
        logging.error("[%s] No deployment UID" % action.node_name)
        return False
    g5k_info = g5k_connect(action, db)
    g5k_site = g5k_info[0]
    g5k_user = g5k_info[1]
    for d in g5k_site.deployments.list(user = g5k_user):
        if d.uid == dep_uid.prop_value:
            return d.status == "terminated"
    logging.error("No deployment with the UUID %s" % deployment)
    return False


def destroying_exec(action, db):
    g5k_info = g5k_connect(action, db)
    g5k_site = g5k_info[0]
    g5k_user = g5k_info[1]
    # Get the jobs of the user
    user_jobs = G5K_SITE.jobs.list(state = "running", user = g5k_user)
    user_jobs += G5K_SITE.jobs.list(state = "waiting", user = g5k_user)
    for job in user_jobs:
        uid_str = str(job.uid)
        if uid_str == action.node_name:
            logging.info("[%s] delete this job" % uid_str)
            job.delete()
            return True
    return False
