from database.connector import row2props
from database.tables import ActionProperty, Node, NodeProperty, Environment
from datetime import datetime
from glob import glob
from lib.config_loader import DATE_FORMAT, get_config
from lib.switch_snmp import turn_on_port, turn_off_port
from paramiko.ssh_exception import BadHostKeyException, AuthenticationException, SSHException
from raspberry.states import SSH_IDX
import logging, os, paramiko, random, shutil, socket, string, subprocess, time

def new_password(stringLength=8):
    """Generate a random string of letters and digits """
    lettersAndDigits = string.ascii_letters + string.digits
    return ''.join(random.choice(lettersAndDigits) for i in range(stringLength))


# Deploy environments
def boot_conf_exec(action, db):
    serial = db.query(NodeProperty
            ).filter(NodeProperty.name  == action.node_name
            ).filter(NodeProperty.prop_name == "serial").first().prop_value
    # Create a folder containing network boot files that will be served via TFTP
    tftpboot_template_folder = "/tftpboot/rpiboot_uboot"
    tftpboot_node_folder = "/tftpboot/%s" % serial
    if os.path.isdir(tftpboot_node_folder):
        shutil.rmtree(tftpboot_node_folder)
    os.mkdir(tftpboot_node_folder)
    for tftpfile in glob("%s/*" % tftpboot_template_folder):
        os.symlink(tftpfile, tftpfile.replace(tftpboot_template_folder, tftpboot_node_folder))
    return True


def turn_off_exec(action, db):
    node_prop = row2props(db.query(NodeProperty
        ).filter(NodeProperty.name  == action.node_name
        ).filter(NodeProperty.prop_name.in_(["switch", "port_number"])).all())
    # Turn off port
    turn_off_port(node_prop["switch"], node_prop["port_number"])
    return True


def turn_on_exec(action, db):
    node_prop = row2props(db.query(NodeProperty
        ).filter(NodeProperty.name  == action.node_name
        ).filter(NodeProperty.prop_name.in_(["switch", "port_number"])).all())
    # Turn on port
    turn_on_port(node_prop["switch"], node_prop["port_number"])
    return True


def turn_on_post(action, db):
    node_ip = db.query(NodeProperty
            ).filter(NodeProperty.name  == action.node_name
            ).filter(NodeProperty.prop_name == "ip").first().prop_value
    cmd = "ping -W 1 -c 1 %s" % node_ip
    subproc = subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return subproc.returncode == 0


def ssh_test_post(action, db):
    node_ip = db.query(NodeProperty
            ).filter(NodeProperty.name  == action.node_name
            ).filter(NodeProperty.prop_name == "ip").first().prop_value
    # By default, we use the ssh_user of the environment. We assume the environment is deployed
    ssh_user = db.query(Environment
        ).filter(Environment.name  == action.environment
        ).filter(Environment.prop_name == "ssh_user"
        ).first().prop_value
    expected_hostname = action.node_name
    # Check if the node boots from the NFS filesystem
    if action.process == "deploy":
        if action.state_idx <= SSH_IDX:
            ssh_user = "root"
            expected_hostname = "nfspi"
    elif action.process == "reboot":
        if action.reboot_state is not None and len(action.reboot_state) > 0:
            state_info = action.reboot_state.split("?!")
            if len(state_info) == 2 and state_info[0] == "deploy" and int(state_info[1]) <= SSH_IDX:
                ssh_user = "root"
                expected_hostname = "nfspi"
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(node_ip, username=ssh_user, timeout=1.0)
        (stdin, stdout, stderr) = ssh.exec_command("cat /etc/hostname")
        return_code = stdout.channel.recv_exit_status()
        myname = stdout.readlines()[0].strip()
        ssh.close()
        if myname == expected_hostname:
            return True
        else:
            logging.error("[%s] wrong filesystem (expected: %s, found: %s)" % (
                action.node_name, expected_hostname, myname))
            return False
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return False


def env_copy_exec(action, db):
    env_path = get_config()["env_path"]
    node_ip = db.query(NodeProperty
            ).filter(NodeProperty.name  == action.node_name
            ).filter(NodeProperty.prop_name == "ip").first().prop_value
    pimaster_prop = row2props(db.query(NodeProperty).filter(NodeProperty.name  == "pimaster").all())
    env_prop = row2props(db.query(Environment).filter(Environment.name  == action.environment).all())
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(node_ip, username="root", timeout=1.0)
        # Get the path to the IMG file
        img_path = env_path + env_prop["img_name"]
        logging.info("[%s] copy %s to the SDCARD" % (action.node_name, img_path))
        # Write the image of the environment on SD card
        deploy_cmd = "rsh -o StrictHostKeyChecking=no %s@%s 'cat %s' | tar xzOf - | \
            pv -n -p -s %s 2> progress-%s.txt | dd of=/dev/mmcblk0 bs=4M conv=fsync &" % (
            pimaster_prop["user"], pimaster_prop["ip"], img_path, env_prop["img_size"], action.node_name)
        (stdin, stdout, stderr) = ssh.exec_command(deploy_cmd)
        return_code = stdout.channel.recv_exit_status()
        ssh.close()
        act_prop = ActionProperty()
        act_prop.node_name = action.node_name
        act_prop.prop_name = "percent"
        act_prop.prop_value = 0
        db.add(act_prop)
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return True


def env_copy_post(action, db):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username="root", timeout=1.0)
        if ps_ssh(ssh, "mmcblk0") > 0:
            ret_fct = True
        else:
            ret_fct = False
        ssh.close()
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return ret_fct


def env_check_exec(action, db):
    ret_fct = False
    img_size = db.query(Environment).filter(Environment.name  == action.environment
        ).filter(Environment.prop_name  == "img_size"
        ).first().prop_value
    percent_prop = db.query(ActionProperty
        ).filter(ActionProperty.node_name  == action.node_name
        ).filter(ActionProperty.prop_name  == "percent"
        ).first()
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username="root", timeout=1.0)
        if ps_ssh(ssh, "mmcblk0") == 0:
            ret_fct = True
        else:
            cmd = "tail -n 1 progress-%s.txt" % action.node_name
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
            output = stdout.readlines()
            if len(output) == 0:
                logging.warning("%s: no progress value for the running environment copy" % action.node_name)
                updated = datetime.strptime(action.updated_at, DATE_FORMAT)
                elapsedTime = (datetime.now() - updated).total_seconds()
                # Compute the progress value with an assumed transfert rate of 8 MB/s
                percent = round(elapsedTime * 8000000 * 100 / img_size)
            else:
                percent = output[0].strip()
            percent_prop.prop_value = percent
        ssh.close()
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return ret_fct


def delete_partition_exec(action, db):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username="root", timeout=1.0)
        # Register the size of the existing partition
        cmd = "rm progress-%s.txt; fdisk -l /dev/mmcblk0" % action.node_name
        (stdin, stdout, stderr) = ssh.exec_command(cmd)
        return_code = stdout.channel.recv_exit_status()
        output = stdout.readlines()
        # Delete the second partition
        cmd = "(echo d; echo 2; echo w) | fdisk -u /dev/mmcblk0"
        (stdin, stdout, stderr) = ssh.exec_command(cmd)
        return_code = stdout.channel.recv_exit_status()
        ssh.close()
        # Save the system size in sectors
        act_prop = ActionProperty()
        act_prop.node_name = action.node_name
        act_prop.prop_name = "system_size"
        act_prop.prop_value = int(output[-1].split()[3])
        db.add(act_prop)
        return True
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return False


def create_partition_exec(action, db):
    sector_start = db.query(Environment
        ).filter(Environment.name  == action.environment
        ).filter(Environment.prop_name  == "sector_start"
        ).first().prop_value
    act_prop = row2props(db.query(ActionProperty
        ).filter(ActionProperty.node_name  == action.node_name
        ).filter(ActionProperty.prop_name.in_(["system_size", "part_size" ])
        ).all())
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username="root", timeout=1.0)
        moreMB = act_prop["part_size"]
        if moreMB == "whole":
            logging.info("[%s] create a partition with the whole free space" % action.node_name)
            cmd = ("(echo n; echo p; echo 2; echo '%s'; echo ''; echo w) | fdisk -u /dev/mmcblk0" % sector_start)
        else:
            # Total size of the new partition in sectors (512B)
            moreSpace = act_prop["system_size"] + (int(moreMB) * 1024 * 1024 / 512)
            logging.info("[%s] create a partition with a size of %d sectors" % (action.node_name, moreSpace))
            cmd = ("(echo n; echo p; echo 2; echo '%s'; echo '+%d'; echo w) | fdisk -u /dev/mmcblk0" %
                (sector_start, moreSpace))
        (stdin, stdout, stderr) = ssh.exec_command(cmd)
        return_code = stdout.channel.recv_exit_status()
        cmd = "partprobe"
        (stdin, stdout, stderr) = ssh.exec_command(cmd)
        return_code = stdout.channel.recv_exit_status()
        ssh.close()
        return True
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return False


def mount_partition_exec(action, db):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username="root", timeout=1.0)
        # Update the deployment
        cmd = "mount /dev/mmcblk0p1 boot_dir"
        (stdin, stdout, stderr) = ssh.exec_command(cmd)
        return_code = stdout.channel.recv_exit_status()
        cmd = "mount /dev/mmcblk0p2 fs_dir"
        (stdin, stdout, stderr) = ssh.exec_command(cmd)
        return_code = stdout.channel.recv_exit_status()
        ssh.close()
        return True
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return False


def mount_partition_post(action, db):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username="root", timeout=1.0)
        # Check the boot_dir mount point
        cmd = "ls boot_dir/ | wc -l"
        (stdin, stdout, stderr) = ssh.exec_command(cmd)
        return_code = stdout.channel.recv_exit_status()
        output = stdout.readlines()
        nb_files = int(output[-1].strip())
        if nb_files < 5:
            logging.error("[%s] boot partition is not mounted" % action.node_name)
            return False
        # Delete the bootcode.bin file to prevent RPI3 to boot from SDCARD
        cmd = "rm boot_dir/bootcode.bin"
        (stdin, stdout, stderr) = ssh.exec_command(cmd)
        return_code = stdout.channel.recv_exit_status()
        # Check the fs_dir mount point
        cmd = "ls fs_dir/ | wc -l"
        (stdin, stdout, stderr) = ssh.exec_command(cmd)
        return_code = stdout.channel.recv_exit_status()
        output = stdout.readlines()
        nb_files = int(output[-1].strip())
        if nb_files < 2:
            logging.error("[%s] fs partition is not mounted" % action.node_name)
            return False
        return True
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return False


def resize_partition_exec(action, db):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username="root", timeout=1.0)
        cmd = "resize2fs /dev/mmcblk0p2 &> /dev/null &"
        (stdin, stdout, stderr) = ssh.exec_command(cmd)
        return_code = stdout.channel.recv_exit_status()
        ssh.close()
        return True
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return False


def resize_partition_post(action, db):
    try:
        ret_fct = False
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username="root", timeout=1.0)
        if ps_ssh(ssh, "resize2fs") > 0:
            ret_fct = True
        else:
            # Parse the output of the resizefs command
            cmd = "resize2fs /dev/mmcblk0p2"
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
            output = stderr.readlines()
            if len(output) > 2:
                if 'Nothing to do!' in output[1]:
                    ret_fct = True
        ssh.close()
        return ret_fct
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return False


def wait_resizing_exec(action, db):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username="root", timeout=1.0)
        ret_fct = False
        if ps_ssh(ssh, "resize2fs") == 0:
            ret_fct = True
        ssh.close()
        return ret_fct
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return False


def system_conf_exec(action, db):
    pwd = db.query(ActionProperty
        ).filter(ActionProperty.node_name  == action.node_name
        ).filter(ActionProperty.prop_name == "os_password"
        ).first()
    os_password = ""
    if pwd is None:
        # Generate the password
        os_password = new_password()
        act_prop = ActionProperty()
        act_prop.node_name = action.node_name
        act_prop.prop_name = "os_password"
        act_prop.prop_value = os_password
        db.add(act_prop)
    else:
        os_password = pwd.prop_value
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username="root",
            timeout=1.0,
            banner_timeout=1.0,
            auth_timeout=1.0)
        if action.environment.startswith("tiny_core"):
            # Set the hostname to modify the bash prompt
            cmd = "sed -i 's/$/ host=%s/g' boot_dir/cmdline3.txt" % action.node_name
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
        if action.environment.startswith("ubuntu"):
            # Set the password of the 'ubuntu' user
            cmd = "sed -i 's/tototiti/%s/' boot_dir/user-data" % os_password
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
            # Set the hostname to modify the bash prompt
            cmd = "echo '%s' > fs_dir/etc/hostname" % action.node_name
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
            # Create a ssh folder in the root folder of the SD CARD's file system
            cmd = "mkdir fs_dir/root/.ssh"
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
            # Add the public key of the server
            cmd = "cp /root/.ssh/authorized_keys fs_dir/root/.ssh/authorized_keys"
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
        if action.environment.startswith("raspbian"):
            # Create the ssh file in the boot partition to start SSH on startup
            cmd = "touch boot_dir/ssh"
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
            # Avoid the execution of the expand/resize script
            cmd = "sed -i 's:init=.*$::' boot_dir/cmdline.txt"
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
            # Set the hostname to modify the bash prompt
            cmd = "echo '%s' > fs_dir/etc/hostname" % action.node_name
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
            # Create a ssh folder in the root folder of the SD CARD's file system
            cmd = "mkdir fs_dir/root/.ssh"
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
            # Add the public key of the server
            cmd = "cp /root/.ssh/authorized_keys fs_dir/root/.ssh/authorized_keys"
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
        if action.environment == "raspbian_cloud9":
            cmd = "sed -i 's/-a :/-a admin:%s/' fs_dir/etc/systemd/system/cloud9.service" % os_password
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
        if action.environment == "raspbian_ttyd":
            cmd = "sed -i 's/toto/%s/' fs_dir/etc/rc.local" % os_password
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return True


def boot_files_exec(action, db):
    serial = db.query(NodeProperty
        ).filter(NodeProperty.name  == action.node_name
        ).filter(NodeProperty.prop_name == "serial"
        ).first().prop_value
    # Copy boot files to the tftp directory
    tftpboot_node_folder = "/tftpboot/%s" % serial
    # Delete the existing tftp directory
    shutil.rmtree(tftpboot_node_folder)
    # Create an empty tftp directory
    os.mkdir(tftpboot_node_folder)
    cmd = "scp -o 'StrictHostKeyChecking no' -r root@%s:boot_dir/* %s" % (action.node_ip, tftpboot_node_folder)
    subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    # Reboot to initialize the operating system
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username="root",
            timeout=1.0,
            banner_timeout=1.0,
            auth_timeout=1.0)
        (stdin, stdout, stderr) = ssh.exec_command("reboot")
        return_code = stdout.channel.recv_exit_status()
        ssh.close()
        if return_code == 0:
            # Waiting for the node is turned off
            ret = 0
            while ret == 0:
                logging.info("[%s] Waiting lost connection..." % action.node_name)
                time.sleep(1)
                ret = os.system("ping -W 1 -c 1 %s" % action.node_ip)
            return True
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return False


def system_update_exec(action, db):
    update_os = db.query(ActionProperty
        ).filter(ActionProperty.node_name  == action.node_name
        ).filter(ActionProperty.prop_name == "update_os"
        ).first().prop_value
    if update_os == "no":
        # Do not update the operating system
        logging.info("[%s] the OS update is disabled" % action.node_name)
        return True
    # Update the operating system
    ssh_user = db.query(Environment
        ).filter(Environment.name  == action.environment
        ).filter(Environment.prop_name == "ssh_user"
        ).first().prop_value
    if action.environment.startswith("raspbian") or action.environment.startswith("ubuntu"):
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(action.node_ip, username=ssh_user,
                timeout=1.0,
                banner_timeout=1.0,
                auth_timeout=1.0)
            cmd = "apt-get update"
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
            ret_fct = return_code == 0
            if ret_fct:
                cmd = "bash -c 'apt -y dist-upgrade &> /dev/null' &"
                (stdin, stdout, stderr) = ssh.exec_command(cmd)
                return_code = stdout.channel.recv_exit_status()
            else:
                logging.warning("[%s] updating the OS failed" % action.node_name)
            ssh.close()
            return ret_fct
        except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
            logging.warning("[%s] SSH connection failed" % action.node_name)
        return False
    else:
        return True


def system_update_post(action, db):
    update_os = db.query(ActionProperty
        ).filter(ActionProperty.node_name  == action.node_name
        ).filter(ActionProperty.prop_name == "update_os"
        ).first().prop_value
    if update_os == "no":
        # Do not update the operating system
        return True
    # Update the operating system
    ssh_user = db.query(Environment
        ).filter(Environment.name  == action.environment
        ).filter(Environment.prop_name == "ssh_user"
        ).first().prop_value
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username=ssh_user, 
            timeout=1.0,
            banner_timeout=1.0,
            auth_timeout=1.0)
        ret_fct = True
        if ps_ssh(ssh, "'update\|upgrade'") == 0:
            cmd = "rm /boot/bootcode.bin"
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
        else:
            ret_fct = False
        ssh.close()
        return ret_fct
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return False


def boot_update_exec(action, db):
    update_os = db.query(ActionProperty
        ).filter(ActionProperty.node_name  == action.node_name
        ).filter(ActionProperty.prop_name == "update_os"
        ).first().prop_value
    if update_os == "no":
        # The operating system is not updated => do not update the boot files
        return True
    ssh_user = db.query(Environment
        ).filter(Environment.name  == action.environment
        ).filter(Environment.prop_name == "ssh_user"
        ).first().prop_value
    serial = db.query(NodeProperty
        ).filter(NodeProperty.name  == action.node_name
        ).filter(NodeProperty.prop_name == "serial"
        ).first().prop_value
    # Copy boot files to the tftp directory
    tftpboot_node_folder = "/tftpboot/%s" % serial
    # Delete the existing tftp directory
    shutil.rmtree(tftpboot_node_folder)
    # Create an empty tftp directory
    os.mkdir(tftpboot_node_folder)
    cmd = ""
    if action.environment.startswith("ubuntu"):
        cmd = "scp -o 'StrictHostKeyChecking no' -r root@%s:/boot/firmware/* %s" % (action.node_ip, tftpboot_node_folder)
    if action.environment.startswith("raspbian"):
        cmd = "scp -o 'StrictHostKeyChecking no' -r root@%s:/boot/* %s" % (action.node_ip, tftpboot_node_folder)
    if len(cmd) > 0:
        subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(action.node_ip, username=ssh_user,
                timeout=1.0,
                banner_timeout=1.0,
                auth_timeout=1.0)
            cmd = "reboot"
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
            ssh.close()
        except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
            logging.warning("[%s] SSH connection failed" % action.node_name)
    return True


def user_conf_exec(action, db):
    act_prop = row2props(db.query(ActionProperty
        ).filter(ActionProperty.node_name  == action.node_name
        ).filter(ActionProperty.prop_name.in_(["os_password", "ssh_key_1", "ssh_key_2" ])
        ).all())
    ssh_user = db.query(Environment
        ).filter(Environment.name  == action.environment
        ).filter(Environment.prop_name == "ssh_user"
        ).first().prop_value
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username=ssh_user,
            timeout=1.0,
            banner_timeout=1.0,
            auth_timeout=1.0)
        # Get the user SSH key from the DB
        my_ssh_keys = ""
        if "ssh_key_1" in act_prop and len(act_prop["ssh_key_1"]) > 256:
            if len(my_ssh_keys) == 0:
                my_ssh_keys = "%s" % act_prop["ssh_key_1"]
            else:
                my_ssh_keys = "%s\n%s" % (my_ssh_keys, act_prop["ssh_key_1"])
        if "ssh_key_2" in act_prop and len(act_prop["ssh_key_2"]) > 256:
            if len(my_ssh_keys) == 0:
                my_ssh_keys = "%s" % act_prop["ssh_key_2"]
            else:
                my_ssh_keys = "%s\n%s" % (my_ssh_keys, act_prop["ssh_key_2"])
        if len(my_ssh_keys) > 0:
            # Add the public key of the user
            cmd = "echo '%s' >> .ssh/authorized_keys" % my_ssh_keys
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
        if action.environment == "tiny_core":
            # Change the 'tc' user password
            cmd = "echo -e '%s\n%s' | sudo passwd tc; filetool.sh -b" % (
                    act_prop["os_password"], act_prop["os_password"])
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
        if action.environment.startswith("raspbian"):
            # Change the 'pi' user password
            cmd = "echo -e '%s\n%s' | passwd pi" % (act_prop["os_password"], act_prop["os_password"])
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
        ssh.close()
        return True
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return False


# Test if the processus exists on the remote node
def ps_ssh(ssh_session, process):
    try:
        (stdin, stdout, stderr) = ssh_session.exec_command("ps aux | grep %s | grep -v grep | wc -l" % process)
        return_code = stdout.channel.recv_exit_status()
        output = stdout.readlines()
        return int(output[0].strip())
    except SSHException:
        return -1


# Create environment images
def img_part_exec(action, db):
    server = cluster_desc["nodes"][action.node_name]
    environment = cluster_desc["environments"][node.environment]
    logging.info("[%s] create the image '%s'" % (action.node_name, node.temp_info))
    ret_fct = False
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username=environment["ssh_user"], timeout=1.0)
        cmd = "fdisk -l /dev/mmcblk0"
        (stdin, stdout, stderr) = ssh.exec_command(cmd)
        return_code = stdout.channel.recv_exit_status()
        output = stdout.readlines()
        if len(output) > 10:
            logging.error("[%s] more than 2 partitions detected. Can not create another partition!" %
            action.node_name)
            ret_fct = False
        else:
            # Check there is enough free space on the disk
            nb_sectors = int(output[0].split()[-2])
            last_sector = int(output[-1].split()[2])
            # Size of the new partition (size occupied by the filesystem * 1.7 to compress the image) in sectors
            partition_size = int(last_sector * 1.7)
            # We need partition_size available sectors + 20 sectors
            if nb_sectors - last_sector > partition_size + 20:
                # Install the pv tool
                cmd = "apt install pv"
                (stdin, stdout, stderr) = ssh.exec_command(cmd)
                return_code = stdout.channel.recv_exit_status()
                logging.info("[%s] nb sectors: %d, last sector: %d" % (action.node_name, nb_sectors, last_sector))
                # Create a new partition that begins at (last_sector + 20 sectors)
                cmd = ("(echo n; echo ''; echo ''; echo '%d'; echo '+%d'; echo w) | fdisk -u /dev/mmcblk0" %
                        ((last_sector + 20), partition_size))
                (stdin, stdout, stderr) = ssh.exec_command(cmd)
                return_code = stdout.channel.recv_exit_status()
                # Save the end of the user partitions
                node.temp_info = "%s %d" % (node.temp_info, last_sector)
                ret_fct = True
            else:
                logging.error("[%s] no enough space on the disk. Only %d sectors available, %d required sectors" %
                        (action.node_name, (nb_sectors - last_sector), partition_size))
        ssh.close()
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return ret_fct


def img_format_exec(action, db):
    server = cluster_desc["nodes"][action.node_name]
    environment = cluster_desc["environments"][node.environment]
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username=environment["ssh_user"], timeout=1.0)
        cmd = "partprobe"
        (stdin, stdout, stderr) = ssh.exec_command(cmd)
        return_code = stdout.channel.recv_exit_status()
        # Get the name of the new partition
        cmd = "ls /dev/mmcblk0p*"
        (stdin, stdout, stderr) = ssh.exec_command(cmd)
        return_code = stdout.channel.recv_exit_status()
        output = stdout.readlines()
        partition_name = output[-1].strip()
        logging.info("[%s] format the partition '%s'" % (action.node_name, partition_name))
        cmd = "mkfs.ext4 %s &> /dev/null &" % partition_name
        (stdin, stdout, stderr) = ssh.exec_command(cmd)
        return_code = stdout.channel.recv_exit_status()
        ssh.close()
        # Save the partition name
        node.temp_info = "%s %s" % (node.temp_info, partition_name)
        return True
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return False


def img_copy_exec(action, db):
    server = cluster_desc["nodes"][action.node_name]
    environment = cluster_desc["environments"][node.environment]
    ret_fct = False
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username=environment["ssh_user"], timeout=1.0)
        end_partition = int(node.temp_info.split()[1])
        partition_name = node.temp_info.split()[2]
        cmd = "mkdir img_part"
        (stdin, stdout, stderr) = ssh.exec_command(cmd)
        return_code = stdout.channel.recv_exit_status()
        if ps_ssh(ssh, partition_name) == 0:
            # Check if the new partition is mounted
            cmd = "mount -f | grep img_part"
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
            output = stdout.readlines()
            mounted_part = len(output) > 0
            if not mounted_part:
                logging.info("[%s] mount the partition '%s'" % (action.node_name, partition_name))
                cmd = "mount %s img_part" % partition_name
                (stdin, stdout, stderr) = ssh.exec_command(cmd)
                return_code = stdout.channel.recv_exit_status()
                mounted_part = return_code == 0
            if mounted_part:
                # Write the size of the uncompressed image file to the JSON
                env_name = node.temp_info.split()[0]
                env_file_path = cluster_desc["env_cfg_dir"] + env_name + ".json"
                with open(env_file_path, "r") as jsonfile:
                    env_data = json.load(jsonfile)
                env_data["img_size"] = (end_partition + 10) * 512
                with open(env_file_path, "w") as jsonfile:
                    json.dump(env_data, jsonfile)
                # Copy the system to the image file
                cmd = "dd if=/dev/mmcblk0 of=img_part/%s.img bs=512 count=%d &" % (env_name, end_partition + 10)
                (stdin, stdout, stderr) = ssh.exec_command(cmd)
                return_code = stdout.channel.recv_exit_status()
                ret_fct = True
                node.temp_info = 0
            else:
                logging.error("[%s] can not mount the partition '%s'" % (action.node_name, partition_name))
        ssh.close()
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return ret_fct

def img_copy_check_exec(action, db):
    server = cluster_desc["nodes"][action.node_name]
    environment = cluster_desc["environments"][node.environment]
    ret_fct = False
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username=environment["ssh_user"], timeout=1.0)
        if ps_ssh(ssh, "count") == 0:
            ret_fct = True
        else:
            cmd = f"tail -n 1 progress-{server['name']}.txt"
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
            output = stdout.readlines()
            if len(output) == 0:
                logging.warning("[%s] no progress value for the running image copy" % action.node_name)
                updated = datetime.datetime.strptime(str(node.updated_at), "%Y-%m-%d %H:%M:%S")
                elapsedTime = (datetime.datetime.now() - updated).total_seconds()
                # Compute the progress value with an assumed transfert rate of 8 MB/s
                percent = elapsedTime * 8000000 * 100 / environment["img_size"]
            else:
                percent = output[0].strip()
            node.temp_info = int(percent)
        ssh.close()
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return ret_fct


def img_customize_exec(action, db):
    server = cluster_desc["nodes"][action.node_name]
    environment = cluster_desc["environments"][node.environment]
    ret_fct = False
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username=environment["ssh_user"], timeout=1.0)
        # Get a free loop device
        cmd = "losetup -f"
        (stdin, stdout, stderr) = ssh.exec_command(cmd)
        return_code = stdout.channel.recv_exit_status()
        loop_device = stdout.readlines()[0].strip()
        logging.info("[%s] delete the third partition by using '%s'" % (action.node_name, loop_device))
        # Link the .img file to the loop device
        cmd = "cd img_part; losetup -P %s *.img" % loop_device
        (stdin, stdout, stderr) = ssh.exec_command(cmd)
        return_code = stdout.channel.recv_exit_status()
        # Delete the third partition
        cmd = "(echo d; echo 3; echo w) | fdisk -u %s" % loop_device
        (stdin, stdout, stderr) = ssh.exec_command(cmd)
        return_code = stdout.channel.recv_exit_status()
        # Release the loop device
        cmd = "losetup -d %s" % loop_device
        (stdin, stdout, stderr) = ssh.exec_command(cmd)
        return_code = stdout.channel.recv_exit_status()
        ret_fct = True
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return ret_fct


def img_compress_exec(action, db):
    server = cluster_desc["nodes"][action.node_name]
    environment = cluster_desc["environments"][node.environment]
    ret_fct = False
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username=environment["ssh_user"], timeout=1.0)
        # Compress the filesystem image
        cmd = "cd img_part; gzip -9 *.img &"
        (stdin, stdout, stderr) = ssh.exec_command(cmd)
        return_code = stdout.channel.recv_exit_status()
        ssh.close()
        ret_fct = True
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return ret_fct


def img_compress_check_exec(action, db):
    server = cluster_desc["nodes"][action.node_name]
    environment = cluster_desc["environments"][node.environment]
    ret_fct = False
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username=environment["ssh_user"], timeout=1.0)
        if ps_ssh(ssh, "gzip") == 0:
            # Get both the filename and the size of the compressed image file
            cmd = "rm -f progress-{server['name']}.txt; cd img_part; ls -l *.img.gz | tail -n 1"
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
            output = stdout.readlines()[0].split()
            node.temp_info = "%s %s" % (output[-1], output[4])
            ret_fct = True
        else:
            cmd = f"tail -n 1 progress-{server['name']}.txt"
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
            output = stdout.readlines()
            if len(output) == 0:
                logging.warning("[%s] no progress value for the running image compression" % action.node_name)
                updated = datetime.datetime.strptime(str(node.updated_at), "%Y-%m-%d %H:%M:%S")
                elapsedTime = (datetime.datetime.now() - updated).total_seconds()
                # Compute the progress value from past experiments on Pi 3B+
                percent = elapsedTime * 1500000 * 100 / environment["img_size"]
            else:
                percent = output[0].strip()
            node.temp_info = percent
        ssh.close()
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return ret_fct


def upload_exec(action, db):
    server = cluster_desc["nodes"][action.node_name]
    environment = cluster_desc["environments"][node.environment]
    img_name = node.temp_info.split()[0]
    img_size = node.temp_info.split()[1]
    logging.info("[%s] upload the file '%s'" % (action.node_name, img_name))
    cmd = f"""rsh -o "StrictHostKeyChecking no" %s@%s "cat img_part/%s" | \
                pv -n -p -s %s 2> /tmp/progress-{action.node_name}.txt | dd of=%s%s bs=4M conv=fsync &""" % (
                        environment["ssh_user"], action.node_ip, img_name, img_size,
                        cluster_desc["img_dir"], img_name)
    subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    node.temp_info = 0
    return True


def upload_check_exec(action, db):
    server = cluster_desc["nodes"][action.node_name]
    environment = cluster_desc["environments"][node.environment]
    cmd = "ps aux | grep %s | grep -v grep | wc -l" % action.node_ip
    process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    nb_line = process.stdout.decode("utf-8").strip()
    if int(nb_line) == 0:
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(action.node_ip, username=environment["ssh_user"], timeout=1.0)
            cmd = f"rm progress-{server['name']}.txt"
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
            ssh.close()
            # Load the new environement to the cluster description
            load_cluster_desc()
            return True
        except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
            logging.warning("[%s] SSH connection failed" % action.node_name)
    else:
        cmd = f"tail -n 1 /tmp/progress-{server['name']}.txt"
        process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        output = process.stdout.decode("utf-8").strip()
        if len(output) == 0:
            logging.warning("[%s] no progress value for the running environment copy" % action.node_name)
            updated = datetime.datetime.strptime(str(node.updated_at), "%Y-%m-%d %H:%M:%S")
            elapsedTime = (datetime.datetime.now() - updated).total_seconds()
            # Compute the progress value with an assumed transfert rate of 8 MB/s
            percent = elapsedTime * 8000000 * 100 / environment["img_size"]
        else:
            percent = output
        node.temp_info = int(percent)
    return False


# Hard Reboot nodes (turn_off -> turn_on -> coming_back)
def coming_back_exec(action, db):
    server = cluster_desc["nodes"][action.node_name]
    environment = cluster_desc["environments"][node.environment]
    try:
        # Test the SSH connection
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if use_env_ssh_user(node.temp_info):
            ssh.connect(action.node_ip, username=environment["ssh_user"], timeout=1.0)
        else:
            ssh.connect(action.node_ip, username="root", timeout=1.0)
        ssh.close()
        # Come back to the deployment process
        node.process = "deploy"
        node.state = node.temp_info
        node.temp_info = None
        # Always return False to avoid rewriting the node.state property
        return False
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return False


# Destroying deployments
def destroying_exec(action, db):
    server = cluster_desc["nodes"][action.node_name]
    # When destroying initialized deployments, the environment is unset
    if node.environment is not None:
        environment = cluster_desc["environments"][node.environment]
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if server["model"] == "RPI3Bplus":
            # Delete the bootcode.bin file
            try:
                # Try to connect to the deployed environment
                ssh.connect(action.node_ip, username=environment["ssh_user"], timeout=1.0)
                (stdin, stdout, stderr) = ssh.exec_command("rm -f /boot/bootcode.bin")
                return_code = stdout.channel.recv_exit_status()
                ssh.close()
            except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
                logging.info("[%s] can not connect to the deployed environment" % action.node_name)
                try:
                    # Try to connect to the nfs environment
                    ssh.connect(action.node_ip, username="root", timeout=1.0)
                    cmd = "mount /dev/mmcblk0p1 boot_dir"
                    (stdin, stdout, stderr) = ssh.exec_command(cmd)
                    return_code = stdout.channel.recv_exit_status()
                    (stdin, stdout, stderr) = ssh.exec_command("rm -f boot_dir/bootcode.bin")
                    return_code = stdout.channel.recv_exit_status()
                    ssh.close()
                except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
                    logging.info("[%s] can not connect to the NFS environment" % action.node_name)
        if server["model"] == "RPI4B" and environment["name"].startswith("raspbian"):
            # Check the booloader configuration (netboot)
            try:
                # Try to connect to the deployed environment
                ssh.connect(action.node_ip, username=environment["ssh_user"], timeout=1.0)
                # Check the booted system is the NFS system
                (stdin, stdout, stderr) = ssh.exec_command("cat /etc/hostname")
                return_code = stdout.channel.recv_exit_status()
                myname = stdout.readlines()[0].strip()
                if myname != "nfspi":
                    (stdin, stdout, stderr) = ssh.exec_command("rpi-eeprom-config | grep BOOT_ORDER")
                    return_code = stdout.channel.recv_exit_status()
                    output = stdout.readlines()
                    if len(output) > 0 and output[0].strip() != "BOOT_ORDER=0x2":
                        logging.error("[%s] wrong boot order value. Please update the EEPROM config!" % action.node_name)
                        return False
                else:
                    logging.info("[%s] Destroy a node running on the NFS system." % action.node_name)
                ssh.close()
            except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
                logging.info("[%s] can not connect to the deployed environment" % action.node_name)
    # Delete the tftpboot folder
    tftpboot_node_folder = "/tftpboot/%s" % server["id"]
    if os.path.isdir(tftpboot_node_folder):
        shutil.rmtree(tftpboot_node_folder)
    return True
