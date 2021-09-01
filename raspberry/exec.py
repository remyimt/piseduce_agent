from database.tables import ActionProperty, RaspNode, RaspEnvironment
from datetime import datetime
from glob import glob
from lib.config_loader import get_config
from lib.switch_snmp import turn_on_port, turn_off_port
from paramiko.ssh_exception import BadHostKeyException, AuthenticationException, SSHException
from raspberry.states import SSH_IDX
import logging, os, paramiko, random, shutil, socket, string, subprocess, time

# SSH timeout in seconds
SSH_TIMEOUT = 3

# Test if the processus exists on the remote node
def ps_ssh(ssh_session, process):
    try:
        (stdin, stdout, stderr) = ssh_session.exec_command("ps aux | grep %s | grep -v grep | wc -l" % process)
        return_code = stdout.channel.recv_exit_status()
        output = stdout.readlines()
        return int(output[0].strip())
    except SSHException:
        return -1


# Generate a random string of letters and digits
def new_password(stringLength=8):
    lettersAndDigits = string.ascii_letters + string.digits
    return ''.join(random.choice(lettersAndDigits) for i in range(stringLength))


# States of the 'deploy' process (deploy environments)
def boot_conf_exec(action, db):
    serial = db.query(RaspNode).filter(RaspNode.name  == action.node_name).first().serial
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
    node = db.query(RaspNode).filter(RaspNode.name  == action.node_name).first()
    # Turn off port
    turn_off_port(node.switch, node.port_number)
    return True


def turn_on_exec(action, db):
    node = db.query(RaspNode).filter(RaspNode.name  == action.node_name).first()
    # Turn on port
    turn_on_port(node.switch, node.port_number)
    return True


def turn_on_post(action, db):
    node_ip = db.query(RaspNode).filter(RaspNode.name  == action.node_name).first().ip
    cmd = "ping -W 1 -c 1 %s" % node_ip
    subproc = subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return subproc.returncode == 0


def ssh_test_post(action, db):
    node_ip = db.query(RaspNode).filter(RaspNode.name  == action.node_name).first().ip
    # By default, we use the ssh_user of the environment. We assume the environment is deployed
    ssh_user = db.query(RaspEnvironment).filter(RaspEnvironment.name == action.environment).first().ssh_user
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
        ssh.connect(node_ip, username = ssh_user, timeout = SSH_TIMEOUT)
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
    node_ip = db.query(RaspNode).filter(RaspNode.name == action.node_name).first().ip
    # WARN: the pimaster SSH user is in pimaster.switch (sorry)
    pimaster = db.query(RaspNode).filter(RaspNode.name == "pimaster").first()
    env = db.query(RaspEnvironment).filter(RaspEnvironment.name  == action.environment).first()
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(node_ip, username = "root", timeout = SSH_TIMEOUT)
        # Get the path to the IMG file
        img_path = env_path + env.img_name
        logging.info("[%s] copy %s to the SDCARD" % (action.node_name, img_path))
        # Write the image of the environment on SD card
        deploy_cmd = "rsh -o StrictHostKeyChecking=no %s@%s 'cat %s' | tar xzOf - | \
            pv -n -p -s %s 2> progress-%s.txt | dd of=/dev/mmcblk0 bs=4M conv=fsync &" % (
            pimaster.switch, pimaster.ip, img_path, env.img_size, action.node_name)
        (stdin, stdout, stderr) = ssh.exec_command(deploy_cmd)
        return_code = stdout.channel.recv_exit_status()
        ssh.close()
        act_prop = db.query(ActionProperty
            ).filter(ActionProperty.node_name == action.node_name
            ).filter(ActionProperty.prop_name == "percent").first()
        if act_prop is None:
            owner_email = db.query(ActionProperty
                ).filter(ActionProperty.node_name == action.node_name
                ).first().owner
            act_prop = ActionProperty()
            act_prop.node_name = action.node_name
            act_prop.prop_name = "percent"
            act_prop.prop_value = 0
            act_prop.owner = owner_email
            db.add(act_prop)
        else:
            act_prop.prop_value = 0
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return True


def env_copy_post(action, db):
    ret_fct = False
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username = "root", timeout = SSH_TIMEOUT)
        if ps_ssh(ssh, "mmcblk0") > 0:
            ret_fct = True
        ssh.close()
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return ret_fct


def env_check_exec(action, db):
    ret_fct = False
    img_size = db.query(RaspEnvironment).filter(RaspEnvironment.name  == action.environment).first().img_size
    percent_prop = db.query(ActionProperty
        ).filter(ActionProperty.node_name  == action.node_name
        ).filter(ActionProperty.prop_name  == "percent"
        ).first()
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username = "root", timeout = SSH_TIMEOUT)
        if ps_ssh(ssh, "mmcblk0") == 0:
            ret_fct = True
        else:
            cmd = "tail -n 1 progress-%s.txt" % action.node_name
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
            output = stdout.readlines()
            if len(output) == 0:
                logging.warning("%s: no progress value for the running environment copy" % action.node_name)
                elapsedTime = int(time.time()) - action.updated_at
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
        ssh.connect(action.node_ip, username = "root", timeout = SSH_TIMEOUT)
        # Register the size of the existing partition
        cmd = "rm progress-%s.txt; fdisk -l /dev/mmcblk0" % action.node_name
        (stdin, stdout, stderr) = ssh.exec_command(cmd)
        return_code = stdout.channel.recv_exit_status()
        output = stdout.readlines()
        if "mmcblk0p2" in output[-1]:
            # Delete the second partition
            cmd = "(echo d; echo 2; echo w) | fdisk -u /dev/mmcblk0"
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
            ssh.close()
        else:
            logging.info("[%s] No second partition detected" % action.node_name)
        return True
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return False


def create_partition_exec(action, db):
    sector_start = db.query(RaspEnvironment
        ).filter(RaspEnvironment.name  == action.environment).first().sector_start
    size_str = db.query(ActionProperty
        ).filter(ActionProperty.node_name  == action.node_name
        ).filter(ActionProperty.prop_name == "part_size"
        ).first().prop_value
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username = "root", timeout = SSH_TIMEOUT)
        if "gb" in size_str:
            part_size = int(size_str.replace("gb", "")) * 1024
            # Total size of the new partition in sectors (512B)
            part_size = part_size * 1024 * 1024 / 512
            logging.info("[%s] create a partition with a size of %d sectors" % (action.node_name, part_size))
            cmd = ("(echo n; echo p; echo 2; echo '%s'; echo '+%d'; echo w) | fdisk -u /dev/mmcblk0" %
                (sector_start, part_size))
        else:
            logging.info("[%s] create a partition with the whole free space" % action.node_name)
            cmd = ("(echo n; echo p; echo 2; echo '%s'; echo ''; echo w) | fdisk -u /dev/mmcblk0" % sector_start)
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
        ssh.connect(action.node_ip, username = "root", timeout = SSH_TIMEOUT)
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
        ssh.connect(action.node_ip, username = "root", timeout = SSH_TIMEOUT)
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
        if action.environment.startswith("ubuntu"):
            cmd = "rm boot_dir/firmware/bootcode.bin && sync"
        else:
            cmd = "rm boot_dir/bootcode.bin && sync"
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
        ssh.connect(action.node_ip, username = "root", timeout = SSH_TIMEOUT)
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
        ssh.connect(action.node_ip, username = "root", timeout = SSH_TIMEOUT)
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
        ssh.connect(action.node_ip, username = "root", timeout = SSH_TIMEOUT)
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
        owner_email = db.query(ActionProperty
            ).filter(ActionProperty.node_name == action.node_name
            ).first().owner
        act_prop = ActionProperty()
        act_prop.node_name = action.node_name
        act_prop.prop_name = "os_password"
        act_prop.prop_value = os_password
        act_prop.owner = owner_email
        db.add(act_prop)
    else:
        os_password = pwd.prop_value
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username = "root", timeout = SSH_TIMEOUT)
        if action.environment.startswith("picore"):
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
    serial = db.query(RaspNode).filter(RaspNode.name  == action.node_name).first().serial
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
        ssh.connect(action.node_ip, username = "root", timeout = SSH_TIMEOUT)
        (stdin, stdout, stderr) = ssh.exec_command("reboot")
        return_code = stdout.channel.recv_exit_status()
        ssh.close()
        # Waiting for the node is turned off
        ret = 0
        max_attempt = 10
        while ret == 0 and max_attempt > 0:
            max_attempt -= 1
            logging.info("[%s] Waiting lost connection..." % action.node_name)
            time.sleep(3)
            ret = os.system("ping -W 1 -c 1 %s" % action.node_ip)
        return ret != 0
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.exception("[%s] SSH connection failed" % action.node_name)
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
    ssh_user = db.query(RaspEnvironment).filter(RaspEnvironment.name  == action.environment).first().ssh_user
    if action.environment.startswith("raspbian") or action.environment.startswith("ubuntu"):
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(action.node_ip, username = ssh_user, timeout = SSH_TIMEOUT)
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
    ssh_user = db.query(RaspEnvironment).filter(RaspEnvironment.name  == action.environment).first().ssh_user
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username = ssh_user, timeout = SSH_TIMEOUT)
        ret_fct = True
        if ps_ssh(ssh, "'update\|upgrade'") == 0:
            if action.environment.startswith("ubuntu"):
                cmd = "rm /boot/firmware/bootcode.bin"
            else:
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
    ssh_user = db.query(RaspEnvironment).filter(RaspEnvironment.name  == action.environment).first().ssh_user
    serial = db.query(RaspNode).filter(RaspNode.name  == action.node_name).first().serial
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
            ssh.connect(action.node_ip, username = ssh_user, timeout = SSH_TIMEOUT)
            cmd = "reboot"
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
            ssh.close()
        except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
            logging.warning("[%s] SSH connection failed" % action.node_name)
    return True


def user_conf_exec(action, db):
    act_prop = db.query(ActionProperty
        ).filter(ActionProperty.node_name  == action.node_name
        ).filter(ActionProperty.prop_name.in_(["os_password", "form_ssh_key", "account_ssh_key" ])
        ).all()
    os_password = None
    form_ssh_key = None
    account_ssh_key = None
    for prop in act_prop:
        if prop.prop_name == "os_password":
            os_password = prop.prop_value
        elif prop.prop_name == "form_ssh_key":
            form_ssh_key = prop.prop_value
        elif prop.prop_name == "account_ssh_key":
            account_ssh_key = prop.prop_value
    ssh_user = db.query(RaspEnvironment).filter(RaspEnvironment.name  == action.environment).first().ssh_user
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username = ssh_user, timeout = SSH_TIMEOUT)
        # Get the user SSH key from the DB
        my_ssh_keys = ""
        # Copy the SSH key provided in the configuring form
        if form_ssh_key is not None and len(form_ssh_key) > 256:
            my_ssh_keys = form_ssh_key
        if account_ssh_key is not None and len(account_ssh_key) > 256:
            if len(my_ssh_keys) == 0:
                my_ssh_keys = account_ssh_key
            else:
                my_ssh_keys = "%s\n%s" % (my_ssh_keys, account_ssh_key)
        if len(my_ssh_keys) > 0:
            # Add the public key of the user
            cmd = "echo '%s' >> .ssh/authorized_keys" % my_ssh_keys
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
        if action.environment.startswith("picore"):
            # Change the 'tc' user password
            cmd = "echo -e '%s\n%s' | sudo passwd tc; filetool.sh -b" % (os_password, os_password)
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
        if action.environment.startswith("raspbian"):
            # Change the 'pi' user password
            cmd = "echo -e '%s\n%s' | passwd pi" % (os_password, os_password)
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
        ssh.close()
        return True
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return False


# Destroying deployments
def destroying_exec(action, db):
    node = db.query(RaspNode).filter(RaspNode.name  == action.node_name).first()
    if action.environment is not None:
        ssh_user = db.query(RaspEnvironment).filter(RaspEnvironment.name  == action.environment).first().ssh_user
        # When destroying initialized deployments, the environment is unset
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if node.model.startswith("RPI3"):
            # Delete the bootcode.bin file
            try:
                cmd = "rm /boot/bootcode.bin && sync"
                if action.environment.startswith("ubuntu"):
                    cmd = "rm /boot/firmware/bootcode.bin && sync"
                # Try to connect to the deployed environment
                ssh.connect(action.node_ip, username = ssh_user, timeout = SSH_TIMEOUT)
                (stdin, stdout, stderr) = ssh.exec_command(cmd)
                return_code = stdout.channel.recv_exit_status()
                ssh.close()
            except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
                logging.info("[%s] can not connect to the deployed environment" % action.node_name)
                try:
                    # Try to connect to the nfs environment
                    ssh.connect(action.node_ip, username = "root", timeout = SSH_TIMEOUT)
                    cmd = "mount /dev/mmcblk0p1 boot_dir"
                    (stdin, stdout, stderr) = ssh.exec_command(cmd)
                    return_code = stdout.channel.recv_exit_status()
                    cmd = "rm boot_dir/bootcode.bin && sync"
                    if action.environment.startswith("ubuntu"):
                        cmd = "rm boot_dir/firmware/bootcode.bin && sync"
                    (stdin, stdout, stderr) = ssh.exec_command(cmd)
                    return_code = stdout.channel.recv_exit_status()
                    ssh.close()
                except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
                    logging.info("[%s] can not connect to the NFS environment" % action.node_name)
        if node.model.startswith("RPI4"):
            # Check the booloader configuration (netboot)
            try:
                # Try to connect to the deployed environment
                ssh.connect(action.node_ip, username = ssh_user, timeout = SSH_TIMEOUT)
                # Check the booted system is the NFS system
                (stdin, stdout, stderr) = ssh.exec_command("cat /etc/hostname")
                return_code = stdout.channel.recv_exit_status()
                myname = stdout.readlines()[0].strip()
                if myname == "nfspi":
                    logging.info("[%s] Destroy a node running on the NFS system." % action.node_name)
                else:
                    (stdin, stdout, stderr) = ssh.exec_command("rpi-eeprom-config | grep BOOT_ORDER")
                    return_code = stdout.channel.recv_exit_status()
                    output = stdout.readlines()
                    if len(output) > 0:
                        boot_order_line = output[0].strip()
                        if boot_order_line.startswith("BOOT_ORDER") and boot_order_line[-1] != "2":
                            logging.error("[%s] wrong boot order value. Please update the EEPROM config!" % action.node_name)
                            return False
                    else:
                        logging.info("[%s] No boot_order line (environment: %s)" % (action.node_name, action.environment))
                ssh.close()
            except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
                logging.info("[%s] can not connect to the deployed environment" % action.node_name)
    # Delete the tftpboot folder
    tftpboot_node_folder = "/tftpboot/%s" % node.serial
    if os.path.isdir(tftpboot_node_folder):
        shutil.rmtree(tftpboot_node_folder)
    return True


# Register environments
def uncompress_exec(action, db):
    ssh_user = db.query(RaspEnvironment).filter(RaspEnvironment.name == action.environment).first().ssh_user
    img_path = db.query(ActionProperty
        ).filter(ActionProperty.node_name == action.node_name
        ).filter(ActionProperty.prop_name == "img_path").first().prop_value
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username = ssh_user, timeout = SSH_TIMEOUT)
        cmd = "ls -l %s" % img_path
        (stdin, stdout, stderr) = ssh.exec_command(cmd)
        return_code = stdout.channel.recv_exit_status()
        if len(stdout.readlines()) == 0:
            logging.error("[%s] No file '%s'" % (action.node_name, img_path))
            return False
        cmd = "rm new_env.img; tar xf %s &" % img_path
        logging.error(cmd)
        (stdin, stdout, stderr) = ssh.exec_command(cmd)
        return_code = stdout.channel.recv_exit_status()
        ssh.close()
        return True
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return False


def uncompress_post(action, db):
    ssh_user = db.query(RaspEnvironment).filter(RaspEnvironment.name == action.environment).first().ssh_user
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username = ssh_user, timeout = SSH_TIMEOUT)
        ret_fct = False
        if ps_ssh(ssh, "tar") == 0:
            ret_fct = True
        ssh.close()
        return ret_fct
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return False

def read_info_exec(action, db):
    ssh_user = db.query(RaspEnvironment).filter(RaspEnvironment.name == action.environment).first().ssh_user
    act_prop = db.query(ActionProperty
        ).filter(ActionProperty.node_name == action.node_name
        ).filter(ActionProperty.prop_name.in_(["img_path", "env_name"])
        ).all()
    for prop in act_prop:
        if prop.prop_name == "img_path":
            img_path = prop.prop_value
        else:
            env_name = prop
    file_name = os.path.basename(img_path)
    img_name = file_name.replace(".tar.gz", "")
    prefix = None
    ssh_user = None
    img_size = None
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username = ssh_user, timeout = SSH_TIMEOUT)
        # Retrieve the first sector number of the second partition
        cmd = "fdisk -l %s" % img_name
        (stdin, stdout, stderr) = ssh.exec_command(cmd)
        return_code = stdout.channel.recv_exit_status()
        output = stdout.readlines()
        if len(output) > 9:
            first_sector = int(output[9].split()[1])
            if first_sector in [ 532480 ]:
                prefix = "raspbian_"
                ssh_user = "root"
            elif first_sector in [ 526336 ]:
                prefix = "ubuntu_"
                ssh_user = "root"
            elif first_sector in [ 195693 ]:
                prefix = "picore_"
                ssh_user = "tc"
            else:
                logging.error("[%s] Unknown sector_start %d" % (action.node_name, sector_start))
        else:
            logging.error("[%s] Wrong fdisk output '%s'" % (action.node_name, output))
        if prefix is None:
            return False
        else:
            # Retrieve the size of the uncompressed image
            cmd = "ls -l %s" % img_name
            (stdin, stdout, stderr) = ssh.exec_command(cmd)
            return_code = stdout.channel.recv_exit_status()
            output = stdout.readlines()[0].split()
            if len(output) > 7:
                img_size = int(output[4])
                # Add the environment to the database
                new_env_name = "%s%s" % (prefix, env_name.prop_value)
                env_name.prop_value = new_env_name
                existing = db.query(RaspEnvironment).filter(RaspEnvironment.name == new_env_name).first()
                if existing is not None:
                    db.delete(existing)
                new_env = RaspEnvironment()
                new_env.name = new_env_name
                new_env.img_name = file_name
                new_env.img_size = img_size
                new_env.sector_start = first_sector
                new_env.ssh_user = ssh_user
                new_env.web = False
                new_env.state = "uploading"
                db.add(new_env)
                return True
            else:
                logging.error("[%s] wrong 'ls' return: can not read the image size" % action.node_name)
                return False
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return False


def img_upload_exec(action, db):
    ssh_user = db.query(RaspEnvironment).filter(RaspEnvironment.name == action.environment).first().ssh_user
    env_path = get_config()["env_path"]
    img_path = db.query(ActionProperty
        ).filter(ActionProperty.node_name == action.node_name
        ).filter(ActionProperty.prop_name == "img_path").first().prop_value
    cmd = "scp -o 'StrictHostKeyChecking no' root@%s:%s %s" % (action.node_ip, img_path, env_path)
    subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return True


def img_upload_post(action, db):
    ssh_user = db.query(RaspEnvironment).filter(RaspEnvironment.name == action.environment).first().ssh_user
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(action.node_ip, username = ssh_user, timeout = SSH_TIMEOUT)
        ret_fct = False
        if ps_ssh(ssh, "scp") == 0:
            # Delete the action properties
            act_prop = db.query(ActionProperty
                ).filter(ActionProperty.node_name == action.node_name
                ).filter(ActionProperty.prop_name.in_(["img_path", "env_name"])
                ).all()
            for prop in act_prop:
                if prop.prop_name == "env_name":
                    env_name = prop.prop_value
                db.delete(prop)
            # Update the environment state
            env = db.query(RaspEnvironment).filter(RaspEnvironment.name == env_name).first()
            env.state = "available"
            ret_fct = True
        ssh.close()
        return ret_fct
    except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
        logging.warning("[%s] SSH connection failed" % action.node_name)
    return False
