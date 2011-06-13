#!/usr/bin/env python
### Script provided by DataStax.

import urllib2, os, re, shlex, subprocess, time, sys, glob, random, time, traceback
from optparse import OptionParser

# Full path must be used since this script will execute at
# startup as no root user
confPath = os.path.expanduser("/home/ubuntu/brisk/resources/cassandra/conf/")
opsConfPath = os.path.expanduser("/etc/opscenter/")

isDEV = False

clusterseed = 0
opscenterseed = 0

internalip = 0
publichostname = 0
launchindex = 0
reservationid = False
clustername = False
jmxPass = False
userdata = False
clusterlist = []
options = False

tokens = {}

def exe(command, log=True):
    # Helper function to execute commands and print traces of the command and output for debugging/logging purposes
    process = subprocess.Popen(shlex.split(command), stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    read = process.communicate()
    
    if log:
        # Print output on next line if it exists
        if len(read[0]) > 0:
            print '[EXEC] ' + time.strftime("%m/%d/%y-%H:%M:%S", time.localtime()) + ' ' + command + ":\n" + read[0]
        elif len(read[1]) > 0:
            print '[ERROR] ' + time.strftime("%m/%d/%y-%H:%M:%S", time.localtime()) + ' ' + command + ":\n" + read[1]
    
    if not log or (len(read[0]) == 0 and len(read[1]) == 0):
        print '[EXEC] ' + time.strftime("%m/%d/%y-%H:%M:%S", time.localtime()) + ' ' + command
    
    return process

def pipe(command1, command2, log=True):
    # Helper function to execute piping commands and print traces of the commands and output for debugging/logging purposes
    p1 = subprocess.Popen(shlex.split(command1), stdout=subprocess.PIPE)
    p2 = subprocess.Popen(shlex.split(command2), stdin=p1.stdout, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    p1.stdout.close()  # Allow p1 to receive a SIGPIPE if p2 exits.
    read = p2.stdout.read()

    
    if not log:
        read = ""
    
    # Print output on next line if it exists
    if len(read) > 0:
        print '[PIPE] ' + time.strftime("%m/%d/%y-%H:%M:%S", time.localtime()) + ' ' + command1 + ' | ' + command2 + ":\n" + read
    else:
        print '[PIPE] ' + time.strftime("%m/%d/%y-%H:%M:%S", time.localtime()) + ' ' + command1 + ' | ' + command2

    output = p2.communicate()[0]
    
    if log:
        if output and len(output) > 0 and len(output[0]) > 0:
            print '[PIPE] ' + time.strftime("%m/%d/%y-%H:%M:%S", time.localtime()) + ' ' + command1 + ' | ' + command2 + ":\n" + output[0]
        elif output and len(output) > 1 and len(output[1] > 0):
            print '[PIPE] ' + time.strftime("%m/%d/%y-%H:%M:%S", time.localtime()) + ' ' + command1 + ' | ' + command2 + ":\n" + output[1]

        return output


def clearMOTD():
    # To clear the default MOTD
    exe('sudo rm -rf /etc/motd')
    exe('sudo touch /etc/motd')

def getAddresses():
    # Find internal IP address for seed list
    global internalip
    req = urllib2.Request('http://instance-data/latest/meta-data/local-ipv4')
    internalip = urllib2.urlopen(req).read()
    
    # Find public hostname for JMX
    req = urllib2.Request('http://instance-data/latest/meta-data/public-hostname')
    global publichostname
    publichostname = urllib2.urlopen(req).read()
    
    # Find launch index for token splitting
    req = urllib2.Request('http://instance-data/latest/meta-data/ami-launch-index')
    global launchindex
    launchindex = int(urllib2.urlopen(req).read())
    
    # Find reservation-id for cluster-id and jmxpass
    req = urllib2.Request('http://instance-data/latest/meta-data/reservation-id')
    global reservationid, jmxPass, clustername
    reservationid = urllib2.urlopen(req).read()
    jmxPass = reservationid
    clustername = reservationid

    # Try to get EC2 User Data
    try:
        req = urllib2.Request('http://instance-data/latest/user-data/')
        global userdata
        userdata = urllib2.urlopen(req).read()
        
        # Setup parser
        parser = OptionParser()

        # DEV OPTIONS
        # Dev switch
        parser.add_option("-d", "--dev", action="store_true", dest="dev")
        # Developmental option that allows for a pull from the developmental branch
        parser.add_option("-t", "--anttests", action="store_true", dest="anttests")
        # Developmental option that allows for a pull from the developmental branch
        parser.add_option("-u", "--smokeurl", action="store", type="string", dest="smokeurl")
        # Developmental option that allows for a pull from the developmental branch
        parser.add_option("-f", "--smokefile", action="store", type="string", dest="smokefile")
        # Option that specifies how the ring will be divided
        parser.add_option("-b", "--branch", action="store", type="string", dest="branch")
        # Developmental option that allows for a pull from the developmental branch of the AMI codebase
        parser.add_option("-a", "--amibranch", action="store", type="string", dest="amibranch")
        
        # Developmental option that allows for an emailed report of the startup diagnostics
        parser.add_option("-e", "--email", action="store", type="string", dest="email")
        # Option that specifies how the ring will be divided
        parser.add_option("-n", "--clustername", action="store", type="string", dest="clustername")
        # Option that specifies how the ring will be divided
        parser.add_option("-p", "--paidopscenter", action="store", type="string", dest="paidopscenter")
        # Option that specifies how the ring will be divided
        parser.add_option("-o", "--opscenter", action="store", type="string", dest="opscenter")
        # Option that specifies how the ring will be divided
        parser.add_option("-c", "--cfsreplication", action="store", type="string", dest="cfsreplication")
        # Option that specifies how the ring will be divided
        parser.add_option("-v", "--vanillanodes", action="store", type="string", dest="vanillanodes")
        # Option that specifies how the ring will be divided
        parser.add_option("-s", "--clustersize", action="store", type="string", dest="clustersize")
        # # Option that specifies an alternative reflector.php
        # parser.add_option("-r", "--reflector", action="store", type="string", dest="reflector")
        # Developmental option that allows for a non-interactive instance on EBS instances
        parser.add_option("-m", "--manual", action="store_true", dest="manual")
        
        # Grab provided reflector through provided userdata
        global options
        try:
            (options, args) = parser.parse_args(userdata.strip().split(" "))
        except:
            print '[ERROR] One of the options was not set correctly. Please verify your settings'
            print userdata
        if options.dev:
            global isDEV
            isDEV = True
            exe('sudo touch .ami_is_in_dev_mode')
        if isDEV:
            if options.anttests:
                print '[INFO] ant realclean test will be run 1.5 minutes after startup.'
                exe('touch .ant_test')
            if options.smokeurl and options.smokefile:
                print '[INFO] Retrieving smoke testing tarball: ' + options.smokeurl
                print '[INFO] Executing smoke testing file: ' + options.smokefile
                with open('.smoke_test', 'w') as f:
                    f.write(options.smokeurl + "\n" + options.smokefile)
            elif options.smokeurl or options.smokefile:
                print '[WARN] Both -u and -f have to be set together in order for smoke tests to run.'
            if options.branch:
                os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-6-sun"
                os.environ["HADOOP_NATIVE_ROOT"] = "/home/ubuntu/hadoop-lzo-0.4.10/"

                print '[INFO] Using branch: ' + options.branch
                savedPath = os.getcwd()
                os.chdir('/home/ubuntu/brisk')
                exe('git checkout ' + options.branch)
                exe('git pull')
                exe('ant clean')
                exe('ant')
                os.chdir(savedPath)
            if not os.path.isfile('.ami_switched_to_dev_branch') and options.amibranch:
                print '[INFO] Dev pulls are being performed from this point forth.'
                exe('git checkout ' + options.amibranch)
                exe('git pull')
                exe('sudo touch .ami_switched_to_dev_branch')
                exe('./.configurebrisk.py')
                sys.exit()

        if options.email:
            with open('.email', 'w') as f:
                print '[INFO] Setting up diagnostic email using: ' + options.email
                f.write(options.email)
        if options.clustername:
            print '[INFO] Using cluster name: ' + options.clustername
            clustername = options.clustername
        if (options.paidopscenter or options.opscenter) and int(launchindex) == 0:
            if options.paidopscenter:
                userpass = options.paidopscenter
            else:
                userpass = options.opscenter

            if len(userpass.split(':')) == 2:
                if not os.path.isfile('.opscenter_node'):
                    print '[INFO] Installing OpsCenter...'
                    user = userpass.split(':')[0]
                    password = userpass.split(':')[1]
                    print '[INFO] Using username: ' + user
                    print '[INFO] Using password: ' + password

                    if options.paidopscenter:
                        pipe('echo "deb http://' + user + ':' + password + '@deb.opsc.datastax.com/ unstable main"', 'sudo tee -a /etc/apt/sources.list.d/datastax.sources.list')
                    else:
                        pipe('echo "deb http://' + user + ':' + password + '@deb.opsc.datastax.com/free unstable main"', 'sudo tee -a /etc/apt/sources.list.d/datastax.sources.list')

                    pipe('echo "deb http://debian.riptano.com/maverick maverick main"', 'sudo tee -a /etc/apt/sources.list.d/datastax.sources.list')
                    exe('sudo apt-get update')
                    
                    if options.paidopscenter:
                        exe('sudo apt-get -y install opscenter')
                    else:
                        exe('sudo apt-get -y install opscenter-free')

            else:
                print '[ERROR] Not installing OpsCenter. Credentials were not in the correct format. (user:password)'
        if options.cfsreplication:
            print '[INFO] Using cfsreplication factor: ' + options.cfsreplication
            with open('.brisk_replication_factor', 'w') as f:
                f.write(options.cfsreplication)
            if options.vanillanodes and options.clustersize:
                if int(options.cfsreplication) > (int(options.clustersize) - int(options.vanillanodes)):
                    print '[ERROR] Using cfsreplication factor of 1 because previous cfsreplication factor was greater than available brisk nodes'
                    exe('rm .brisk_replication_factor')
        if options.vanillanodes:
            print '[INFO] Using vanilla nodes: ' + options.vanillanodes
            options.vanillanodes = int(options.vanillanodes)
            if int(launchindex) < options.vanillanodes:
                exe('touch .this_node_is_vanilla')
            if not options.clustersize:
                print '[ERROR] Vanilla option was set without Cluster Size.'
                print '[ERROR] Continuing as a collection of 1-node clusters.'
                sys.exit(1)
        if options.clustersize:
            print '[INFO] Using cluster size: ' + options.clustersize
        # if options.reflector:
        #     print '[INFO] Using reflector: ' + options.reflector
    except Exception, e:
        print "[INFO] No User Data was set. Naming cluster the same as the reservation ID."
        
    # Currently always set to true packaging
    if not isDEV or (options and options.manual):
        # Remove script files
        exe('sudo rm .configurebrisk.py')
        print '[INFO] Using manual option. Deleting .configurebrisk.py now. This AMI will never change any configs nor start brisk after this first run.'
    
    global clusterseed, opscenterseed

    # Brisk DC splitting
    stayinloop = True
    while stayinloop:
        print '[INFO] Reflector loop...'
        defaultReflector = 'http://reflector.datastax.com/brisk-reflector.php'
        if options and options.vanillanodes and options.vanillanodes != options.clustersize:
            req = urllib2.Request(defaultReflector + '?indexid=' + str(launchindex) + '&reservationid=' + reservationid + '&internalip=' + internalip + '&externaldns=' + publichostname + '&secondDCstart=' + str(options.vanillanodes))
            expectedResponses = 2
        else:
            req = urllib2.Request(defaultReflector + '?indexid=' + str(launchindex) + '&reservationid=' + reservationid + '&internalip=' + internalip + '&externaldns=' + publichostname + '&secondDCstart=0')
            expectedResponses = 1
        req.add_header('User-agent', 'DataStaxSetup')
        try:
            r = urllib2.urlopen(req).read()
            r = r.split("\n")

            status =  "[INFO] " + time.strftime("%m/%d/%y-%H:%M:%S", time.localtime()) + " Received " + r[0] + " of " + str(expectedResponses) + " responses from: \n"
            status += "       " + str(r[2:])
            with open('.current_status', 'w') as f:
                f.write(status)
            
            if options and options.vanillanodes and not options.clustersize:
                print '[ERROR] -v only works with -s also set. Starting up nodes with NO configurations.'
                stayinloop = False
                break
            if int(r[0]) == expectedResponses:
                r.pop(0)
                opscenterDNS = r[0]
                r.pop(0)
                global clusterlist

                # Assign the first IP to be a seed
                clusterlist.append(r[0])
                opscenterseed = clusterlist[0]
                
                if options and options.vanillanodes:
                    # Add one more IP to be a seed
                    clusterlist.append(r[1])
                stayinloop = False
            else:
                if options and options.clustersize:
                    time.sleep(2 + random.randint(0, int(options.clustersize) / 4 + 1))
                else:
                    time.sleep(2 + random.randint(0, 5))
        except:
            traceback.print_exc(file=sys.stdout)
            time.sleep(2 + random.randint(0, 5))

    with open('.current_status', 'w') as f:
        f.write('Complete!')

    if options and options.vanillanodes and not options.clustersize:
        sys.exit(0)

    if options and (options.paidopscenter or options.opscenter):
        with open('.opscenter_node', 'w') as f:
            f.write(opscenterDNS)
    
    if userdata:
        print "[INFO] Started with user data set to:"
        print userdata
    else:
        print "[INFO] No user data was set."
    if clusterlist:
        print "[INFO] Cluster list: " + str(clusterlist)
    else:
        print "[INFO] Cluster seed: " + str(clusterseed)
    print "[INFO] OpsCenter: " + str(opscenterseed)

def calculateTokens():
    import tokentool
    global tokens

    initalized = False
    if options and options.vanillanodes:
        tokentool.initialRingSplit([int(options.vanillanodes), int(options.clustersize) - int(options.vanillanodes)])
        initalized = True
    elif options and options.clustersize:
        tokentool.initialRingSplit([int(options.clustersize)])
        initalized = True
    
    if initalized:
        tokentool.focus()
        tokentool.calculateTokens()
        tokens = tokentool.originalTokens

def constructYaml():
    with open(confPath + 'cassandra.yaml', 'r') as f:
        yaml = f.read()

    # Create the seed list
    global clusterlist
    if len(clusterlist) > 0:
        print "[INFO] Using a list of seeds"
        seedsYaml = ''
        for ip in clusterlist:
            seedsYaml += ip + ','
        seedsYaml = seedsYaml[:-1]
    else:
        print "[INFO] Using a single seed"
        seedsYaml = clusterseed
    
    # Set seeds
    p = re.compile('seeds:.*')
    yaml = p.sub('seeds: "' + seedsYaml + '"', yaml)
    
    # Set listen_address
    p = re.compile('listen_address:.*\s*#')
    yaml = p.sub('listen_address: ' + internalip + '\n\n#', yaml)
    
    # Set rpc_address
    yaml = yaml.replace('rpc_address: localhost', 'rpc_address: 0.0.0.0')
    
    # Set cluster_name to reservationid
    yaml = yaml.replace("cluster_name: 'Test Cluster'", "cluster_name: '" + clustername + "'")
    
    # Construct token for an equally split ring
    if options and options.clustersize:
        if options.vanillanodes:
            if launchindex < options.vanillanodes:
                token = tokens[0][launchindex]
            else:
                token = tokens[1][launchindex - options.vanillanodes]
        else:
            token = tokens[0][launchindex]

        p = re.compile( 'initial_token:(\s)*#')
        yaml = p.sub( 'initial_token: ' + str(token) + "\n\n#", yaml)
    
    with open(confPath + 'cassandra.yaml', 'w') as f:
        f.write(yaml)
    
    print '[INFO] cassandra.yaml configured.'

def constructOpscenterConf():
    try:
        with open(opsConfPath + 'opscenterd.conf', 'r') as f:
            opsConf = f.read()
        
        # Configure OpsCenter
        opsConf = opsConf.replace('port = 8080', 'port = 7199')
        opsConf = opsConf.replace('interface = 127.0.0.1', 'interface = 0.0.0.0')
        opsConf = opsConf.replace('seed_hosts = localhost', 'seed_hosts = ' + opscenterseed)
        
        with open(opsConfPath + 'opscenterd.conf', 'w') as f:
            f.write(opsConf)
            
        print '[INFO] opscenterd.conf configured.'
    except:
        print '[INFO] opscenterd.conf not configured since conf was unable to be located.'

def constructEnv():
    envsh = None
    with open(confPath + 'cassandra-env.sh', 'r') as f:
        envsh = f.read()
    
    # Clear commented line
    envsh = envsh.replace('# JVM_OPTS="$JVM_OPTS -Djava.rmi.server.hostname=<public name>"', 'JVM_OPTS="$JVM_OPTS -Djava.rmi.server.hostname=<public name>"')
    
    # Set JMX hostname and password file
    settings = 'JVM_OPTS="$JVM_OPTS -Djava.rmi.server.hostname=' + internalip + '"\n'
    
    # Perform the replacement
    p = re.compile('JVM_OPTS="\$JVM_OPTS -Djava.rmi.server.hostname=(.*\s*)*?#')
    envsh = p.sub(settings + '\n\n#', envsh)
    
    with open(confPath + 'cassandra-env.sh', 'w') as f:
        f.write(envsh)
    
    print '[INFO] cassandra-env.sh configured.'
    
def mountRAID():
    # Only create raid0 once. Mount all times in init.d script.
    if not os.path.isfile('.raid_already_exists'):

        # Remove EC2 default /mnt from fstab
        fstab = ''
        fileToOpen = '/etc/fstab'
        exe('sudo chmod 777 ' + fileToOpen)
        with open(fileToOpen, 'r') as f:
            for line in f:
                if not "/mnt" in line:
                    fstab += line
        with open(fileToOpen, 'w') as f:
            f.write(fstab)
        exe('sudo chmod 644 ' + fileToOpen)
        
        # Create a list of devices
        devices = glob.glob("/dev/sd*")
        devices.remove('/dev/sda1')
        devices.sort()
        print '[INFO] Unformatted devices: ' + str(devices)
        
        # Check if there are enough drives to start a RAID set
        if len(devices) > 1:
            # Make sure the devices are umounted, then run fdisk on each device
            print '[INFO] Clear "invalid flag 0x0000 of partition table 4" by issuing a write, then running fdisk on each device...'
            formatCommands = """echo 'n
    p
    1


    t
    fd
    w'"""
            for device in devices:
                print '[INFO] Confirming devices are not mounted:'
                exe('sudo umount ' + device, False)
                pipe("echo 'w'", 'sudo fdisk -c -u ' + device)
                pipe(formatCommands, 'sudo fdisk -c -u ' + device)
        
            # Create a list of partitions to RAID
            exe('sudo fdisk -l')
            partitions = glob.glob("/dev/sd*[0-9]")
            partitions.remove('/dev/sda1')
            partitions.sort()
            print '[INFO] Partitions about to be added to RAID0 set: ' + str(partitions)
        
            # Make sure the partitions are umounted and create a list string
            partionList = ''
            for partition in partitions:
                print '[INFO] Confirming partitions are not mounted:'
                exe('sudo umount ' + partition, False)
                partionList += partition + ' '
            partionList = partionList.strip()
        
            print '[INFO] Creating the RAID0 set:'
            pipe('yes', 'sudo mdadm --create /dev/md0 --chunk=256 --level=0 --raid-devices=' + str(len(devices)) + ' ' + partionList, False)
            pipe('echo DEVICE ' + partionList, 'sudo tee /etc/mdadm/mdadm.conf')
            pipe('mdadm --detail --scan', 'sudo tee -a /etc/mdadm/mdadm.conf')
            exe('blockdev --setra 65536 /dev/md0')

            print '[INFO] Formatting the RAID0 set:'
            exe('sudo mkfs.xfs -f /dev/md0')
            
            # Configure fstab and mount the new RAID0 device
            raidMnt = '/raid0'
            pipe("echo '/dev/md0\t" + raidMnt + "\txfs\tdefaults,nobootwait,noatime\t0\t0'", 'sudo tee -a /etc/fstab')
            exe('sudo mkdir ' + raidMnt)
            exe('sudo mount -a')
            exe('sudo mkdir -p ' + raidMnt + '/cassandra/')
            exe('sudo chown -R ubuntu:ubuntu ' + raidMnt + '/cassandra')
        
            print '[INFO] Showing RAID0 details:'
            exe('cat /proc/mdstat')
            exe('sudo mdadm --detail /dev/md0')

        else:
            # Make sure the device is umounted, then run fdisk on the device
            print '[INFO] Clear "invalid flag 0x0000 of partition table 4" by issuing a write, then running fdisk on the device...'
            formatCommands = """echo 'd
    n
    p
    1


    t
    83
    w'"""
            exe('sudo umount ' + devices[0])
            pipe("echo 'w'", 'sudo fdisk -c -u ' + devices[0])
            pipe(formatCommands, 'sudo fdisk -c -u ' + devices[0])
        
            # Create a list of partitions to RAID
            exe('sudo fdisk -l')
            partitions = glob.glob("/dev/sd*[0-9]")
            partitions.remove('/dev/sda1')
            partitions.sort()
            
            print '[INFO] Formatting the new partition:'
            exe('sudo mkfs.xfs -f ' + partitions[0])
            
            # Configure fstab and mount the new formatted device
            mntPoint = '/mnt'
            pipe("echo '" + partitions[0] + "\t" + mntPoint + "\txfs\tdefaults,nobootwait,noatime\t0\t0'", 'sudo tee -a /etc/fstab')
            exe('sudo mkdir ' + mntPoint)
            exe('sudo mount -a')
            
            # Delete old data if present
            exe('sudo rm -rf ' + raidMnt + '/cassandra')
            exe('sudo rm -rf /var/lib/cassandra')
            exe('sudo rm -rf /var/log/cassandra')

            # Create cassandra directory
            exe('sudo mkdir -p ' + mntPoint + '/cassandra')
            exe('sudo chown -R cassandra:cassandra ' + mntPoint + '/cassandra')
        
        # Change cassandra.yaml to point to the new data directories
        with open(confPath + 'cassandra.yaml', 'r') as f:
            yaml = f.read()
        if len(partitions) > 1:
            yaml = yaml.replace('/var/lib/cassandra/data', raidMnt + '/cassandra/data')
            yaml = yaml.replace('/var/lib/cassandra/saved_caches', raidMnt + '/cassandra/saved_caches')
            yaml = yaml.replace('/var/lib/cassandra/commitlog', raidMnt + '/cassandra/commitlog')
        else:
            yaml = yaml.replace('/var/lib/cassandra/data', mntPoint + '/cassandra/data')
            yaml = yaml.replace('/var/lib/cassandra/saved_caches', mntPoint + '/cassandra/saved_caches')
        with open(confPath + 'cassandra.yaml', 'w') as f:
            f.write(yaml)
        
        # Remove the old cassandra folders
        subprocess.Popen("sudo rm -rf /var/log/cassandra/*", shell=True)
        subprocess.Popen("sudo rm -rf /var/lib/cassandra/*", shell=True)

        # Never create raid array again
        exe('sudo touch .raid_already_exists')

        print "[INFO] Mounted Raid.\n"

def additionalConfigurations():

    # ========= To be implemented by init.d script =========

    # Set limits
    pipe('echo 1', 'sudo tee /proc/sys/vm/overcommit_memory')
    
    # Install JNA
    if not os.path.isfile('brisk/resources/cassandra/lib/jna.jar'):
        exe('sudo ln -s /usr/share/java/jna.jar brisk/resources/cassandra/lib/jna.jar')

def additionalDevConfigurations():
    if isDEV:
        exe('sudo rm -rf /raid0/cassandra')
        exe('sudo rm -rf /var/lib/cassandra')
        exe('sudo rm -rf /var/log/cassandra')
        exe('ls -al')




clearMOTD()
getAddresses()

calculateTokens()
constructYaml()
constructOpscenterConf()
constructEnv()

mountRAID()

additionalConfigurations()
additionalDevConfigurations()

print "[INFO] .configurebrisk.py completed!\n"
