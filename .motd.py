#!/usr/bin/env python
### Script provided by DataStax.

import shlex, subprocess, time, urllib2, os

isDEV = False
if os.path.isfile('.ami_is_in_dev_mode'):
    isDEV = True

if not isDEV:
    # nodetoolStatement = "nodetool -h localhost ring"
    nodetoolStatement = "brisk/resources/cassandra/bin/nodetool -h localhost ring"
else:
    nodetoolStatement = "brisk/resources/cassandra/bin/nodetool -h localhost ring"

try:
    with open('.presetup/VERSION', 'r') as f:
        version = f.readline().strip()
except:
    version = "<<~/.presetup/VERSION missing>>"

req = urllib2.Request('http://instance-data/latest/meta-data/ami-launch-index')
launchIndex = urllib2.urlopen(req).read()
req = urllib2.Request('http://instance-data/latest/meta-data/public-hostname')
hostname = urllib2.urlopen(req).read()

try:
    req = urllib2.Request('http://instance-data/latest/user-data/')
    global userdata
    userdata = urllib2.urlopen(req).read()
    print
    print "Cluster started with these options:"
    print userdata
    print
except:
    print "No cluster configurations set."


waitingforstatus = False
while True:
    try:
        with open('.current_status', 'r') as f:
            status = f.read()
        if not status == 'Complete!':
            print status
        else:
            break
    except:
        if not waitingforstatus:
            print "Waiting for cluster to boot..."
            waitingforstatus = True
    time.sleep(5)

print """Waiting for nodetool...
(The cluster is now booting up. This should only take a moment.)
"""

retcode = 0
while True:
    retcode = subprocess.call(shlex.split(nodetoolStatement), stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    if (int(retcode) != 3):
        break

stoppedErrorMsg = False
while True:
    nodetoolOut = subprocess.Popen(shlex.split(nodetoolStatement), stderr=subprocess.PIPE, stdout=subprocess.PIPE).stdout.read()
    if (nodetoolOut.find("Error") == -1 and nodetoolOut.find("error") and len(nodetoolOut) > 0):
        if not stoppedErrorMsg:
            if waitingforstatus:
                time.sleep(15)
            stoppedErrorMsg = True
        else:
            break

print nodetoolOut

opscenterIP = None
opscenterInstalled = None
try:
    with open('.opscenter_node', 'r') as f:
        opscenterIP = f.read()
    packageQuery = subprocess.Popen(shlex.split("dpkg-query -l 'opscenter'"), stderr=subprocess.PIPE, stdout=subprocess.PIPE).stdout.read()
    if packageQuery:
        opscenterInstalled = True
except:
    pass

print """
Nodetool: brisk/resources/cassandra/bin/nodetool -h localhost ring
Cli: brisk/resources/cassandra/bin/cassandra-cli -h localhost
Hive: sudo brisk/bin/brisk hive"""

if opscenterIP and opscenterInstalled:
    print "Opscenter: http://" + opscenterIP + ":8888/"
    print "Please wait 60 seconds if this is the cluster's first start..."

print """
Sample Hive Demo:
    http://www.datastax.com/docs/0.8/brisk/brisk_demo

AMI command switches:
    -s # 
        (cluster size
         REQUIRED for a balanced ring)
    -v # 
        (number of vanilla nodes that only run cassandra
         -s is REQUIRED in order to use this option)
    -c # 
        (the CFS Replication factor
         at least these many non-vanilla nodes REQUIRED)
    -o user:pass 
        (the username and password provided during the free OpsCenter registration)
    -p user:pass 
        (the username and password provided during the paid OpsCenter registration)
    -n "name"
        (the name of the Brisk cluster)
    Visit: http://www.datastax.com/docs/0.8/brisk/install_brisk_ami for the full
        list of options, including developer options.

For more information on Brisk, visit: 
http://www.datastax.com/docs/0.8/brisk/index

For more information on this AMI and proper usage, visit:
http://www.datastax.com/docs/0.8/brisk/install_brisk_ami

For quick support, visit:
IRC: #datastax-brisk channel on irc.freenode.net

------------------------------------
DataStax AMI for DataStax' Brisk(TM)
AMI version """ + str(version) + """

------------------------------------

"""

notices = ''
knownErrors = []
knownErrors.append("yes: write error\n")
knownErrors.append("java.io.ioexception: timedoutexception()\n")
knownErrors.append("caused by: timedoutexception()\n")
for line in open('.startlog'):
    if ('error' in line.lower() or '[warn]' in line.lower() or 'exception' in line.lower()) and not line.lower() in knownErrors:
        notices += line

if len(notices) > 0:
    print "These notices occurred during the startup of this instance:"
    print notices


