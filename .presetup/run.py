#!/usr/bin/env python
### Script provided by DataStax.

import shlex, subprocess, time, sys, os

# Using this AMI: ami-cef405a7

# And this security group:
# SSH  tcp  22  22  0.0.0.0/0
# Custom  tcp  9160  9160  0.0.0.0/0
# Custom  tcp  7000  7000  0.0.0.0/0

def exe(command):
    process = subprocess.Popen(shlex.split(command))
    process.wait()
    return process

def pipe(command1, command2):
    # Helper function to execute piping commands and print traces of the commands and output for debugging/logging purposes
    p1 = subprocess.Popen(shlex.split(command1), stdout=subprocess.PIPE)
    p2 = subprocess.Popen(shlex.split(command2), stdin=p1.stdout)
    p1.stdout.close()  # Allow p1 to receive a SIGPIPE if p2 exits.
    output = p2.communicate()[0]
    return output

# Setup Repositories
exe('sudo add-apt-repository "deb http://archive.canonical.com/ lucid partner"')
exe('sudo add-apt-repository "deb http://www.apache.org/dist/cassandra/debian unstable main"')
exe('sudo add-apt-repository "deb http://debian.riptano.com/maverick maverick main"')
exe('sudo apt-get -y --force-yes update')
exe('sudo apt-get -y --force-yes upgrade')
time.sleep(5)

# Install Java, JNA, Brisk, HTop, ...
exe('sudo apt-get -y --force-yes install sun-java6-jdk libjna-java htop emacs23-nox sysstat iftop binutils pssh pbzip2 xfsprogs zip unzip ruby openssl libopenssl-ruby curl maven2 ant liblzo2-dev')
exe('sudo apt-get -y --force-yes --no-install-recommends install mdadm')
exe('sudo -u ubuntu mvn install')
time.sleep(5)

# Remove OpenJDK
exe('sudo update-alternatives --set java /usr/lib/jvm/java-6-sun/jre/bin/java')
exe('sudo aptitude remove openjdk-6-jre-headless openjdk-6-jre-lib -y')

# For later:
# sudo rm -rf /etc/brisk/

# Setup a link to the motd script that is provided in the git repository
fileToOpen = '.profile'
exe('sudo chmod 777 ' + fileToOpen)
with open(fileToOpen, 'a') as f:
    f.write("""
python .motd.py
export JAVA_HOME=/usr/lib/jvm/java-6-sun
export HADOOP_NATIVE_ROOT=/home/ubuntu/.hadoop-lzo-0.4.10/
""")
exe('sudo chmod 644 ' + fileToOpen)

os.chdir('/root')
fileToOpen = '.profile'
exe('sudo chmod 777 ' + fileToOpen)
with open(fileToOpen, 'w') as f:
    f.write("""
export JAVA_HOME=/usr/lib/jvm/java-6-sun
export HADOOP_NATIVE_ROOT=/home/ubuntu/.hadoop-lzo-0.4.10/
""")
exe('sudo chmod 644 ' + fileToOpen)
os.chdir('/home/ubuntu')

# Create init.d script
initscript = """#!/bin/sh

### BEGIN INIT INFO
# Provides:          
# Required-Start:    $remote_fs $syslog
# Required-Stop:     
# Default-Start:     2 3 4 5
# Default-Stop:      
# Short-Description: Start brisk on boot.
# Description:       Enables brisk on startup.
### END INIT INFO

# Make sure variables get set
export JAVA_HOME=/usr/lib/jvm/java-6-sun
export HADOOP_NATIVE_ROOT=/home/ubuntu/.hadoop-lzo-0.4.10/

# Setup system properties
sudo ulimit -n 32768
echo 1 | sudo tee /proc/sys/vm/overcommit_memory

# Link JNA
sudo ln -s /usr/share/java/jna.jar brisk/resources/cassandra/lib

# Clear old .startlog
cd /home/ubuntu/
echo '' > .startlog
python .launcher.py >> .startlog
"""
exe('sudo touch /etc/init.d/start-ami-brisk.sh')
exe('sudo chmod 777 /etc/init.d/start-ami-brisk.sh')
with open('/etc/init.d/start-ami-brisk.sh', 'w') as f:
    f.write(initscript)
exe('sudo chmod 755 /etc/init.d/start-ami-brisk.sh')

# Setup limits.conf
pipe('echo "* soft nofile 32768"', 'sudo tee -a /etc/security/limits.conf')
pipe('echo "* hard nofile 32768"', 'sudo tee -a /etc/security/limits.conf')

# Setup Brisk to start on boot
exe('sudo update-rc.d -f start-ami-brisk.sh start 99 2 3 4 5 .')

# Clear everything on the way out.
exe('sudo rm .ssh/authorized_keys')

subprocess.Popen("sudo rm -rf /tmp/*", shell=True)
subprocess.Popen("sudo rm -rf /tmp/.*", shell=True)
exe('rm -rf ~/.bash_history')

sys.exit(0)

# Allow SSH within the ring to be easier (only for private AMIs)
# exe('ssh-keygen')
# exe('cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys')
