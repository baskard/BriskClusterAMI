#!/usr/bin/env python

import urllib2, os, re, shlex, subprocess, time, sys, glob, random, time, traceback


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




# if os.path.isfile('.ami_is_in_dev_mode'):
#     exe('git pull')

# Begin configuration this is only run once in Public Packages
if os.path.isfile('.configurebrisk.py'):
    # Configure brisk variables
    exe('python .configurebrisk.py')

    # Download Hadoop's LZO Codec
    exe('wget https://s3.amazonaws.com/brisk-builds/hadoop-lzo-0.4.10.tar.gz', False)
    exe('tar xf hadoop-lzo-0.4.10.tar.gz')
    exe('rm hadoop-lzo-0.4.10.tar.gz')
    exe('mv hadoop-lzo-0.4.10/ .hadoop-lzo-0.4.10/')

    # Set ulimit hard limits
    pipe('echo "* soft nofile 32768"', 'sudo tee -a /etc/security/limits.conf')
    pipe('echo "* hard nofile 32768"', 'sudo tee -a /etc/security/limits.conf')
    pipe('echo "root soft nofile 32768"', 'sudo tee -a /etc/security/limits.conf')
    pipe('echo "root hard nofile 32768"', 'sudo tee -a /etc/security/limits.conf')

# Create /raid0
exe('sudo mount -a')

# Set JAVA_HOME in hadoop-env.sh
pipe('echo "export JAVA_HOME=/usr/lib/jvm/java-6-sun"', 'sudo tee -a brisk/resources/hadoop/conf/hadoop-env.sh')

# Change permission back to being ubuntu's
exe('sudo chown -R ubuntu:ubuntu /home/ubuntu')

# Start a background process to start OpsCenter after a given delay
subprocess.Popen(shlex.split('sudo -u ubuntu python .startopscenter.py &'))

# Actually start Brisk
if os.path.isfile('.this_node_is_vanilla'):
    exe('echo "Starting vanilla node"')
    exe('sudo brisk/bin/brisk cassandra')
else:
    pipe('echo HADOOP_ENABLED=1', 'sudo tee -a /etc/default/brisk')
    if os.path.isfile('.brisk_replication_factor'):
        with open('.brisk_replication_factor', 'r') as f:
            RF = f.read()
        exe('echo "Starting tracker node with RF=' + RF + '"')
        exe('brisk/bin/brisk cassandra -t -Dcfs.replication=' + RF)
    else:
        exe('echo "Starting tracker node with default RF"')
        exe('brisk/bin/brisk cassandra -t')
