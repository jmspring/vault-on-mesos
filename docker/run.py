#!/usr/bin/env python

# Start script for the ZooKeeper service.

# Environment variables:
#
# General:
#   SERVICE_NAME:               name for the service
#   CONTAINER_NAME:             name of the container
#   BASE_DATA_DIR:              the base directory for the service data/log dir
#   <SERVICE_NAME>_INSTANCES:   a comma separated list of service instances
#   <SERVICE_NAME>_INSTANCE_IDS:    a comma separated list of server IDs (one per instance)
#   <SERVICE_NAME>_INSTANCE_CLIENT_PORTS:   a comma separated list of client ports (one per instance)
#   <SERVICE_NAME>_INSTANCE_PEER_PORTS:     a comma separated list of peer ports (one per instance)
#   <SERVICE_NAME>_INSTANCE_LEADER_ELECTION_PORTS:  a comma separated list of leader election ports (one per instance)

from __future__ import print_function

import os
import re
import sys

def _to_env_var_name(s):
    return re.sub(r'[^\w]', '_', s).upper()

os.chdir(os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..'))

# initial config values
baseDataDir = None
configFile = os.path.join('conf', 'zoo.cfg')
logConfigFile = os.path.join('conf', 'log4j.properties')
nodeId = None
clientPort = None

# look up the base environment variables
baseEnvVars = [ 'SERVICE_NAME', 'CONTAINER_NAME', 'BASE_DATA_DIR' ]
for i in baseEnvVars:
    if i not in os.environ:
        sys.stderr.write('Required environment varaiable: {}\n', i)
        sys.exit(1)
baseDataDir = os.environ['BASE_DATA_DIR']
serviceName = os.environ['SERVICE_NAME']
containerName = os.environ['CONTAINER_NAME']

# look up service specific environment variables
nodeEnvVars = {
    '{}_INSTANCES'.format(_to_env_var_name(serviceName)) : None,
    '{}_INSTANCE_IDS'.format(_to_env_var_name(serviceName)) : None,
    '{}_INSTANCE_CLIENT_PORTS'.format(_to_env_var_name(serviceName)) : None,
    '{}_INSTANCE_PEER_PORTS'.format(_to_env_var_name(serviceName)) : None,
    '{}_INSTANCE_LEADER_ELECTION_PORTS'.format(_to_env_var_name(serviceName)) : None
}
for i in nodeEnvVars.keys():
    if i not in os.environ:
        sys.stderr.write('Required environment variable: {}\n'.format(var))
        sys.exit(1)
    else:
        nodeEnvVars[i] = os.environ[i].split(',')
count = len(nodeEnvVars['{}_INSTANCES'.format(_to_env_var_name(serviceName))])
for i in nodeEnvVars.keys():
    if len(nodeEnvVars[i]) != count:
        sys.stderr.write('{} does not have sufficient values.  Need {}, have {}\n'.format(
                            i, baseLen, len(nodeEnvVars[i])))
        sys.exit(1)
        
# setup zookeeper config
servers = {}
for i in range(0, len(nodeEnvVars['{}_INSTANCES'.format(_to_env_var_name(serviceName))])):
    servers['server.{}'.format(nodeEnvVars['{}_INSTANCE_IDS'.format(_to_env_var_name(serviceName))][i])] = '{}:{}:{}'.format(
            nodeEnvVars['{}_INSTANCES'.format(_to_env_var_name(serviceName))][i],
            nodeEnvVars['{}_INSTANCE_PEER_PORTS'.format(_to_env_var_name(serviceName))][i],
            nodeEnvVars['{}_INSTANCE_LEADER_ELECTION_PORTS'.format(_to_env_var_name(serviceName))][i])
    if nodeEnvVars['{}_INSTANCES'.format(_to_env_var_name(serviceName))][i] == containerName:
        nodeId = nodeEnvVars['{}_INSTANCE_IDS'.format(_to_env_var_name(serviceName))][i]
        clientPort = nodeEnvVars['{}_INSTANCE_CLIENT_PORTS'.format(_to_env_var_name(serviceName))][i]
dataDir = '{}/{}'.format(baseDataDir, nodeId)
zooConf = {
    'tickTime': 2000,
    'initLimit': 10,
    'syncLimit': 5,
    'dataDir': dataDir,
    'clientPort': clientPort,
    'quorumListenOnAllIPs': True,
    'autopurge.snapRetainCount':
        int(os.environ.get('MAX_SNAPSHOT_RETAIN_COUNT', 10)),
    'autopurge.purgeInterval':
        int(os.environ.get('PURGE_INTERVAL', 24))
}
print(zooConf)
print(servers)
zooConf.update(servers)
print(zooConf)

# setup logging configuration
LOG_PATTERN = (
    "%d{yyyy'-'MM'-'dd'T'HH:mm:ss.SSSXXX} %-5p [%-35.35t] [%-36.36c]: %m%n")
loggingConf = """# Log4j configuration, logs to rotating file
log4j.rootLogger=INFO,R

log4j.appender.R=org.apache.log4j.RollingFileAppender
log4j.appender.R.File=/var/log/%s/%s.log
log4j.appender.R.MaxFileSize=100MB
log4j.appender.R.MaxBackupIndex=10
log4j.appender.R.layout=org.apache.log4j.PatternLayout
log4j.appender.R.layout.ConversionPattern=%s
""" % (serviceName, containerName, LOG_PATTERN)

# write configs
confDir = os.path.join(dataDir, os.path.dirname(configFile))
if not os.path.exists(confDir):
    os.makedirs(confDir, 0750)

with open(os.path.join(dataDir, configFile), 'w') as f:
    for key, value in zooConf.iteritems():
        f.write("%s=%s\n" % (key, value))
    
with open(os.path.join(dataDir, logConfigFile), 'w') as f:
    f.write(loggingConf)

# write node ID in case of a cluster setup
if len(nodeEnvVars['{}_INSTANCES'.format(_to_env_var_name(serviceName))]) > 1:
    with open(os.path.join(dataDir, 'myid'), 'w') as f:
        f.write('%s\n' % nodeId)
    sys.stderr.write(
        'Starting {}, node id#{} of a {}-node ZooKeeper cluster...\n'
        .format(containerName, nodeId, 
                len(nodeEnvVars['{}_INSTANCES'.format(_to_env_var_name(serviceName))])))
jvmflags = [
    '-server',
    '-showversion',
    '-Dvisualvm.display.name="{}/{}"'.format(
        'local', containerName),
]

os.environ['JVMFLAGS'] = ' '.join(jvmflags) + ' ' + os.environ.get('JVM_OPTS', '')

# Start ZooKeeper
os.execl('bin/zkServer.sh', 'zookeeper', 'start-foreground')
        
    




