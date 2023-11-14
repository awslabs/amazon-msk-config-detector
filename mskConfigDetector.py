# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
    
import boto3
import json
from datetime import datetime
from datetime import timedelta

from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType
import ssl



# Get the cluster connection config
f = open('clusterdetails.json')
data = json.load(f)
strClusterarn= (data['config']['clusterarn'])
mskAuth= (data['config']['authtype'])
sasl_plain_username = (data['config']['user'])
sasl_plain_password = (data['config']['password'])
region=(data['config']['region'])
f.close()

security_protocol = 'SASL_SSL'
sasl_mechanism = 'SCRAM-SHA-512'
if mskAuth=="SASL":
    ssl_context = ssl.create_default_context()
    ssl_context.load_cert_chain(certfile="client.crt", keyfile="client.key")
    ssl_config = {'ssl_context': ssl_context}



clientCW = boto3.client('cloudwatch',region_name=region)
msk = boto3.client('kafka',region_name=region)
response= msk.describe_cluster_v2(ClusterArn=strClusterarn)

getbootstartBrokers=msk.get_bootstrap_brokers(ClusterArn=strClusterarn)

if mskAuth=="PLAIN":
    bootstartBrokers=getbootstartBrokers['BootstrapBrokerString']
elif mskAuth=="SASL":
    bootstartBrokers=getbootstartBrokers['BootstrapBrokerStringSaslScram']


strGP3Candidate=""
strGP3CandidateValue=""
strClusterName=""
strRPartitioncount=""

filename=datetime.now()
filename=filename.strftime("%Y%m%dT%H%M")
filename="output/"+filename + "_MSKDetectorReport.txt"

reportFile = open(filename, "w")



def readgp3config(strInstanceType):
    
    f = open('config/optimalConfigBook.json')
    data = json.load(f)
    if strInstanceType in data['GP3Candidate']:
        strGP3CandidateValue= (data['GP3Candidate'][strInstanceType])
        strGP3Candidate="TRUE"
    else:
        strGP3CandidateValue=""
        strGP3Candidate="FALSE"
    f.close()
    return strGP3Candidate,strGP3CandidateValue

def findtheConfig(strConfigName):
    a="FALSE"
    if strConfigName.startswith("num.io.threads"):
        a="TRUE"
    if strConfigName.startswith("num.network.threads"):
        a="TRUE"
    if strConfigName.startswith("remote.log.reader.threads"):
        a="TRUE"
    if strConfigName.startswith("log.segment.bytes"):
        a="TRUE"
    if strConfigName.startswith("replica.lag.time.max.ms"):
        a="TRUE"
    if strConfigName.startswith("socket.receive.buffer.bytes"):
        a="TRUE"
    if strConfigName.startswith("socket.request.max.bytes"):
        a="TRUE"
    if strConfigName.startswith("socket.send.buffer.bytes"):
        a="TRUE"
    if strConfigName.startswith("num.replica.fetchers"):
        a="TRUE"
    return (a)
   
def readClusterConfig(strClusterConfigarn,strConfigurationRevision,strStorageMode):
    strReturnvalue=""
    if strClusterConfigarn !="default":
        a= msk.describe_configuration_revision(Arn=strClusterConfigarn,Revision=strConfigurationRevision)
        
        b=str(a['ServerProperties'])
        c=b.split("\\n")
        i=0
        strReturnvalue="Current cluster configuration:\n"
        for i in range(len(c)):
        #    if c[i].startswith("unclean.leader.election.enable"):
            if findtheConfig(c[i])=="TRUE":
                strReturnvalue=strReturnvalue + c[i] + "\n"
        
    else:
        if strStorageMode!="TIERED":
            f = open('config/optimalConfigBook.json')
            data = json.load(f)
            strReturnvalue="You are using MSK default configuration and you should change the cluster config as below\n"
            strReturnvalue=strReturnvalue + str((data['nontsdefault'])) + "\n"
            f.close()

    return strReturnvalue

def readRecommendedClusterConfig(strInstanceType,strStorageMode,strGP3Candidate):
    strReturnvalue=""

    f = open('config/optimalConfigBook.json')
    data = json.load(f)
    i=0
    strReturnvalue="\nThe recommended (or default if not specified) configuration for " + strInstanceType +" broker type are as below:\n"
    if strStorageMode=="LOCAL":
        strIndex="nonTS"
    else:
        strIndex="TS"
    
    global strRPartitioncount
    strRPartitioncount=str((data['partitionCount'][strInstanceType]))

    #strReturnvalue=strReturnvalue + str((data['brokerconfigLocal'][strInstanceType])) + "\n"
    a=str((data[strIndex][strInstanceType])).replace("'","").replace(" ","").replace("}","").replace("{","")
    b=a.split(",")
    for i in range(len(b)):
    #    if c[i].startswith("unclean.leader.election.enable"):
        strReturnvalue=strReturnvalue + b[i] + "\n"

    if strGP3Candidate=="TRUE":
        a=str((data['GP3'][strInstanceType])).replace("'","").replace(" ","").replace("}","").replace("{","")
        b=a.split(",")
        i=0
        for i in range(len(b)):
            strReturnvalue=strReturnvalue + b[i] + "\n"

    f.close()
    
    strReturnvalue=strReturnvalue + "\n**Update cluster configuration to recommended value if different from custom configuration. Leave it to default if none specified." + "\n"

    return strReturnvalue

def findNumAZ(strClientSubnets):
    intNumAZ=0
    if strClientSubnets!="":
        a=str(strClientSubnets)
        b=a.split(",")
        i=0
        for i in range(len(b)):
            intNumAZ=intNumAZ+1
    return intNumAZ

def getPartitionCountMetrics(strClusterName,strBrokerID):
    strID="broker" + str(strBrokerID)
    startTime=datetime.now() - timedelta(days=1)
    endTime=startTime + timedelta(minutes=1)
    startTime=startTime.strftime("%Y-%m-%dT%H:%M:00:00")
    endTime=endTime.strftime("%Y-%m-%dT%H:%M:00:00")
    responseCW = clientCW.get_metric_data(
    
        MetricDataQueries=[
            {
                    "Id": strID,
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/Kafka",
                            "MetricName": "PartitionCount",
                            "Dimensions": [
                                {
                                    "Name": "Cluster Name",
                                    "Value": strClusterName
                                },
                                {
                                    "Name": "Broker ID",
                                    "Value": strBrokerID
                                }
                            ]
                        },
                        "Period": 60,
                        "Stat": "Sum",
                        "Unit": "Count"
                    },
                    "ReturnData": True
                }
                
            ],
            StartTime=datetime.strptime(startTime,"%Y-%m-%dT%H:%M:00:00") ,
            EndTime= datetime.strptime(endTime,"%Y-%m-%dT%H:%M:00:00")
    )
    return(responseCW)

def getKafkaDataLogsDiskUsedMetrics(strClusterName,strBrokerID):
    strID="broker" + str(strBrokerID)
    startTime=datetime.now() - timedelta(days=1)
    endTime=startTime + timedelta(minutes=1)
    startTime=startTime.strftime("%Y-%m-%dT%H:%M:00:00")
    endTime=endTime.strftime("%Y-%m-%dT%H:%M:00:00")
    responseCW = clientCW.get_metric_data(
    
        MetricDataQueries=[
            {
                    "Id": strID,
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/Kafka",
                            "MetricName": "KafkaDataLogsDiskUsed",
                            "Dimensions": [
                                {
                                    "Name": "Cluster Name",
                                    "Value": strClusterName
                                },
                                {
                                    "Name": "Broker ID",
                                    "Value": strBrokerID
                                }
                            ]
                        },
                        "Period": 60,
                        "Stat": "Average",
                        "Unit": "Percent"
                    },
                    "ReturnData": True
                }
                
            ],
            StartTime=datetime.strptime(startTime,"%Y-%m-%dT%H:%M:00:00") ,
            EndTime= datetime.strptime(endTime,"%Y-%m-%dT%H:%M:00:00")
    )
    return(responseCW)

#Kafka admin Operation
def getTopicList(bootstrapservers):
    
    if mskAuth=="PLAIN":
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrapservers, client_id='test')
    else:
        admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrapservers,
        security_protocol=security_protocol,
        ssl_context=ssl_context,
        sasl_mechanism=sasl_mechanism,
        sasl_plain_username=sasl_plain_username,
        sasl_plain_password=sasl_plain_password
    )

    topiclist=admin_client.list_topics()
    return (topiclist)

def describeTopic(bootstrapservers,topicname):
    if mskAuth=="PLAIN":
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrapservers, client_id='test')
    else:
        admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrapservers,
        security_protocol=security_protocol,
        ssl_context=ssl_context,
        sasl_mechanism=sasl_mechanism,
        sasl_plain_username=sasl_plain_username,
        sasl_plain_password=sasl_plain_password
    )
    
    
    
    topicconfig = admin_client.describe_topics([topicname])
    replicationfactor=topicconfig[0]['partitions'][0]['replicas']
    replicationfactor=len(replicationfactor)
    result_dict = str(admin_client.describe_configs(config_resources=[ConfigResource(ConfigResourceType.TOPIC, topicname)]))
    segmentMS=""
    minInsyncReplicas=""
    segmentBytes=""

    result_dict=result_dict.replace(" ","")
    if result_dict!="":
        splitvalues=result_dict.split("),")
        for splitvalue in splitvalues:
            splitconfig=splitvalue.split(",")
            
            if splitconfig[0].replace("(","")=="config_names='segment.ms'":
                
                segmentMS=splitconfig[1]
            elif splitconfig[0].replace("(","")=="config_names='min.insync.replicas'":
                
                minInsyncReplicas=splitconfig[1]
            elif splitconfig[0].replace("(","")=="config_names='segment.bytes'":
                
                segmentBytes=splitconfig[1]
    segmentMS=segmentMS.replace("config_value=","").replace("'","")
    minInsyncReplicas=minInsyncReplicas.replace("config_value=","").replace("'","")
    segmentBytes=segmentBytes.replace("config_value=","").replace("'","")
    return (replicationfactor,segmentMS,minInsyncReplicas,segmentBytes)

        
# End Kafka admin operation
    

def main():
    #start of storage
    print ("Generating report for " + strClusterarn)
    if 'StorageMode' in response['ClusterInfo']['Provisioned']['BrokerNodeGroupInfo']:
        strStorageMode=response['ClusterInfo']['Provisioned']['BrokerNodeGroupInfo']['StorageMode']
    else:
        strStorageMode="LOCAL"
    strClientSubnets=response['ClusterInfo']['Provisioned']['BrokerNodeGroupInfo']['ClientSubnets']
    intNumbAZ=findNumAZ (strClientSubnets)
    strNumberOfBrokerNodes=response['ClusterInfo']['Provisioned']['NumberOfBrokerNodes']
    strClusterName=response['ClusterInfo']['ClusterName']

    strProvisionedThroughput=False

    # Provisioned storage throughput is optional and can only be enabled using kafka.m5.4xlarge or a larger broker type
    if 'ProvisionedThroughput' in response['ClusterInfo']['Provisioned']['BrokerNodeGroupInfo']['StorageInfo']['EbsStorageInfo']:
        storageinfo=response['ClusterInfo']['Provisioned']['BrokerNodeGroupInfo']['StorageInfo']['EbsStorageInfo']['ProvisionedThroughput']
        strProvisionedThroughput=storageinfo['Enabled']
        if strProvisionedThroughput==True:
            strVolumeThroughput=storageinfo['VolumeThroughput']

    strInstanceType=response['ClusterInfo']['Provisioned']['BrokerNodeGroupInfo']['InstanceType']
    
    strGP3Candidate, strGP3CandidateValue=readgp3config(strInstanceType)
    
    strReport=  "=================================== Beginning of the report ===================================\n"
    strReport=strReport + "Cluster arn:" + strClusterarn + "\n"
    
    strReport= strReport + "Instance type:" + strInstanceType + "\n"
    strReport= strReport + "Number of brokers:" + str(strNumberOfBrokerNodes) + "\n"
    
    strReport= strReport + "Storage mode:" + str(strStorageMode) + "\n"
    if intNumbAZ<3:
        strReport=strReport + "Number of AZs:"+ str(intNumbAZ) + " (The cluster is deployed across :" + str(intNumbAZ) + " AZs. It is recommended to deploy your cluster across 3 AZs.)" +"\n"
    else:
        strReport=strReport + "Number of AZs:"+ str(intNumbAZ) + " (Cluster is deployed across three AZs and following the best practices of AZ deployment.)" +"\n"

    if strGP3Candidate=="TRUE":
        if strProvisionedThroughput==True:
            strReport=strReport + "**Current provisioned throughput for each borker is:" + str(strVolumeThroughput) + "\n"
            strReport=strReport + "**The recommended provisioned throughput for each " + strInstanceType + " broker is:" + strGP3CandidateValue + "\n"
    else:
        strReport=strReport + "**"+  strInstanceType + " is not eligible for enabling provisioned throughput "  + "\n"

    print ("Reading cluster metadata.....")
    reportFile.write(strReport)

    # end of storage

    # start cluster config
    strReport="\n************ Analyzing cluster config ************" + "\n"
    #print (strReport)
    reportFile.write(strReport)
    strConfigurationRevision=0
    if 'ConfigurationArn' in response['ClusterInfo']['Provisioned']['CurrentBrokerSoftwareInfo']:
        strClusterConfigarn=response['ClusterInfo']['Provisioned']['CurrentBrokerSoftwareInfo']['ConfigurationArn']
        strConfigurationRevision=response['ClusterInfo']['Provisioned']['CurrentBrokerSoftwareInfo']['ConfigurationRevision']
    else:
        strClusterConfigarn='default'
    
    #print (readClusterConfig(strClusterConfigarn,strConfigurationRevision,strStorageMode))
    reportFile.write(readClusterConfig(strClusterConfigarn,strConfigurationRevision,strStorageMode))
    
    strReport=readRecommendedClusterConfig(strInstanceType,strStorageMode,strGP3Candidate)
    #print (strReport)
    reportFile.write(strReport)
    # end cluster config

    
    
    i=0
    strReport="\n************ Analyzing partition count ************" + "\n" + "*Recommended partition count for each " + strInstanceType + " broker type is:" + strRPartitioncount + "\n" +"Current partition count for each broker is: " + "\n"
    #print ("************ Analyzing partition count ************" + "\n")
    #print ("recommended partition count for each " + strInstanceType + " broker type is:" + strRPartitioncount + "\n")
    print ("Reading CloudWatch metrics...")
    reportFile.write (strReport)
    
    intTotalPartition=0
    for i in range(int(strNumberOfBrokerNodes)):
        strCPartitionCount=(getPartitionCountMetrics(strClusterName,str(i+1)))
        strCPartitionCountPerBroker=strCPartitionCount['MetricDataResults'][0]['Values']
        strCPartitionCountPerBroker=str(strCPartitionCountPerBroker).replace("[","").replace("]","")
        if strCPartitionCountPerBroker=="":
            strCPartitionCountPerBroker=0
        strCPartitionCountPerBroker=int(float(str(strCPartitionCountPerBroker)))
        intTotalPartition=intTotalPartition+ strCPartitionCountPerBroker
        strCPartitionCountPerBroker=("Broker ID: " + str(i+1) + "| Partition count:" + str(strCPartitionCountPerBroker) + "\n")
        #print(strCPartitionCountPerBroker)
        reportFile.write(strCPartitionCountPerBroker)
    #print ("Total partitions (including replica) in this cluster are: " + str(intTotalPartition) + "\n")
    reportFile.write("*Total partitions (including replica) in this cluster are: " + str(intTotalPartition) + "\n")
    # partition

#aws cloudwatch get-metric-data --metric-data-queries file://getm.json --start-time 2023-03-10T04:00:00Z --end-time 2023-03-10T04:01:00Z

    # read disk usage
    strReport="\n************ Analyzing disk usage ************" + "\n"
    #print (strReport)
    reportFile.write(strReport)
    strDiskUsageAlert=""
    i=0
    for i in range(int(strNumberOfBrokerNodes)):
        strDiskUsage=(getKafkaDataLogsDiskUsedMetrics(strClusterName,str(i+1)))
        strDiskUsagePerBroker=strDiskUsage['MetricDataResults'][0]['Values']
        strDiskUsagePerBroker=str(strDiskUsagePerBroker).replace("[","").replace("]","")
        if strDiskUsagePerBroker=="":
            strDiskUsagePerBroker=0
        if float(strDiskUsagePerBroker)>80:
           strDiskUsageAlert="ALARM" 
        strDiskUsagePerBroker=float(str(strDiskUsagePerBroker))
        strDiskUsagePerBroker=("Broker ID: " + str(i+1) + "| Disk usage :" + str(strDiskUsagePerBroker) + "%\n")
        
        reportFile.write(strDiskUsagePerBroker)
        
    if strDiskUsageAlert=="ALARM":
        reportFile.write("One or more brokers in the cluster are exceeding 80%' disk usage, which can lead to potential cluster downtime and service disruptions. Immediate action is recommended to address the high disk usage on the affected brokers.\n")
    

    #end disk usage
    
    

    # read Recommended topic config
    f = open('config/optimalConfigBook.json')
    data = json.load(f)
    rMinreplicationfactor=str((data['topicconfig']['minreplicationfactor']))
    rSegmentMS=str((data['topicconfig']['segment.ms']))
    rMinInsyncReplicas=str((data['topicconfig']['min.insync.replicas']))
    if strStorageMode=="TIERED":
        rSegmentBytes=str((data['topicconfig']['segment.bytes.TS']))
    else:
        rSegmentBytes=str((data['topicconfig']['segment.bytes.noTS']))
    
    f.close()
    # end



    #================================ topic analysis ============================
    strReport="\n************ Analyzing topics ************" + "\n"
    #print (strReport)
    reportFile.write(strReport)
    topicNames=getTopicList(bootstartBrokers)
    strReport="Total number of topics:" + str(len(topicNames)) + "\nList of topic:" + "\n" + str(topicNames) +"\n" 
    #print(strReport)
    reportFile.write(strReport)
    print("Analyzing topic configuraiton......")
    for topicName in topicNames:
        
        strTopicName=topicName
        cMinreplicationfactor,cSegmentMS,cMinInsyncReplicas,cSegmentBytes=describeTopic(bootstartBrokers, strTopicName)
        
        strReport=""
        if int(cMinreplicationfactor)<int(rMinreplicationfactor) or int(cSegmentMS<rSegmentMS) or int(cMinInsyncReplicas)<int(rMinInsyncReplicas) or int(cSegmentBytes)<int(rSegmentBytes):
            strReport="\nTopic Config violation for topic: " + strTopicName + "\nCurrent config= replicationfactor: " + str(cMinreplicationfactor) + ",segment.ms:" + str(cSegmentMS) + ",min.insync.replicas:" + str(cMinInsyncReplicas) + ",segment.bytes:"+str(cSegmentBytes)
            strReport=strReport + "\nRecommended config= replicationfactor: " + str(rMinreplicationfactor) + ",segment.ms:" + str(rSegmentMS) + ",min.insync.replicas:" + str(rMinInsyncReplicas) + ",segment.bytes:"+str(rSegmentBytes) + "\n"
        
        #print (strReport)
        print("Analyzing topic" + topicName +" ......")
        reportFile.write(strReport)


    strReport="\n************ General configuration best practices ************" + "\n"
    strReport=strReport +"\n1. Replication factor=1 causes offline partitions during maintanance of cluster, which prevents client to produce/consume messeages\n"
    strReport=strReport +"\n2. segment.ms -Wrong configuration could lead to cluster downtime. For example if segment.ms = 20000, it would causes Kafka to generate a new log segment every 20000 milliseconds instead of once every 7 days (or 604800000 ms) which is the default value. Kafka stores two memory mapped files per log segment. This configuration will generate a high number of memory mapped files and Kafka is approaching the max file limit per Linux process of 262,144.\n"
    strReport=strReport +"\n3. min.insync.replicas should be set to a value equal to or less than the replication factor. Failing to do so could increase the risk of availability loss.\n"
    strReport=strReport + "\n4. Changing max.message.bytes configuration with large number causes brokers to receive too large requests and cannot allocate the memory to read it in user space. Therefore, brokers can become unhealthy. Leave it to default.\n"
    strReport=strReport+ "\n5. It is recommended to keep the default value of segment.bytes (1GB) to avoid generating too many memory mapped files, which may cause Kafka to approach the maximum file limit per Linux process and result in broker unavailability.\n"
    #print(strReport)
    reportFile.write(strReport)
    strReport="=================================== End of the report ===================================\n"
    #print(strReport)
    reportFile.write(strReport)


main()
reportFile.close()
print("Completed the analysis. Check the report in the output directory. File name: "+ filename)
