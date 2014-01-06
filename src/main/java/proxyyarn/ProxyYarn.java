/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package proxyyarn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class ProxyYarn {
  private static final Logger log = LoggerFactory.getLogger(ProxyYarn.class);
  // 10mins
  private static final long clientTimeout = 1000*1000*60*10;
  
  public ProxyYarn() {

  }

  public boolean run() throws Exception {
    Configuration conf = new YarnConfiguration(new Configuration());
    
    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();

    YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
    log.info("Got Cluster metric info from ASM" 
        + ", numNodeManagers=" + clusterMetrics.getNumNodeManagers());

    List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(
        NodeState.RUNNING);
    log.info("Got Cluster node info from ASM");
    for (NodeReport node : clusterNodeReports) {
      log.info("Got node report from ASM for"
          + ", nodeId=" + node.getNodeId() 
          + ", nodeAddress" + node.getHttpAddress()
          + ", nodeRackName" + node.getRackName()
          + ", nodeNumContainers" + node.getNumContainers());
    }

    QueueInfo queueInfo = yarnClient.getQueueInfo("default");
    log.info("Queue info"
        + ", queueName=" + queueInfo.getQueueName()
        + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
        + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
        + ", queueApplicationCount=" + queueInfo.getApplications().size()
        + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());   

    List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
    for (QueueUserACLInfo aclInfo : listAclInfo) {
      for (QueueACL userAcl : aclInfo.getUserAcls()) {
        log.info("User ACL Info for Queue"
            + ", queueName=" + aclInfo.getQueueName()     
            + ", userAcl=" + userAcl.name());
      }
    }   
    
    
    FileSystem fs = FileSystem.get(conf);
    if (!fs.getClass().equals(DistributedFileSystem.class)) {
      log.error("Expected DistributedFileSystem, but was {}", fs.getClass().getSimpleName());
      System.exit(1);
    }

//    ApplicationClientProtocol applicationsManager;
//    InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConf.get(YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS));

//    log.info("Connecting to ResourceManager at {}", rmAddress);
//    Configuration appManagerServerConf = new Configuration(conf);
//    YarnRPC rpc = YarnRPC.create(appManagerServerConf);
//    ApplicationClientProtocol applicationManager = (ApplicationClientProtocol) rpc.getProxy(ApplicationClientProtocol.class, rmAddress, appManagerServerConf);

    String appName = "AccumuloProxyYarn";
    YarnClientApplication app = yarnClient.createApplication();

    // set the application name
    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    ApplicationId appId = appContext.getApplicationId();
    appContext.setApplicationName(appName);
    
//    GetNewApplicationRequest request = Records.newRecord(GetNewApplicationRequest.class);
//    GetNewApplicationResponse response = applicationManager.getNewApplication(request);
//    log.info("Got new ApplicationId=" + response.getApplicationId());

//    ApplicationId appId = response.getApplicationId();

    // Create a new ApplicationSubmissionContext
//    ApplicationSubmissionContext appContext = Records.newRecord(ApplicationSubmissionContext.class);
    // set the ApplicationId
//    appContext.setApplicationId(appId);
    // set the application name
//    appContext.setApplicationName(appName);

    // Create a new container launch context for the AM's container
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

    // Define the local resources required
    Map<String,LocalResource> localResources = new HashMap<String,LocalResource>();

    // Lets assume the jar we need for our ApplicationMaster is available in
    // HDFS at a certain known path to us and we want to make it available to
    // the ApplicationMaster in the launched container
    Path localJarPath = new Path("file:///Users/jelser/projects/accumulo-proxy-yarn/target/accumulo-proxy-yarn-0.0.1-SNAPSHOT.jar");
    Path jarPath = new Path("hdfs:///accumulo-proxy-yarn-0.0.1-SNAPSHOT.jar");
    fs.copyFromLocalFile(false, true, localJarPath, jarPath);
    FileStatus jarStatus = fs.getFileStatus(jarPath);
    LocalResource amJarRsrc = Records.newRecord(LocalResource.class);

    // Set the type of resource - file or archive
    // archives are untarred at the destination by the framework
    amJarRsrc.setType(LocalResourceType.FILE);

    // Set visibility of the resource
    // Setting to most private option i.e. this file will only
    // be visible to this instance of the running application
    amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);

    // Set the location of resource to be copied over into the
    // working directory
    amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));

    // Set timestamp and length of file so that the framework
    // can do basic sanity checks for the local resource
    // after it has been copied over to ensure it is the same
    // resource the client intended to use with the application
    amJarRsrc.setTimestamp(jarStatus.getModificationTime());
    amJarRsrc.setSize(jarStatus.getLen());

    // The framework will create a symlink called AppMaster.jar in the
    // working directory that will be linked back to the actual file.
    // The ApplicationMaster, if needs to reference the jar file, would
    // need to use the symlink filename.
    localResources.put("AppMaster.jar", amJarRsrc);

    // Set the local resources into the launch context
    amContainer.setLocalResources(localResources);

    // Set up the environment needed for the launch context
    Map<String,String> env = new HashMap<String,String>();

    // For example, we could setup the classpath needed.
    // Assuming our classes or jars are available as local resources in the
    // working directory from which the command will be run, we need to append
    // "." to the path.
    // By default, all the hadoop specific classpaths will already be available
    // in $CLASSPATH, so we should be careful not to overwrite it.
    String classPathEnv = "$CLASSPATH:./*:/Users/jelser/projects/accumulo-proxy-yarn/target/lib/*";
    env.put("CLASSPATH", classPathEnv);
    amContainer.setEnvironment(env);

    // Construct the command to be executed on the launched container
    String command = "${JAVA_HOME}" + "/bin/java" + " proxyyarn.ProxyYarnAppMaster 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + " 2>"
        + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";

    List<String> commands = new ArrayList<String>();
    commands.add(command);
    // add additional commands if needed

    // Set the command array into the container spec
    amContainer.setCommands(commands);

    // Define the resource requirements for the container
    // For now, YARN only supports memory so we set the memory
    // requirements.
    // If the process takes more than its allocated memory, it will
    // be killed by the framework.
    // Memory being requested for should be less than max capability
    // of the cluster and all asks should be a multiple of the min capability.
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(256);
    appContext.setResource(capability);

    // Create the request to send to the ApplicationsManager
//    SubmitApplicationRequest appRequest = Records.newRecord(SubmitApplicationRequest.class);
//    appRequest.setApplicationSubmissionContext(appContext);

    // Submit the application to the ApplicationsManager
    // Ignore the response as either a valid response object is returned on
    // success or an exception thrown to denote the failure
//    applicationManager.submitApplication(appRequest);


    // Set the container launch content into the ApplicationSubmissionContext
    appContext.setAMContainerSpec(amContainer);

    // Set the priority for the application master
    Priority pri = Records.newRecord(Priority.class);
    // TODO - what is the range for priority? how to decide? 
    pri.setPriority(0);
    appContext.setPriority(pri);

    // Set the queue to which this application is to be submitted in the RM
    appContext.setQueue("default");

    // Submit the application to the applications manager
    // SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);
    // Ignore the response as either a valid response object is returned on success 
    // or an exception thrown to denote some form of a failure
    log.info("Submitting application to ASM");

    yarnClient.submitApplication(appContext);
    
    return monitorApplication(yarnClient, appId);
/*    Thread.sleep(200);
    
    boolean running = false;
    while(true) {
      GetApplicationReportRequest reportRequest = Records.newRecord(GetApplicationReportRequest.class);
      reportRequest.setApplicationId(appId);
      GetApplicationReportResponse reportResponse = applicationManager.getApplicationReport(reportRequest);
      ApplicationReport report = reportResponse.getApplicationReport();
      
      log.info(report.toString());
      
      YarnApplicationState state = report.getYarnApplicationState();
      switch (state) {
        case NEW:
        case NEW_SAVING:
        case SUBMITTED:
        case ACCEPTED:
          log.info("State: {}", state);
          break;
        case RUNNING:
          log.info("Running application");
          running = true;
          break;
        case FINISHED:
        case FAILED:
        case KILLED:
          log.info("State: {}", state);
          return;
        default:
          log.info("Unknown state: {}", state);
          return;
      }
      
      if (!running) {
        Thread.sleep(1000);
      }
    }*/
    
    
  }
  
  private boolean monitorApplication(YarnClient yarnClient, ApplicationId appId)
      throws YarnException, IOException {

    long clientStartTime = System.currentTimeMillis();
    while (true) {

      // Check app status every 1 second.
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        log.debug("Thread sleep in monitoring loop interrupted");
      }

      // Get application report for the appId we are interested in 
      ApplicationReport report = yarnClient.getApplicationReport(appId);

      log.info("Got application report from ASM for"
          + ", appId=" + appId.getId()
          + ", clientToAMToken=" + report.getClientToAMToken()
          + ", appDiagnostics=" + report.getDiagnostics()
          + ", appMasterHost=" + report.getHost()
          + ", appQueue=" + report.getQueue()
          + ", appMasterRpcPort=" + report.getRpcPort()
          + ", appStartTime=" + report.getStartTime()
          + ", yarnAppState=" + report.getYarnApplicationState().toString()
          + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
          + ", appTrackingUrl=" + report.getTrackingUrl()
          + ", appUser=" + report.getUser());

      YarnApplicationState state = report.getYarnApplicationState();
      FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
      if (YarnApplicationState.FINISHED == state) {
        if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
          log.info("Application has completed successfully. Breaking monitoring loop");
          return true;        
        }
        else {
          log.info("Application did finished unsuccessfully."
              + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
              + ". Breaking monitoring loop");
          return false;
        }       
      }
      else if (YarnApplicationState.KILLED == state 
          || YarnApplicationState.FAILED == state) {
        log.info("Application did not finish."
            + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
            + ". Breaking monitoring loop");
        return false;
      }     

      if (System.currentTimeMillis() > (clientStartTime + clientTimeout)) {
        log.info("Reached client specified timeout for application. Killing application");
        forceKillApplication(yarnClient, appId);
        return false;       
      }
    }     
  }

  /**
   * Kill a submitted application by sending a call to the ASM
   * @param appId Application Id to be killed. 
   * @throws YarnException
   * @throws IOException
   */
  private void forceKillApplication(YarnClient yarnClient, ApplicationId appId)
      throws YarnException, IOException {
    // TODO clarify whether multiple jobs with the same app id can be submitted and be running at 
    // the same time. 
    // If yes, can we kill a particular attempt only?

    // Response can be ignored as it is non-null on success or 
    // throws an exception in case of failures
    yarnClient.killApplication(appId);  
  }

  public static void main(String[] args) throws Exception {
    ProxyYarn py = new ProxyYarn();
    if (py.run()) {
      log.info("Client exited successfully");
      System.exit(0);
    } else {
      log.info("Client did *not* exit successfully");
      System.exit(1);
    }
  }
}
