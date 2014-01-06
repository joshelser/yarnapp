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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class ProxyYarnAppMaster {
  private static final Logger log = LoggerFactory.getLogger(ProxyYarnAppMaster.class);

  private static AtomicInteger numCompletedContainers = new AtomicInteger();
  private static AtomicInteger numFailedContainers = new AtomicInteger();
  private static AtomicInteger numRequestedContainers = new AtomicInteger();
  private static AtomicInteger numAllocatedContainers = new AtomicInteger();

  @SuppressWarnings("rawtypes")
  private static AMRMClientAsync amRMClient;

  private static Integer numTotalContainers = 1;

  private static volatile boolean done;
  private static volatile boolean success;

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();

    amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
    amRMClient.init(conf);
    amRMClient.start();

    // NMCallbackHandler containerListener = createNMCallbackHandler();
    // nmClientAsync = new NMClientAsyncImpl(containerListener);
    // nmClientAsync.init(conf);
    // nmClientAsync.start();

    // Register self with ResourceManager
    // This will start heartbeating to the RM
    String appMasterHostname = NetUtils.getHostname();
    RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster(appMasterHostname, -1, "");
    
    ContainerRequest containerAsk = setupContainerAskForRM();
    amRMClient.addContainerRequest(containerAsk);

    for (int i = 0; i < 50; i++) {
      System.out.println("ProxyYarnAppMaster is running! Iteration " + i);
      Thread.sleep(1000);
    }
    // When the application completes, it should stop all running containers
    // log.info("Application completed. Stopping running containers");
    // nmClientAsync.stop();

    // When the application completes, it should send a finish application
    // signal to the RM
    log.info("Application completed. Signalling finish to RM");

//    FinalApplicationStatus appStatus;
    String appMessage = null;
    success = true;
//    if (numFailedContainers.get() == 0 && numCompletedContainers.get() == numTotalContainers) {
//      appStatus = FinalApplicationStatus.SUCCEEDED;
//    } else {
//      appStatus = FinalApplicationStatus.FAILED;
//      appMessage = "Diagnostics." + ", total=" + numTotalContainers + ", completed=" + numCompletedContainers.get() + ", allocated="
//          + numAllocatedContainers.get() + ", failed=" + numFailedContainers.get();
//      success = false;
//    }
    try {
      amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, appMessage, null);
    } catch (YarnException ex) {
      log.error("Failed to unregister application", ex);
    } catch (IOException e) {
      log.error("Failed to unregister application", e);
    }

    amRMClient.stop();

    System.exit(0);
  }

  private static class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    private static final Logger log = LoggerFactory.getLogger(RMCallbackHandler.class);

    @SuppressWarnings("unchecked")
    @Override
    public void onContainersCompleted(List<ContainerStatus> completedContainers) {
      log.info("Got response from RM for container ask, completedCnt=" + completedContainers.size());
      for (ContainerStatus containerStatus : completedContainers) {
        log.info("Got container status for containerID=" + containerStatus.getContainerId() + ", state=" + containerStatus.getState() + ", exitStatus="
            + containerStatus.getExitStatus() + ", diagnostics=" + containerStatus.getDiagnostics());

        // non complete containers should not be here
        assert (containerStatus.getState() == ContainerState.COMPLETE);

        // increment counters for completed/failed containers
        int exitStatus = containerStatus.getExitStatus();
        if (0 != exitStatus) {
          // container failed
          if (ContainerExitStatus.ABORTED != exitStatus) {
            // shell script failed
            // counts as completed
            numCompletedContainers.incrementAndGet();
            numFailedContainers.incrementAndGet();
          } else {
            // container was killed by framework, possibly preempted
            // we should re-try as the container was lost for some reason
            numAllocatedContainers.decrementAndGet();
            numRequestedContainers.decrementAndGet();
            // we do not need to release the container as it would be done
            // by the RM
          }
        } else {
          // nothing to do
          // container completed successfully
          numCompletedContainers.incrementAndGet();
          log.info("Container completed successfully." + ", containerId=" + containerStatus.getContainerId());
        }
      }

      // ask for more containers if any failed
      int askCount = numTotalContainers - numRequestedContainers.get();
      numRequestedContainers.addAndGet(askCount);

      if (askCount > 0) {
        for (int i = 0; i < askCount; ++i) {
          ContainerRequest containerAsk = setupContainerAskForRM();
          amRMClient.addContainerRequest(containerAsk);
        }
      }

      if (numCompletedContainers.get() == numTotalContainers) {
        done = true;
      }
    }

    @Override
    public void onContainersAllocated(List<Container> allocatedContainers) {
      log.info("Got response from RM for container ask, allocatedCnt=" + allocatedContainers.size());
      numAllocatedContainers.addAndGet(allocatedContainers.size());
      for (Container allocatedContainer : allocatedContainers) {
        log.info("Launching shell command on a new container." + ", containerId=" + allocatedContainer.getId() + ", containerNode="
            + allocatedContainer.getNodeId().getHost() + ":" + allocatedContainer.getNodeId().getPort() + ", containerNodeURI="
            + allocatedContainer.getNodeHttpAddress() + ", containerResourceMemory" + allocatedContainer.getResource().getMemory());
        // + ", containerToken"
        // +allocatedContainer.getContainerToken().getIdentifier().toString());

        // LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(allocatedContainer, containerListener);
        // Thread launchThread = new Thread(runnableLaunchContainer);
        //
        // // launch and start the container on a separate thread to keep
        // // the main thread unblocked
        // // as all containers may not be allocated at one go.
        // launchThreads.add(launchThread);
        // launchThread.start();
      }
    }

    @Override
    public void onShutdownRequest() {
      done = true;
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {}

    @Override
    public float getProgress() {
      // set progress to deliver to RM on next heartbeat
      float progress = (float) numCompletedContainers.get() / numTotalContainers;
      return progress;
    }

    @Override
    public void onError(Throwable e) {
      done = true;
      amRMClient.stop();
    }
  }

  /**
   * Setup the request that will be sent to the RM for the container ask.
   * 
   * @return the setup ResourceRequest to be sent to RM
   */
  private static ContainerRequest setupContainerAskForRM() {
    // setup requirements for hosts
    // using * as any host will do for the distributed shell app
    // set the priority for the request
    Priority pri = Records.newRecord(Priority.class);
    // TODO - what is the range for priority? how to decide?
    pri.setPriority(1);

    // Set up resource type requirements
    // For now, only memory is supported so we set memory requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(256);

    ContainerRequest request = new ContainerRequest(capability, null, null, pri);
    log.info("Requested container ask: " + request.toString());
    return request;
  }

}
