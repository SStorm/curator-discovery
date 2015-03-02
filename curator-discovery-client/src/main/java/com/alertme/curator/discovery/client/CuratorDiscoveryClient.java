/**
 * Copyright (C) 2014 AlertMe.com Ltd
 */
package com.alertme.curator.discovery.client;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.zookeeper.common.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.ws.Service;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class CuratorDiscoveryClient {

    private static final Logger log = LoggerFactory.getLogger(CuratorDiscoveryClient.class);

    private CuratorFramework client;
    private ServiceDiscovery<ServiceDefinition> discovery;

    public CuratorDiscoveryClient(CuratorFramework client) {
        this.client = client;
        JsonInstanceSerializer<ServiceDefinition> serializer =
                new JsonInstanceSerializer<ServiceDefinition>(ServiceDefinition.class);
        discovery = ServiceDiscoveryBuilder.builder(ServiceDefinition.class)
                .client(client)
                .serializer(serializer)
                .basePath(ServiceDefinition.DISCOVERY_BASE_PATH).build();
    }

    // TODO this will eat exceptions at the moment
    public ServiceInstance<ServiceDefinition> acquireInstance(String name) {
        if (client.getState() != CuratorFrameworkState.STARTED) {
            throw new IllegalStateException("Not started, run start() first");
        }
        try {
            Collection<ServiceInstance<ServiceDefinition>> instances = discovery.queryForInstances(name);
            for (ServiceInstance<ServiceDefinition> instance : instances) {
                if (attemptAcquireLock(instance, 0)) {
                    return instance;
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }

    private boolean attemptAcquireLock(ServiceInstance<ServiceDefinition> instance, int attempt) {
        // /discovery/locks/$serviceName/$serviceId/slot/$slotNo
        String lockPath = String.format("/discovery/locks/%s/%s/slot/%d",
                instance.getName(), instance.getId(), attempt);
        InterProcessSemaphoreMutex lock = new InterProcessSemaphoreMutex(client, lockPath);
        try {
            return lock.acquire(1000, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return false;
    }

    public void start() {
        try {
            client.start();
            discovery.start();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }

    public void stop() {
        IOUtils.closeStream(discovery);
        IOUtils.closeStream(client);
    }
}
