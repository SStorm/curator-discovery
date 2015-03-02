/**
 * Copyright (C) 2014 AlertMe.com Ltd
 */
package com.alertme.curator.producer.daemon;

import com.alertme.curator.discovery.client.CuratorDiscoveryClient;
import com.alertme.curator.discovery.client.ServiceDefinition;
import com.alertme.curator.producer.daemon.server.Server;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.zookeeper.common.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Collection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNot.not;

/*
Requires ZooKeeper to be running
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Application.class)
public class ServerIntegrationTest {

    @Value("${zookeeper}")
    private String zooKeeperConnectionString;

    @Value("${serviceType}")
    private String serviceType;

    private CuratorFramework client;

    private ServiceDiscovery<ServiceDefinition> discovery;

    private CuratorDiscoveryClient discoveryClient;
    private CuratorDiscoveryClient discoveryClient2;

    @Before
    public void setup() throws Exception {
        client = CuratorFrameworkFactory.newClient(zooKeeperConnectionString,
                        new ExponentialBackoffRetry(1000, 3));
        client.start();

        JsonInstanceSerializer<ServiceDefinition> serializer =
                new JsonInstanceSerializer<ServiceDefinition>(ServiceDefinition.class);

        discovery = ServiceDiscoveryBuilder.builder(ServiceDefinition.class)
                .client(client)
                .serializer(serializer)
                .basePath(Server.SERVICE_PATH).build();
        discovery.start();

        CuratorFramework clientForDiscovery = CuratorFrameworkFactory.newClient(zooKeeperConnectionString,
                new ExponentialBackoffRetry(1000, 3));
        discoveryClient = new CuratorDiscoveryClient(clientForDiscovery);

        CuratorFramework clientForDiscovery2 = CuratorFrameworkFactory.newClient(zooKeeperConnectionString,
                new ExponentialBackoffRetry(1000, 3));
        discoveryClient2 = new CuratorDiscoveryClient(clientForDiscovery2);

        Thread.sleep(1000);
    }

    @After
    public void tearDown() {
        IOUtils.closeStream(client);
        IOUtils.closeStream(discovery);
        discoveryClient.stop();
        discoveryClient2.stop();
    }

    @Test(timeout = 5000L)
    public void serviceWillBeAvailableForDiscovery() throws Exception {
        // given

        // when
        Collection<ServiceInstance<ServiceDefinition>> services = discovery.queryForInstances(serviceType);

        // then
        assertThat(services, hasSize(1));
        ServiceInstance<ServiceDefinition> instance = services.iterator().next();
        assertThat((String)instance.getPayload().getType(), equalTo(serviceType));
    }

    @Test(timeout = 5000L)
    public void discoveryClientWillLockService() {
        // given
        discoveryClient.start();

        // when
        ServiceInstance<ServiceDefinition> lockedInstance = discoveryClient.acquireInstance(serviceType);

        // then
        assertThat(lockedInstance, not(nullValue()));
    }

    @Test(timeout = 5000L)
    public void twoClientsCantLockTheSameInstance() {
        // given
        discoveryClient.start();
        discoveryClient2.start();

        // when
        ServiceInstance<ServiceDefinition> lockedInstance = discoveryClient.acquireInstance(serviceType);
        ServiceInstance<ServiceDefinition> lockedInstance2 = discoveryClient2.acquireInstance(serviceType);

        // then
        assertThat(lockedInstance, not(nullValue()));
        assertThat(lockedInstance2, nullValue());
    }

    @Test(timeout = 5000L)
    public void whenAClientRelinquishesALockAnotherOneCanLockInstance() {
        // given
        discoveryClient.start();
        discoveryClient2.start();

        // when
        discoveryClient.acquireInstance(serviceType);
        discoveryClient.stop();
        ServiceInstance<ServiceDefinition> lockedInstance2 = discoveryClient2.acquireInstance(serviceType);

        // then
        assertThat(lockedInstance2, not(nullValue()));
    }
}
