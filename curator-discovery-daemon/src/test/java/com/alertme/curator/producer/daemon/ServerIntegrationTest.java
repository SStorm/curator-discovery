/**
 * Copyright (C) 2014 AlertMe.com Ltd
 */
package com.alertme.curator.producer.daemon;

import com.alertme.curator.producer.daemon.server.Server;
import com.alertme.curator.producer.daemon.server.ServiceDefinition;
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
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

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

    private ServiceDiscovery<Map> discovery;

    @Before
    public void setup() throws Exception {
        client = CuratorFrameworkFactory.newClient(zooKeeperConnectionString,
                        new ExponentialBackoffRetry(1000, 3));
        client.start();

        JsonInstanceSerializer<Map> serializer =
                new JsonInstanceSerializer<Map>(Map.class);

        discovery = ServiceDiscoveryBuilder.builder(Map.class)
                .client(client)
                .serializer(serializer)
                .basePath(Server.SERVICE_PATH).build();
        discovery.start();
    }

    @After
    public void tearDown() {
        IOUtils.closeStream(client);
        IOUtils.closeStream(discovery);
    }

    @Test(timeout = 2000L)
    public void serviceWillBeAvailableForDiscovery() throws Exception {
        // given
        // Server starts asynchronously. I'd love to have assertEventually here
        Thread.sleep(1000);

        // when
        Collection<ServiceInstance<Map>> services
                = discovery.queryForInstances(serviceType);

        // then
        assertThat(services, hasSize(1));
        ServiceInstance<Map> instance = services.iterator().next();
        assertThat((String)instance.getPayload().get("serviceType"), equalTo(serviceType));
    }
}
