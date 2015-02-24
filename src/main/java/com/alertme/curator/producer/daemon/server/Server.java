/**
 * Copyright (C) 2014 AlertMe.com Ltd
 */
package com.alertme.curator.producer.daemon.server;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.*;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.zookeeper.common.IOUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class Server implements InitializingBean {

    @Value("${zookeeper}")
    private String zooKeeperConnectionString;

    @Value("${serviceType}")
    private String serviceType;

    @Value("${servicePort}")
    private int port;

    private CuratorFramework client;

    private ServiceDiscovery<ServiceDefinition> discovery;

    public void start() {
        System.out.println("Starting server");
        System.out.println("ZooKeeper: " + zooKeeperConnectionString);
        System.out.println("ServiceType: " + serviceType);
        System.out.println("ServicePort: " + port);

        client = CuratorFrameworkFactory.newClient(zooKeeperConnectionString, new ExponentialBackoffRetry(1000, 3));
        client.start();
        JsonInstanceSerializer<ServiceDefinition> serializer =
                new JsonInstanceSerializer<ServiceDefinition>(ServiceDefinition.class);

        ServiceDefinition definition = new ServiceDefinition(serviceType, port);

        UriSpec uriSpec = new UriSpec("{scheme}://{address}:{port}");
        try {
            ServiceInstance<ServiceDefinition> thisInstance = ServiceInstance.<ServiceDefinition>builder()
                    .name(definition.getType())
                    .payload(definition)
                    .port(definition.getPort())
                    .serviceType(ServiceType.DYNAMIC)
                    .uriSpec(uriSpec)
                    .build();
            discovery = ServiceDiscoveryBuilder.builder(ServiceDefinition.class)
                    .client(client)
                    .serializer(serializer)
                    .thisInstance(thisInstance)
                    .basePath("/discovery/services").build();
            discovery.start();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(discovery);
            IOUtils.closeStream(client);
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        start();
    }
}
