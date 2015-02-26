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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class Server implements InitializingBean, Runnable {

    private static final Logger log = LoggerFactory.getLogger(Server.class);

    public static final String SERVICE_PATH = "/discovery/services/";

    @Value("${zookeeper}")
    private String zooKeeperConnectionString;

    @Value("${serviceType}")
    private String serviceType;

    @Value("${servicePort}")
    private int port;

    private CuratorFramework client;

    private ServiceDiscovery<Map> discovery;

    private boolean run = true;

    public void init() {
        log.info("ZooKeeper: " + zooKeeperConnectionString);
        log.info("ServiceType: " + serviceType);
        log.info("ServicePort: " + port);

        client = CuratorFrameworkFactory.newClient(zooKeeperConnectionString, new ExponentialBackoffRetry(1000, 3));
        JsonInstanceSerializer<Map> serializer =
                new JsonInstanceSerializer<Map>(Map.class);

        ServiceDefinition definition = new ServiceDefinition(serviceType, port);

        UriSpec uriSpec = new UriSpec("{scheme}://{address}:{port}");
        try {
            ServiceInstance<Map> thisInstance = ServiceInstance.<Map>builder()
                    .name(definition.getType())
                    .payload(createDefinition())
                    .port(definition.getPort())
                    .serviceType(ServiceType.DYNAMIC)
                    .uriSpec(uriSpec)
                    .build();
            log.info("Registering instance of service, id is {}", thisInstance.getId());
            discovery = ServiceDiscoveryBuilder.builder(Map.class)
                    .client(client)
                    .serializer(serializer)
                    .thisInstance(thisInstance)
                    .basePath(SERVICE_PATH).build();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        init();
    }

    public void stop() {
        run = false;
        closeCurator();
    }

    @Override
    public void run() {
        log.info("Server starting...");
        try {
            client.start();
            discovery.start();
            while (run) {
                Thread.sleep(100);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            closeCurator();
        }
    }

    private Map<String, Object> createDefinition() {
        Map<String, Object> definition = new HashMap<String, Object>();
        definition.put("serviceType", serviceType);
        definition.put("port", port);
        return definition;
    }

    private void closeCurator() {
        log.info("Closing curator client");
        IOUtils.closeStream(discovery);
        IOUtils.closeStream(client);
    }
}
