/**
 * Copyright (C) 2014 AlertMe.com Ltd
 */
package com.alertme.curator.producer.daemon;

import com.alertme.curator.producer.daemon.server.Server;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.ThreadPoolExecutor;

@Component
public class Runner implements InitializingBean, DisposableBean {

    @Resource
    private Server server;

    private Thread serverThread;

    private void startServer() {
        serverThread = new Thread(server);
        serverThread.start();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        startServer();
    }

    @Override
    public void destroy() throws Exception {
        server.stop();
    }
}
