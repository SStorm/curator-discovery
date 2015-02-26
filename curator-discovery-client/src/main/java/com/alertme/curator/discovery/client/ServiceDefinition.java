/**
 * Copyright (C) 2014 AlertMe.com Ltd
 */
package com.alertme.curator.discovery.client;

public class ServiceDefinition {

    private String type;
    private int port;

    public ServiceDefinition() {
    }

    public ServiceDefinition(String type, int port) {
        this.type = type;
        this.port = port;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
