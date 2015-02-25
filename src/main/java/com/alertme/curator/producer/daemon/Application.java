/**
 * Copyright (C) 2014 AlertMe.com Ltd
 */
package com.alertme.curator.producer.daemon;

import com.alertme.curator.producer.daemon.server.Server;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;

@EnableAutoConfiguration
@ComponentScan
@Configuration
public class Application {

    public static void main (String args[]) {
        SpringApplication.run(Application.class);
    }

}
