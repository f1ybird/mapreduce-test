package com.kevin.mapreduce.mr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogbackApp {

    private final static Logger logger = LoggerFactory.getLogger(LogbackApp.class);

    public static void main(String[] args) {
        logger.info("logback 成功了");
        logger.error("logback 成功了");
        logger.debug("logback 成功了");
    }
}