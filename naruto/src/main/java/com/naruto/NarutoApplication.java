package com.naruto;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * CommandLineRunner、ApplicationRunner 接口是在容器启动成功后的最后一步回调（类似开机自启动）
 * 可使用 ApplicationRunner 在 springboot 启动后测试下log4j2 还是 logback
 *
 */
@SpringBootApplication
public class NarutoApplication implements ApplicationRunner {
    private static final Logger logger = LogManager.getLogger(NarutoApplication.class);

    public static void main( String[] args ){
        SpringApplication.run(NarutoApplication.class, args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        logger.debug("Debugging log");
        logger.info("Info log");
        logger.warn("Hey, This is a warning!");
        logger.error("Oops! We have an Error. OK");
        logger.fatal("Damn! Fatal error. Please fix me.");
    }

}
