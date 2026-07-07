package com.fintech.pix.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A dedicated bounded pool for the blocking partner call, separate from Kafka consumer threads.
 * This is what lets the manual timeout in {@link com.fintech.pix.partner.ResilientPartnerCaller}
 * actually abandon a hung call instead of blocking a consumer thread indefinitely — the consumer
 * thread just waits on a Future with a deadline, it never runs the blocking I/O itself.
 */
@Configuration
@Profile({"worker", "all"})
public class PartnerExecutorConfig {

    @Bean(destroyMethod = "shutdown")
    public ExecutorService partnerCallExecutor(@Value("${app.partner.executor-pool-size:20}") int poolSize) {
        AtomicInteger counter = new AtomicInteger();
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable, "partner-call-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        return Executors.newFixedThreadPool(poolSize, threadFactory);
    }
}
