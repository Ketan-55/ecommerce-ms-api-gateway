package com.ecommerce.api_gateway.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.UUID;
@Component
public class GlobalLoggingFilter implements GlobalFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(GlobalLoggingFilter.class);

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {

        long startTime = System.currentTimeMillis();

        // 🔥 Generate Correlation ID
        String correlationId = UUID.randomUUID().toString();

        // 🔥 Incoming Request
        ServerHttpRequest request = exchange.getRequest();

        String method = request.getMethodValue();

        String path = request.getURI().getPath();

        log.info("➡️ Incoming Request | CorrelationId={} | Method={} | Path={}", correlationId, method, path);

        // 🔥 Add Correlation ID Header
        ServerHttpRequest mutatedRequest = request.
                mutate().header("X-Correlation-Id", correlationId).build();

        ServerWebExchange mutatedExchange =
                exchange.mutate()
                        .request(mutatedRequest)
                        .build();
        return  chain.filter(mutatedExchange)
                .then(Mono.fromRunnable(() -> {

                    long executionTime =
                            System.currentTimeMillis() - startTime;

                    int statusCode =
                            exchange.getResponse()
                                    .getStatusCode()
                                    .value();

                    log.info(
                            "⬅️ Response | CorrelationId={} | Status={} | Time={} ms",
                            correlationId,
                            statusCode,
                            executionTime
                    );
                }));
    }

    @Override
    public int getOrder() {
        return -1;
    }
}
