package com.michelin.ns4kafka.services.connect;

import com.michelin.ns4kafka.services.KafkaAsyncExecutorConfig;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.client.ProxyHttpClient;
import io.micronaut.http.filter.OncePerRequestHttpServerFilter;
import io.micronaut.http.filter.ServerFilterChain;
import org.reactivestreams.Publisher;

import javax.inject.Inject;
import java.net.URI;
import java.util.List;
import java.util.Optional;

@Filter(KafkaConnectClientProxy.CONNECT_PROXY_PREFIX + "/**")
public class KafkaConnectClientProxy extends OncePerRequestHttpServerFilter {
    public static final String CONNECT_PROXY_PREFIX = "/connect-proxy";
    public static final String CONNECT_PROXY_HEADER = "X-Connect-Cluster";

    @Inject
    ProxyHttpClient client;
    @Inject
    List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfigs;

    @Override
    public Publisher<MutableHttpResponse<?>> doFilterOnce(HttpRequest<?> request, ServerFilterChain chain) {
        // retrieve the connectConfig based on Header
        if (!request.getHeaders().contains(KafkaConnectClientProxy.CONNECT_PROXY_HEADER)) {
            return Publishers.just(new Exception("Missing required Header " + KafkaConnectClientProxy.CONNECT_PROXY_HEADER));
        }
        String cluster = request.getHeaders().get(KafkaConnectClientProxy.CONNECT_PROXY_HEADER);

        Optional<KafkaAsyncExecutorConfig> config = kafkaAsyncExecutorConfigs.stream()
                .filter(kafkaAsyncExecutorConfig -> kafkaAsyncExecutorConfig.getName().equals(cluster))
                .findFirst();
        if (config.isEmpty()) {
            return Publishers.just(new Exception("No ConnectConfig found for cluster [" + cluster + "]"));
        }

        // mutate the request with proper URL and Authent
        HttpRequest<?> mutatedRequest = mutateKafkaConnectRequest(request, config.get().getConnect());
        // call it
        return client.proxy(mutatedRequest);
        // If required to modify the response, use this
        /* return Publishers.map(client.proxy(mutatedRequest),
                response -> response.header("X-My-Response-Header", "YYY"));*/
    }

    public MutableHttpRequest<?> mutateKafkaConnectRequest(HttpRequest<?> request, KafkaAsyncExecutorConfig.ConnectConfig connectConfig) {

        URI newURI = URI.create(connectConfig.getUrl());
        return request.mutate()
                .uri(b -> b
                        .scheme(newURI.getScheme())
                        .host(newURI.getHost())
                        .port(newURI.getPort())
                        .replacePath(StringUtils.prependUri(
                                newURI.getPath(),
                                request.getPath().substring(KafkaConnectClientProxy.CONNECT_PROXY_PREFIX.length())
                        ))
                )
                .basicAuth(connectConfig.getBasicAuthUsername(), connectConfig.getBasicAuthPassword());
    }
}