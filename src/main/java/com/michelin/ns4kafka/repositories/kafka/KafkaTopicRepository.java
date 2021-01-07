package com.michelin.ns4kafka.repositories.kafka;

import com.fasterxml.jackson.databind.deser.std.MapEntryDeserializer;
import com.michelin.ns4kafka.controllers.TopicController;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ResourceSecurityPolicy;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.TopicRepository;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Value;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Singleton
@KafkaListener(
        offsetReset = OffsetReset.EARLIEST,
        offsetStrategy = OffsetStrategy.DISABLED,
        properties = @Property(name = ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, value = "false")
)
public class KafkaTopicRepository extends KafkaStore<Topic> implements TopicRepository {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicRepository.class);
    @Inject
    NamespaceRepository namespaceRepository;

    public KafkaTopicRepository(@Value("${ns4kafka.store.kafka.topics.prefix}.topics") String kafkaTopic,
                                      @KafkaClient("topics-producer") Producer<String, Topic> kafkaProducer) {
        super(kafkaTopic, kafkaProducer);
    }

    @io.micronaut.configuration.kafka.annotation.Topic(value = "${ns4kafka.store.kafka.topics.prefix}.topics")
    void receive(ConsumerRecord<String, Topic> record) {
        super.receive(record);
    }

    @Override
    public List<Topic> findAllForNamespace(String namespace, TopicController.TopicListLimit limit) {
        Optional<Namespace> namespaceOptional = namespaceRepository.findByName(namespace);
        if(namespaceOptional.isPresent()) {
            return kafkaStore.values()
                    .stream()
                    .filter(topic -> topic.getMetadata().getCluster().equals(namespaceOptional.get().getCluster()) &&
                            namespaceOptional.get()
                            .getPolicies()
                            .stream()
                            .filter(resourceSecurityPolicy -> {
                                switch(limit){
                                    case OWNED:
                                        return resourceSecurityPolicy.getSecurityPolicy() == ResourceSecurityPolicy.SecurityPolicy.OWNER;
                                    case ACCESS_GIVEN:
                                        return resourceSecurityPolicy.getSecurityPolicy() != ResourceSecurityPolicy.SecurityPolicy.OWNER;
                                    case ALL:
                                    default:
                                        return true;
                                }
                            })
                            .anyMatch(resourceSecurityPolicy -> matchTopicAgainstPolicy(topic, resourceSecurityPolicy))
                    )
                    .collect(Collectors.toList());
        } else {
            return Collections.EMPTY_LIST;
        }
    }

    @Override
    public List<Topic> findAllForCluster(String cluster) {
        return kafkaStore.values()
                .stream()
                .filter(topic -> topic.getMetadata().getCluster().equals(cluster))
                .collect(Collectors.toList());
    }

    @Override
    public Optional<Topic> findByName(String namespace, String topic) {
        return findAllForNamespace(namespace, TopicController.TopicListLimit.ALL)
                .stream()
                .filter( t -> t.getMetadata().getName().equals(topic))
                .findFirst();

    }

    private boolean matchTopicAgainstPolicy(Topic topic, ResourceSecurityPolicy resourceSecurityPolicy){
        if(resourceSecurityPolicy.getResourceType() == ResourceSecurityPolicy.ResourceType.TOPIC){
            switch (resourceSecurityPolicy.getResourcePatternType()){
                case REGEXP:
                    return topic.getMetadata().getName().matches(resourceSecurityPolicy.getResource());
                case PREFIXED:
                    return topic.getMetadata().getName().startsWith(resourceSecurityPolicy.getResource());
                case LITERAL:
                    return topic.getMetadata().getName().equals(resourceSecurityPolicy.getResource());
            }
        }
        return false;

    }

    @Override
    public Topic create(Topic topic) {
        return this.produce(topic.getMetadata().getCluster()+"/"+topic.getMetadata().getName(), topic);
    }
}
