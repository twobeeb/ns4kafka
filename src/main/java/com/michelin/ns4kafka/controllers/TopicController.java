package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.*;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.TopicRepository;
import io.micronaut.http.annotation.*;
import io.swagger.v3.oas.annotations.tags.Tag;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
@Tag(name = "Topics")
@Controller("/api/namespaces/{namespace}/topics")
public class TopicController {
    @Inject
    NamespaceRepository namespaceRepository;
    @Inject
    TopicRepository topicRepository;

    /**
     * @param namespace The namespace to query
     * @param limit Optional restricts the scope of the return list
     * @return The list of all Topics names available for that namespace (owned and accessible)
     */
    @Get
    public List<String> list(String namespace, @Nullable @QueryValue TopicListLimit limit){
        //TODO TopicList
        if(limit==null){
            limit=TopicListLimit.ALL;
        }
        return topicRepository.findAllForNamespace(namespace, limit)
                .stream()
                .map(topic -> topic.getMetadata().getName())
                .collect(Collectors.toList());
    }

    @Get("{topic}")
    public Optional<Topic> getTopic(String namespace, String topic){
        return topicRepository.findByName(namespace, topic);
    }
    @Post("{topic}")
    public Topic create(String namespace, @Body Topic topic){
        //TODO
        // 0. (Done) User Allowed ?
        // 1. Request Allowed ?
        // 2. Request Valid ?
        // 3. Store
        Namespace ns = namespaceRepository.findByName(namespace).orElseThrow(() -> new RuntimeException("Namespace not found"));
        //AdminClient.create()

        //pour les topics dont je suis owner, somme d'usage
        // pour le topic à créer usageTopic
        // si somme + usageTopic > quota KO

        return topicRepository.create(topic);


    }

    public enum TopicListLimit {
        ALL,
        OWNED,
        ACCESS_GIVEN
    }
}
