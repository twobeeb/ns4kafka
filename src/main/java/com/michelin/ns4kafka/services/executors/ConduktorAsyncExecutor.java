package com.michelin.ns4kafka.services.executors;

import com.michelin.ns4kafka.config.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.services.conduktor.ConduktorClientV1;
import com.michelin.ns4kafka.services.conduktor.entities.PlatformGroup;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.util.StringUtils;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

@Slf4j
@Requires(property = "ns4kafka.conduktor.enabled", value = StringUtils.TRUE)
@Singleton
public class ConduktorAsyncExecutor {

    /**
     * We might want to format the group ids with a prefix like "namespace-"
     * so that we ignore other existing groups during the reconciliation
     */
    public static final String GROUP_ID_PREFIX = "namespace-";

    /**
     * Name of the group in Conduktor
     */
    public static final String GROUP_NAME = "Project %s";
    @Inject
    private List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfigList;

    @Inject
    ConduktorClientV1 conduktorClient;
    @Inject
    NamespaceRepository namespaceRepository;

    public void run() {
        synchronizeGroups();
    }

    private void synchronizeGroups(){
        log.debug("Starting User Groups collection for Conduktor synchronization");
        // List groups in ns4kafka

        List<Namespace> namespaces = kafkaAsyncExecutorConfigList.stream()
                .filter(KafkaAsyncExecutorConfig::isManageConduktor)
                .flatMap(config -> namespaceRepository.findAllForCluster(config.getName()).stream())
                .toList();
        // List groups in Conduktor
        List<PlatformGroup> cdkGroups = conduktorClient.listGroups()
                .stream()
                .filter(platformGroup -> platformGroup.getId().startsWith(GROUP_ID_PREFIX))
                .toList();

        var toCreate = namespaces.stream()
                .filter(namespace -> cdkGroups.stream().noneMatch(cdkGroup -> cdkGroup.getId().equals(namespaceToGroupId(namespace))))
                .toList();
        var toDelete = cdkGroups.stream()
                .filter(cdkGroup -> namespaces.stream().noneMatch(namespace -> namespaceToGroupId(namespace).equals(cdkGroup.getId())))
                .toList();
        var toCheck = cdkGroups.stream()
                .filter(cdkGroup -> namespaces.stream().anyMatch(namespace -> namespaceToGroupId(namespace).equals(cdkGroup.getId())))
                .toList();
        if (log.isDebugEnabled()) {
            toCreate.forEach(namespace -> log.debug("Groups to create: " + namespaceToGroupId(namespace)));
            toDelete.forEach(group -> log.debug("Groups to delete: " + group.getId()));
        }
        toCreate.forEach(namespace -> conduktorClient.createGroup(
                PlatformGroup.builder()
                        .id(namespaceToGroupId(namespace))
                        .name(String.format(GROUP_NAME, namespace.getMetadata().getName()))
                        .external_groups(Optional.of(namespace.getMetadata().getLabels().get("gitlab")).stream().toList())
                        .build()));
        toDelete.forEach(platformGroup -> conduktorClient.deleteGroup(platformGroup.getId()));

    }
    private String namespaceToGroupId(Namespace namespace){

        // This will give us `namespace-rf4mgbl0`
        return String.format(GROUP_ID_PREFIX+"%s", namespace.getMetadata().getName());
    }
}
