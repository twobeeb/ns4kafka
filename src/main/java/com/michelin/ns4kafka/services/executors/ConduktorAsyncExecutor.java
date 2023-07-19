package com.michelin.ns4kafka.services.executors;

import com.michelin.ns4kafka.config.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.services.AccessControlEntryService;
import com.michelin.ns4kafka.services.conduktor.ConduktorClientV1;
import com.michelin.ns4kafka.services.conduktor.entities.PlatformGroup;
import com.michelin.ns4kafka.services.conduktor.entities.PlatformPermission;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.util.StringUtils;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.michelin.ns4kafka.models.AccessControlEntry.ResourceType;

@Slf4j
@Requires(property = "ns4kafka.conduktor.enabled", value = StringUtils.TRUE)
@Singleton
public class ConduktorAsyncExecutor {

    /**
     * We might want to format the group ids with a prefix like "namespace-"
     * so that we ignore other existing groups during the reconciliation
     */
    public static final String CDK_GROUP_PREFIX = "namespace-";

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
    @Inject
    AccessControlEntryService accessControlEntryService;

    public void run() {
        synchronizeGroups();
    }

    private void synchronizeGroups(){
        log.debug("Starting User Groups collection for Conduktor synchronization");
        // List groups in ns4kafka
        //var test = conduktorClient.listGroupPermissions("copain");

        List<Namespace> namespaces = kafkaAsyncExecutorConfigList.stream()
                .filter(KafkaAsyncExecutorConfig::isManageConduktor)
                .flatMap(config -> namespaceRepository.findAllForCluster(config.getName()).stream())
                .toList();
        // List groups in Conduktor
        var groups = conduktorClient.listGroups();
        //var ns = namespaces.get(0);

        List<PlatformGroup> cdkGroups = conduktorClient.listGroups()
                .stream()
                .filter(platformGroup -> platformGroup.getGroupId().startsWith(CDK_GROUP_PREFIX))
                .toList();
        /*var gp = PlatformGroup.builder()
                //.id(namespaceToGroupId(ns))
                .slug(namespaceToGroupId(ns).toLowerCase(Locale.ROOT))
                .name(String.format(GROUP_NAME, ns.getMetadata().getName()))
                .description("I want this to be optional")
                .externalGroups(List.of("LDAP-"+ns.getMetadata().getName().toUpperCase(Locale.ROOT)))
                //.externalGroups(Optional.of(ns.getMetadata().getLabels().get("gitlab")).stream().toList())
                .build();
        var resp = conduktorClient.createGroup(gp);*/

        var toCreate = namespaces.stream()
                .filter(namespace -> cdkGroups.stream().noneMatch(cdkGroup -> cdkGroup.getGroupId().equals(namespaceToGroupId(namespace))))
                .toList();
        var toDelete = cdkGroups.stream()
                .filter(cdkGroup -> namespaces.stream().noneMatch(namespace -> namespaceToGroupId(namespace).equals(cdkGroup.getGroupId())))
                .toList();
        var toCheck = cdkGroups.stream()
                .filter(cdkGroup -> namespaces.stream().anyMatch(namespace -> namespaceToGroupId(namespace).equals(cdkGroup.getGroupId())))
                .toList();
        if (log.isDebugEnabled()) {
            toCreate.forEach(namespace -> log.debug("Groups to create: " + namespaceToGroupId(namespace)));
            toDelete.forEach(group -> log.debug("Groups to delete: " + group.getGroupId()));
        }
        toCreate.forEach(namespace -> {
            conduktorClient.createGroup(
                    PlatformGroup.builder()
                            .groupId(namespaceToGroupId(namespace))
                            .description("PLEASE STOP")
                            .name(String.format(GROUP_NAME, namespace.getMetadata().getName()))
                            .externalGroups(Optional.of(namespace.getMetadata().getLabels().get("ldap-group")).stream().toList())
                            .build());
            synchronizePermissions(namespace);
        });

        toDelete.forEach(platformGroup -> conduktorClient.deleteGroup(platformGroup.getGroupId()));
        toCheck.forEach(platformGroup -> {
            var ns = namespaces.stream()
                    .filter(namespace -> namespaceToGroupId(namespace).equals(platformGroup.getGroupId()))
                    .findFirst()
                    .get();
            if(hasChanges(ns, platformGroup))
                conduktorClient.updateGroup(platformGroup.getGroupId(),
                        PlatformGroup.builder()
                                //.groupId(namespaceToGroupId(namespace))
                                .description("PLEASE STOP")
                                .name(String.format(GROUP_NAME, ns.getMetadata().getName()))
                                .externalGroups(Optional.of(ns.getMetadata().getLabels().get("ldap-group")).stream().toList())
                                .build());
            synchronizePermissions(ns);

        });


    }

    private void synchronizePermissions(Namespace namespace) {
        // Conduktor provide 3 useful verbs
        // PUT (replace all),
        // DELETE (remove individual) and
        // PATCH (add individual).
        // As a first draft, we just check if there's something changed.
        // If so, we just PUT all the permissions
        // Optimisations welcome.
        var groupId = namespaceToGroupId(namespace);
        var acls = accessControlEntryService.findAllForNamespace(namespace);

        // scope of CDK sync
        var KEEP_RESOURCES = List.of(ResourceType.TOPIC,
                ResourceType.GROUP,
                ResourceType.CONNECT,
                ResourceType.SCHEMA);
        var cdk_actual_acls = conduktorClient.listGroupPermissions(groupId);
        // translate into Conduktor ACLS
        var cdk_expected_acls_0 = acls.stream()
                .filter(acl -> KEEP_RESOURCES.contains(acl.getSpec().getResourceType()))
                .flatMap(acl -> aclToPermission(acl).stream());
        var cdk_expected_acls_1 = acls.stream()
                .map(accessControlEntry -> accessControlEntry.getMetadata().getCluster())
                .distinct()
                .map(s -> {
                    var clusterPermission = new PlatformPermission.ClusterPermission();
                    clusterPermission.setClusterId(s);
                    clusterPermission.setPermissions(List.of("clusterViewBroker", "clusterViewACL"));
                    return clusterPermission;
                });
        var cdk_expected_acls = Stream.concat(cdk_expected_acls_0, cdk_expected_acls_1).toList();
        conduktorClient.updateGroupPermissions(groupId, cdk_expected_acls);
    }

    private List<PlatformPermission> aclToPermission(AccessControlEntry acl) {
        switch (acl.getSpec().getResourceType()) {
            case TOPIC:
                var topicPermission = new PlatformPermission.TopicPermission();
                topicPermission.setClusterId(acl.getMetadata().getCluster());
                topicPermission.setTopicPattern(acl.getSpec().getResource()+(acl.getSpec().getResourcePatternType().equals(AccessControlEntry.ResourcePatternType.PREFIXED)?"*":""));
                topicPermission.setPermissions(List.of("topicConsume", "topicViewConfig"));
                var subjectPermission = new PlatformPermission.SubjectPermission();
                subjectPermission.setClusterId(acl.getMetadata().getCluster());
                subjectPermission.setSubjectPattern(acl.getSpec().getResource()+(acl.getSpec().getResourcePatternType().equals(AccessControlEntry.ResourcePatternType.PREFIXED)?"*":""));
                subjectPermission.setPermissions(List.of("subjectView"));
                return List.of(topicPermission, subjectPermission);
            case GROUP:
                var groupPermission = new PlatformPermission.GroupPermission();
                groupPermission.setClusterId(acl.getMetadata().getCluster());
                groupPermission.setConsumerGroupPattern(acl.getSpec().getResource()+(acl.getSpec().getResourcePatternType().equals(AccessControlEntry.ResourcePatternType.PREFIXED)?"*":""));
                groupPermission.setPermissions(List.of("consumerGroupView", "consumerGroupReset"));
                return List.of(groupPermission);
            case CONNECT:
                var connectorPermission = new PlatformPermission.ConnectorPermission();
                connectorPermission.setClusterId(acl.getMetadata().getCluster());
                connectorPermission.setConnectorId(null);
                connectorPermission.setConnectNamePattern(acl.getSpec().getResource()+(acl.getSpec().getResourcePatternType().equals(AccessControlEntry.ResourcePatternType.PREFIXED)?"*":""));
                connectorPermission.setPermissions(List.of("kafkaConnectorStatus", "kafkaConnectRestart", "kafkaConnectPauseResume"));
                return List.of(connectorPermission);
            default:
                return List.of();
        }
    }

    private String namespaceToGroupId(Namespace namespace){

        // This will give us `namespace-rf4mgbl0`
        return String.format(CDK_GROUP_PREFIX +"%s", namespace.getMetadata().getName()).toLowerCase();
    }
    private boolean hasChanges(Namespace namespace, PlatformGroup group){
        if(!namespace.getMetadata().getLabels().get("ldap-group").equals(group.getExternalGroups().get(0)))
            return true;
        // if name is different
        // if group owner is different
        // if whatever namespace value must be synced with Conduktor
        // otherwise false
        return false;
    }
}
