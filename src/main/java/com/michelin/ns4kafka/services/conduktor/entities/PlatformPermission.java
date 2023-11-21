package com.michelin.ns4kafka.services.conduktor.entities;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.michelin.ns4kafka.validation.ResourceValidator;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.List;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "resourceType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = PlatformPermission.ClusterPermission.class, name = "Cluster"),
        @JsonSubTypes.Type(value = PlatformPermission.TopicPermission.class, name = "Topic"),
        @JsonSubTypes.Type(value = PlatformPermission.GroupPermission.class, name = "ConsumerGroup"),
        @JsonSubTypes.Type(value = PlatformPermission.SubjectPermission.class, name = "Subject"),
        @JsonSubTypes.Type(value = PlatformPermission.ConnectorPermission.class, name = "KafkaConnect")
})
@Data
@SuperBuilder
@AllArgsConstructor
@NoArgsConstructor
public abstract class PlatformPermission {

    private String clusterId;
    private List<String> permissions;

    @Data
    @NoArgsConstructor
    public static class ClusterPermission extends PlatformPermission {
        private String resourceType="Cluster";
    }
    @Data
    @NoArgsConstructor
    public static class TopicPermission extends PlatformPermission {
        private String resourceType="Topic";
        private String topicPattern;
    }
    @Data
    @NoArgsConstructor
    public static class GroupPermission extends PlatformPermission {
        private String resourceType="ConsumerGroup";
        private String consumerGroupPattern;
    }
    @Data
    @NoArgsConstructor
    public static class ConnectorPermission extends PlatformPermission {
        private String resourceType="KafkaConnect";
        private String connectClusterId;
        private String connectorNamePattern;
    }
    @Data
    @NoArgsConstructor
    public static class SubjectPermission extends PlatformPermission {
        private String resourceType="Subject";
        private String subjectPattern;
    }
}
