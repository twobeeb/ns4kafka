package com.michelin.ns4kafka.services.conduktor.entities;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class PlatformGroup {
    private String id;
    private String name;
    private String description;
    private List<String> external_groups;

}
