package com.michelin.ns4kafka.services.conduktor.entities;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class PlatformGroup {
    private String groupId;
    private String name;
    private String description="STOP IT";
    private List<String> externalGroups;

}
