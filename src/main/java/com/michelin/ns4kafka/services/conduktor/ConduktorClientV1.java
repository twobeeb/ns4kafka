package com.michelin.ns4kafka.services.conduktor;

import com.michelin.ns4kafka.services.conduktor.entities.PlatformGroup;
import com.michelin.ns4kafka.services.conduktor.entities.PlatformPermission;
import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import io.micronaut.http.client.annotation.Client;

import java.util.List;

@Client(value = "${ns4kafka.conduktor.url}/public/v1/")
@Header(name = HttpHeaders.AUTHORIZATION, value = "Bearer ${ns4kafka.conduktor.token}")
public interface ConduktorClientV1 {
    @Get("/groups")
    List<PlatformGroup> listGroups();

    @Post("/groups")
    void createGroup(@Body PlatformGroup data);

    @Put("/groups/{group}")
    void updateGroup(String group, @Body PlatformGroup data);

    @Delete("/groups/{group}")
    void deleteGroup(String group);

    @Get("/groups/{group}/permissions")
    List<PlatformPermission> listGroupPermissions(String group);
    @Put("/groups/{group}/permissions")
    void updateGroupPermissions(String group, @Body List<PlatformPermission> permissions);

}
