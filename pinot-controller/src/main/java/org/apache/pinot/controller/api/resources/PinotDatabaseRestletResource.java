/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.databind.node.ArrayNode;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.exception.DatabaseAlreadyExistsException;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.ManualAuthorization;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.config.DatabaseConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = Constants.DATABASE_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class PinotDatabaseRestletResource {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotDatabaseRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/databases")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_DATABASE)
  @ApiOperation(value = "List all database names", notes = "Lists all database names")
  public String listDatabaseNames() {
    List<String> dbNames = _pinotHelixResourceManager.getDatabaseNames();
    ArrayNode ret = JsonUtils.newArrayNode();
    if (dbNames != null) {
      for (String db : dbNames) {
        ret.add(db);
      }
    }
    return ret.toString();
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/databases")
  @ApiOperation(value = "Add a new database", notes = "Adds a new database")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Successfully created database"),
      @ApiResponse(code = 409, message = "database already exists"),
      @ApiResponse(code = 400, message = "Missing or invalid request body")
  })
  @ManualAuthorization // performed after parsing schema
  public ConfigSuccessResponse addDatabase(
      String dbJsonString,
      @Context HttpHeaders httpHeaders,
      @Context Request request) {
    try {
      Pair<DatabaseConfig, Map<String, Object>> databaseAndUnrecognizedProperties =
          JsonUtils.stringToObjectAndUnrecognizedProperties(dbJsonString, DatabaseConfig.class);
      DatabaseConfig database = databaseAndUnrecognizedProperties.getLeft();
      _pinotHelixResourceManager.addDatabase(database);
      return new ConfigSuccessResponse(database.getDatabaseName() + " successfully created", databaseAndUnrecognizedProperties.getRight());
    } catch (IOException e) {
      String msg = String.format("Invalid database config json string: %s", dbJsonString);
      throw new ControllerApplicationException(LOGGER, msg, Response.Status.BAD_REQUEST, e);
    } catch (DatabaseAlreadyExistsException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT, e);
    }
  }
}
