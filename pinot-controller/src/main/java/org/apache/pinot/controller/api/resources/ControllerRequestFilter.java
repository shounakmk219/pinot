package org.apache.pinot.controller.api.resources;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;
import org.apache.pinot.common.exception.DatabaseNotFoundException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Provider
@PreMatching
public class ControllerRequestFilter implements ContainerRequestFilter {

  public static final Logger LOGGER = LoggerFactory.getLogger(ControllerRequestFilter.class);
  private static final List<String> TABLE_NAME_KEYS = List.of("tableName", "tableNameWithType", "schemaName");

  @Inject
  PinotHelixResourceManager _resourceManager;

  @Override
  public void filter(ContainerRequestContext requestContext)
      throws IOException {
    // replaces value of "database" query param and header with databaseId
    translateDatabaseName(requestContext);
    // uses the databaseId from query param or header to translate table name to FQN
    translateTableName(requestContext);
  }

  private void translateDatabaseName(ContainerRequestContext requestContext) {
    MultivaluedMap<String, String> queryParams = requestContext.getUriInfo().getQueryParameters();
    String currentUri = requestContext.getUriInfo().getRequestUri().toString();
    if (requestContext.getHeaders().containsKey(Constants.DATABASE)) {
      String databaseName = requestContext.getHeaderString(Constants.DATABASE);
      String databaseId = _resourceManager.getDatabaseByName(databaseName).getId();
      // replace "database" header value with databaseId
      requestContext.getHeaders().replace(Constants.DATABASE, List.of(databaseId));
    }
    if (queryParams.containsKey(Constants.DATABASE)) {
      String databaseName = queryParams.getFirst(Constants.DATABASE);
      String databaseId = _resourceManager.getDatabaseByName(databaseName).getId();
      // replace "database" query param value with databaseId
      String updatedUri = currentUri.replaceAll(String.format("%s=%s", Constants.DATABASE, databaseName),
          String.format("%s=%s", Constants.DATABASE, databaseId));
      try {
        requestContext.setRequestUri(new URI(updatedUri));
      } catch (URISyntaxException e) {
        LOGGER.error("Unable to translate the database value from {} to {}", databaseName, databaseId);
      }
    }
  }

  private void translateTableName(ContainerRequestContext requestContext) {
    MultivaluedMap<String, String> queryParams = requestContext.getUriInfo().getQueryParameters();
    String uri = requestContext.getUriInfo().getRequestUri().toString();
    String databaseId = null;
    if (requestContext.getHeaders().containsKey(Constants.DATABASE)) {
      databaseId = requestContext.getHeaderString(Constants.DATABASE);
    }
    if (queryParams.containsKey(Constants.DATABASE)) {
      databaseId = queryParams.getFirst(Constants.DATABASE);
    }
    for (String key : TABLE_NAME_KEYS) {
      if (queryParams.containsKey(key)) {
        String tableName = queryParams.getFirst(key);
        String translatedTableName = null;
        try {
          translatedTableName = _resourceManager.getFullyQualifiedTableName(tableName, databaseId);
        } catch (DatabaseNotFoundException e) {
          LOGGER.error(String.format("Table name translation failed. %s", e.getMessage()));
          translatedTableName = tableName;
        }
        // table is not part of default database
        if (!translatedTableName.equals(tableName)) {
          uri = uri.replaceAll(String.format("%s=%s", key, tableName),
              String.format("%s=%s", key, translatedTableName));
          try {
            requestContext.setRequestUri(new URI(uri));
          } catch (URISyntaxException e) {
            LOGGER.error("Unable to translate the table name from {} to {}", tableName, translatedTableName);
          }
        }
        break;
      }
    }
  }
}
