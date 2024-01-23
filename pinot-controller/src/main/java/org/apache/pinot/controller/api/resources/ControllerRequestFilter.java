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
    // uses the database name from query param or header to build actual table name
    translateTableName(requestContext);
  }

  private void translateTableName(ContainerRequestContext requestContext) {
    MultivaluedMap<String, String> queryParams = requestContext.getUriInfo().getQueryParameters();
    String uri = requestContext.getUriInfo().getRequestUri().toString();
    String databaseName = null;
    if (requestContext.getHeaders().containsKey(Constants.DATABASE)) {
      databaseName = requestContext.getHeaderString(Constants.DATABASE);
    }
    for (String key : TABLE_NAME_KEYS) {
      if (queryParams.containsKey(key)) {
        String tableName = queryParams.getFirst(key);
        String actualTableName = _resourceManager.getActualTableName(tableName, databaseName);
        // table is not part of default database
        if (!actualTableName.equals(tableName)) {
          uri = uri.replaceAll(String.format("%s=%s", key, tableName),
              String.format("%s=%s", key, actualTableName));
          try {
            requestContext.setRequestUri(new URI(uri));
          } catch (URISyntaxException e) {
            LOGGER.error("Unable to translate the table name from {} to {}", tableName, actualTableName);
          }
        }
        break;
      }
    }
  }
}
