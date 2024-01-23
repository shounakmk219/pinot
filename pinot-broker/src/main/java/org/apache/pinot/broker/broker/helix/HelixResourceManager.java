package org.apache.pinot.broker.broker.helix;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


public class HelixResourceManager {
  private final HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;
  private TableCache _tableCache;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final String _clusterName;

  public HelixResourceManager(HelixManager helixZkManager, HelixAdmin helixAdmin, String clusterName,
      TableCache tableCache) {
    _helixZkManager = helixZkManager;
    _helixAdmin = helixAdmin;
    _propertyStore = _helixZkManager.getHelixPropertyStore();
    _clusterName = clusterName;
    _tableCache = tableCache;
  }

  /**
   * Get all resource names.
   *
   * @return List of resource names
   */
  public List<String> getAllResources() {
    return _helixAdmin.getResourcesInCluster(_clusterName);
  }

  public List<String> getDatabaseNames() {
    Set<String> databaseNames = new HashSet<>();
    for (String resourceName : getAllResources()) {
      if (TableNameBuilder.isTableResource(resourceName)) {
        String[] split = resourceName.split("\\.");
        databaseNames.add(split.length == 2 ? split[0] : null);
      }
    }
    return new ArrayList<>(databaseNames);
  }

  public String getActualTableName(String tableName, String databaseName) {
    String[] tableSplit = tableName.split("\\.");
    if (tableSplit.length > 2) {
      // TODO revisit this handling
      throw new IllegalArgumentException(String.format("Table name %s contains more than 1 '.' in it", tableName));
    } else if (tableSplit.length == 2) {
      databaseName = tableSplit[0];
      tableName = tableSplit[1];
    }
    if (databaseName != null && !databaseName.isBlank()) {
      tableName = String.format("%s.%s", databaseName, tableName);
    }
    if (_tableCache.isIgnoreCase()) {
      String actualTableName = _tableCache.getActualTableName(tableName);
      return actualTableName != null ? actualTableName : tableName;
    } else {
      return tableName;
    }
  }
}

