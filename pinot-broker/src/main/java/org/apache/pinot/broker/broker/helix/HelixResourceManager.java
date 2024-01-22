package org.apache.pinot.broker.broker.helix;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.exception.DatabaseNotFoundException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.spi.config.DatabaseConfig;


public class HelixResourceManager {
  private final HelixManager _helixZkManager;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;

  public HelixResourceManager(HelixManager helixZkManager) {
    _helixZkManager = helixZkManager;
    _propertyStore = _helixZkManager.getHelixPropertyStore();
  }

  public List<String> getDatabaseNames() {
    return ZKMetadataProvider.getDatabases(_propertyStore).stream()
        .map(DatabaseConfig::getDatabaseName).collect(Collectors.toList());
  }

  public DatabaseConfig getDatabaseByName(String dbName) {
    return ZKMetadataProvider.getDatabases(_propertyStore).stream()
        .filter(db -> db.getDatabaseName().equals(dbName)).findFirst().orElse(null);
  }

  public String getFullyQualifiedTableName(String tableName, String databaseId)
      throws DatabaseNotFoundException {
    final String[] tableSplit = tableName.split("\\.");
    if (tableSplit.length > 2) {
      throw new IllegalArgumentException(String.format("Table name %s contains more than 1 '.' in it", tableName));
    } else if (tableSplit.length == 2) {
      databaseId = Optional.ofNullable(getDatabaseByName(tableSplit[0]))
          .orElseThrow(() -> new DatabaseNotFoundException(String.format("Database %s does not exist", tableSplit[0])))
          .getId();
      tableName = tableSplit[1];
    }
    if (databaseId == null || databaseId.isBlank()) {
      return tableName;
    }
    return String.format("%s.%s", databaseId, tableName);
  }
}
