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
package org.apache.pinot.common.messages;

import com.google.common.base.Preconditions;
import java.util.UUID;
import org.apache.helix.model.Message;

/**
 * This Helix message is sent from the controller to the servers to remove TableDataManager when the table is deleted.
 */
public class TableDeletionMessage extends Message {
  public static final String DELETE_TABLE_MSG_SUB_TYPE = "DELETE_TABLE";

  public TableDeletionMessage(String tableNameWithType) {
    super(MessageType.USER_DEFINE_MSG, UUID.randomUUID().toString());
    setResourceName(tableNameWithType);
    setMsgSubType(DELETE_TABLE_MSG_SUB_TYPE);
    // Give it infinite time to process the message, as long as session is alive
    setExecutionTimeout(-1);
  }

  public TableDeletionMessage(Message message) {
    super(message.getRecord());
    String msgSubType = message.getMsgSubType();
    Preconditions.checkArgument(msgSubType.equals(DELETE_TABLE_MSG_SUB_TYPE),
        "Invalid message sub type: " + msgSubType + " for TableDeletionMessage");
  }
}
