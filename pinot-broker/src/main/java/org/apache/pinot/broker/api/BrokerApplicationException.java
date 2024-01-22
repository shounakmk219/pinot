package org.apache.pinot.broker.api;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;


public class BrokerApplicationException extends WebApplicationException {
  public BrokerApplicationException(String message, Response.Status status) {
    super(message, status);
  }
}
