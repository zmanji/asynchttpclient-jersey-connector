package com.zameermanji.jeresy.connectors.asynchttpclient.connector;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.Configuration;
import org.glassfish.jersey.client.spi.Connector;
import org.glassfish.jersey.client.spi.ConnectorProvider;

public class AsyncHttpClientProvider implements ConnectorProvider {
  @Override
  public Connector getConnector(Client client, Configuration runtimeConfig) {
    return new AsyncHttpClientConnector(client, runtimeConfig);
  }
}
