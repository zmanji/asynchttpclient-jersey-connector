package com.zameermanji.jeresy.connectors.asynchttpclient.connector;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Response.StatusType;

import static org.asynchttpclient.Dsl.asyncHttpClient;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.asynchttpclient.AsyncCompletionHandlerBase;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;
import org.asynchttpclient.request.body.generator.BodyChunk;
import org.asynchttpclient.request.body.generator.FeedableBodyGenerator;
import org.asynchttpclient.request.body.generator.QueueBasedFeedableBodyGenerator;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.ClientRequest;
import org.glassfish.jersey.client.ClientResponse;
import org.glassfish.jersey.client.spi.AsyncConnectorCallback;
import org.glassfish.jersey.client.spi.Connector;
import org.glassfish.jersey.message.internal.OutboundMessageContext;
import org.glassfish.jersey.message.internal.Statuses;

public class AsyncHttpClientConnector implements Connector {
  private AsyncHttpClient asyncHttpClient;

  public AsyncHttpClientConnector(Client client, Configuration configuration) {
    DefaultAsyncHttpClientConfig.Builder ahcConfig = new DefaultAsyncHttpClientConfig.Builder();
    // TODO(zmanji): Figure out how to pass in a config here or expose the other options

    asyncHttpClient = asyncHttpClient(ahcConfig);
  }

  private RequestBuilder buildRequest(ClientRequest request) {
    RequestBuilder base = new RequestBuilder();
    base.setMethod(request.getMethod());
    base.setUrl(request.getUri().toString());

    Integer readTimeout = Optional.ofNullable(request.resolveProperty(ClientProperties.READ_TIMEOUT, Integer.class)).orElse(0);
    Boolean followRedirects = Optional.ofNullable(request.resolveProperty(ClientProperties.FOLLOW_REDIRECTS, Boolean.class)).orElse(true);

    base.setFollowRedirect(followRedirects);
    base.setReadTimeout(readTimeout);

    for (Map.Entry<String, List<String>> headerEntry : request.getStringHeaders().entrySet()) {
      base.setHeader(headerEntry.getKey(), headerEntry.getValue());
    }

    if (request.hasEntity()) {
      // Buffer so we minimize the number of puts into the body generator queue
      request.enableBuffering();
      // Buffer at most one buffer size of data and block the writer (jersey background thread) until netty consumes it.
      FeedableBodyGenerator bodyGenerator = new BlockingBoundedQueueFeedableBodyGenerator(1);

      request.setStreamProvider(new OutboundMessageContext.StreamProvider() {
        @Override
        public OutputStream getOutputStream(int contentLength) throws IOException {
          return new FeedableBodyGeneratorOutputStream(bodyGenerator);
        }
      });

      base.setBody(bodyGenerator);
    }

    return base;
  }

  private ClientResponse makeResponse(ClientRequest request, Response response) {
    StatusType status = Statuses.from(response.getStatusCode(), response.getStatusText());
    ClientResponse clientResponse = new ClientResponse(status, request);

    response.getHeaders().iteratorAsString().forEachRemaining(new Consumer<Map.Entry<String, String>>() {
      @Override
      public void accept(Map.Entry<String, String> stringStringEntry) {
        clientResponse.header(stringStringEntry.getKey(), stringStringEntry.getValue());
      }
    });

    try {
      clientResponse.setResolvedRequestUri(response.getUri().toJavaNetURI());
    } catch (URISyntaxException e) {
      throw new ProcessingException(e);
    }

    // TODO(zmanji): Consider using an AsyncHandler to feed the entity stream.
    clientResponse.setEntityStream(response.getResponseBodyAsStream());

    return clientResponse;
  }

  @Override
  public ClientResponse apply(ClientRequest request) {
    CompletableFuture<ClientResponse> future = asyncHttpClient.executeRequest(buildRequest(request))
            .toCompletableFuture().thenApply(new Function<Response, ClientResponse>() {
      @Override
      public ClientResponse apply(Response response) {
        return makeResponse(request, response);
      }
    });

    try {
      if (request.hasEntity()) {
        request.writeEntity();
      }
      return future.get();
    } catch (ProcessingException e) {
      throw e;
    } catch (Exception e) {
      throw new ProcessingException(e);
    }
  }

  @Override
  public Future<?> apply(ClientRequest request, AsyncConnectorCallback callback) {
    ListenableFuture<Response> f = asyncHttpClient.executeRequest(buildRequest(request), new AsyncCompletionHandlerBase() {
      @Override
      public Response onCompleted(Response response) throws Exception {
        callback.response(makeResponse(request, response));
        return response;
      }
      @Override
      public void onThrowable(Throwable t) {
        callback.failure(t);
      }
    });

    try {
      if (request.hasEntity()) {
        request.writeEntity();
      }
    } catch (IOException e) {
      f.abort(e);
      callback.failure(e);
    }
    return f;
  }

  @Override
  public String getName() {
    return asyncHttpClient.getConfig().getUserAgent();
  }

  @Override
  public void close() {
    try {
      asyncHttpClient.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Like the BoundedQueueFeedableBodyGenerator but block the thread when feeding.
   */
  private static class BlockingBoundedQueueFeedableBodyGenerator extends QueueBasedFeedableBodyGenerator<BlockingQueue<BodyChunk>> {
    BlockingBoundedQueueFeedableBodyGenerator(int capacity) {
      super(new ArrayBlockingQueue<>(capacity, true));
    }

    @Override
    protected boolean offer(BodyChunk chunk) throws InterruptedException {
      queue.put(chunk);
      return true;
    }
  }

  /**
   * An output stream that feeds the bytes into a generator. Need to to bridge the entity writing from jersey to the
   * asynchttpclient request body.
   */
  private static class FeedableBodyGeneratorOutputStream extends OutputStream {
    private final FeedableBodyGenerator generator;

    FeedableBodyGeneratorOutputStream(FeedableBodyGenerator generator) {
      this.generator = generator;
    }

    @Override
    public void write(int b) throws IOException {
      byte[] buffer = new byte[1];
      buffer[0] = (byte) b;
      write(buffer);
    }

    @Override
    public void write(byte b[]) throws IOException {
      write(b, 0, b.length);
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
      try {
        ByteBuf buf = Unpooled.wrappedBuffer(b, off, len);
        boolean fed = generator.feed(buf, false);
        if (!fed) {
          throw new IllegalStateException("Feeding failed");
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void close() throws IOException {
      try {
        boolean fed = generator.feed(Unpooled.EMPTY_BUFFER, true);
        if (!fed) {
          throw new IllegalStateException("Feeding failed");
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }
}
