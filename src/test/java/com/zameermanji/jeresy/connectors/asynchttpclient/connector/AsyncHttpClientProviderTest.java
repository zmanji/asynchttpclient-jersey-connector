package com.zameermanji.jeresy.connectors.asynchttpclient.connector;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.Produces;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.message.internal.MessageBodyProviderNotFoundException;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class AsyncHttpClientProviderTest extends JerseyTest {
  private static final String PATH = "/hello";
  private static final String SLOW_PATH = "/slow";
  private static final String MESSAGE = "Hello World!";
  private static final String LARGE_MESSAGE = "Large String Recieved";
  private static final int LARGE_STRING_SIZE = 1024 * 1024 * 32;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Path(SLOW_PATH)
  public static class SlowResource {
    @GET
    public String slow() {
      try {
        Thread.sleep(1000);
        return MESSAGE;
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Path(PATH)
  public static class HelloResource {
    @GET
    public String getHello() {
      return MESSAGE;
    }

    @POST
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.TEXT_PLAIN)
    public String postHello(String body) {
      if (body.equals("testing")) {
        return MESSAGE;
      } else if (body.length() == LARGE_STRING_SIZE) {
        return LARGE_MESSAGE;
      } else {
        return "Unexpected Body: " + body;
      }
    }
  }

  @Override
  protected Application configure() {
    set(TestProperties.CONTAINER_PORT, 0);
    return new ResourceConfig(HelloResource.class, SlowResource.class);
  }

  @Override
  protected void configureClient(ClientConfig config) {
    config.connectorProvider(new AsyncHttpClientProvider());
    // Limit concurrency to stress underlying nio
    config.property(ClientProperties.ASYNC_THREADPOOL_SIZE, 1);
    config.property(ClientProperties.BACKGROUND_SCHEDULER_THREADPOOL_SIZE, 1);
  }

  private String getBody(Response r) throws IOException {
    InputStream i = (InputStream) r.getEntity();
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    i.transferTo(result);
    return result.toString(StandardCharsets.UTF_8);
  }

  @Test()
  public void testGetSync() throws Exception {
    Response r = target(PATH).request().get();
    assertEquals(200, r.getStatus());

    String body = getBody(r);

    assertEquals(MESSAGE, body);
    r.close();
  }

  @Test()
  public void testGetAsync() throws Exception {
    Response r = target(PATH).request().async().get().get();
    assertEquals(200, r.getStatus());

    String body = getBody(r);

    assertEquals(MESSAGE, body);
    r.close();
  }

  @Test()
  public void testPostSync() throws Exception {
    Response r = target(PATH).request().post(Entity.text("testing"));
    assertEquals(200, r.getStatus());

    String body = getBody(r);

    assertEquals(MESSAGE, body);
    r.close();
  }

  @Test()
  public void testPostAsync() throws Exception {
    Response r = target(PATH).request().async().post(Entity.text("testing")).get();
    assertEquals(200, r.getStatus());

    String body = getBody(r);

    assertEquals(MESSAGE, body);
    r.close();
  }

  @Test()
  public void testPostLargeTextAsync() throws Exception {
    char[] chars = new char[LARGE_STRING_SIZE];
    Arrays.fill(chars, 'a');
    String str = new String(chars);

    Response r = target(PATH).request().async().post(Entity.text(str)).get();
    assertEquals(200, r.getStatus());

    String body = getBody(r);

    assertEquals(LARGE_MESSAGE, body);
    r.close();
  }

  @Test()
  public void testPostJsonAsync() throws Exception {
    Response r = target(PATH).request().async().post(Entity.json("{}")).get();
    assertEquals(415, r.getStatus());

    String body = getBody(r);

    assertEquals("", body);
    r.close();
  }

  @Test()
  public void testPostJsonPOJOAsyncFailure() throws Exception {
    Future<Response> f = target(PATH).request().async().post(Entity.json(new SomePOJO()));

    thrown.expect(ExecutionException.class);
    thrown.expectCause(instanceOf(MessageBodyProviderNotFoundException.class));

    f.get();
  }

  @Test()
  public void testPostJsonPOJOFailure() throws Exception {
    thrown.expect(MessageBodyProviderNotFoundException.class);
    target(PATH).request().post(Entity.json(new SomePOJO()));
  }

  @Test
  public void testGetSlowWithReadTimeout() throws Exception {
    Future<Response> f = target(SLOW_PATH).request().property(ClientProperties.READ_TIMEOUT, 1).async().get();

    thrown.expectCause(instanceOf(ProcessingException.class));
    thrown.expectMessage(containsString("Read timeout to localhost"));
    thrown.expectMessage(endsWith("1 ms"));

    f.get();
  }

  @Test(expected = TimeoutException.class)
  public void testGetSlowSanity() throws Exception {
    target(SLOW_PATH).request().async().get().get(1, TimeUnit.SECONDS);
    fail();
  }

  /**
   * An integration test to stress the async io of the http client. We configure jersey to have a single thread
   * for it's background executors, so the async invoker must return a future that will be completed by client async
   * via netty. This calls an endpoint that takes 1s to return a response.
   *
   * If the http client was blocking, we would exhaust the jersey thread pool immediately and this test would take 10+
   * seconds to complete.
   *
   * Since it is not blocking, it should complete within two seconds.
   */
  @Test(timeout = 2000)
  public void testGetSlowRepeatedly() throws Exception {
    int numRequests = 10;
    List<CompletableFuture<Response>> futures = new ArrayList<>();

    for (int i = 0; i < numRequests; i++) {
      CompletableFuture<Response> f = new CompletableFuture<>();
      target(SLOW_PATH).request().async().get(new InvocationCallback<Response>() {
        @Override
        public void completed(Response response) {
          f.complete(response);
        }

        @Override
        public void failed(Throwable throwable) {
          f.completeExceptionally(throwable);
        }
      });

      futures.add(f);
    }

    // Wait until all requests have resolved
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();

    assertEquals(numRequests, futures.size());
    futures.forEach(new Consumer<CompletableFuture<Response>>() {
      @Override
      public void accept(CompletableFuture<Response> future) {
        assertTrue(future.isDone());
        assertFalse(future.isCompletedExceptionally());

        try {
            Response r = future.getNow(null);
            assertEquals(200, r.getStatus());
          String body = getBody(r);

          assertEquals(MESSAGE, body);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  private static class SomePOJO {
    final String str = "a";
    final Instant a = Instant.now();
  }
}
