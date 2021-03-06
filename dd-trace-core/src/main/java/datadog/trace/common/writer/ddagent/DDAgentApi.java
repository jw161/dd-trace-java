package datadog.trace.common.writer.ddagent;

import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.Moshi;
import com.squareup.moshi.Types;
import datadog.common.exec.CommonTaskExecutor;
import datadog.trace.api.Config;
import datadog.trace.common.writer.unixdomainsockets.UnixDomainSocketFactory;
import datadog.trace.core.ContainerInfo;
import datadog.trace.core.DDTraceCoreInfo;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Dispatcher;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okio.BufferedSink;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.msgpack.core.MessagePack.Code.FIXARRAY_PREFIX;

/** The API pointing to a DD agent */
@Slf4j
public class DDAgentApi {
  private static final String DATADOG_META_LANG = "Datadog-Meta-Lang";
  private static final String DATADOG_META_LANG_VERSION = "Datadog-Meta-Lang-Version";
  private static final String DATADOG_META_LANG_INTERPRETER = "Datadog-Meta-Lang-Interpreter";
  private static final String DATADOG_META_LANG_INTERPRETER_VENDOR =
      "Datadog-Meta-Lang-Interpreter-Vendor";
  private static final String DATADOG_META_TRACER_VERSION = "Datadog-Meta-Tracer-Version";
  private static final String DATADOG_CONTAINER_ID = "Datadog-Container-ID";
  private static final String X_DATADOG_TRACE_COUNT = "X-Datadog-Trace-Count";

  private static final int HTTP_TIMEOUT = 1; // 1 second for conenct/read/write operations
  private static final String TRACES_ENDPOINT_V3 = "v0.3/traces";
  private static final String TRACES_ENDPOINT_V4 = "v0.4/traces";
  private static final long MILLISECONDS_BETWEEN_ERROR_LOG = TimeUnit.MINUTES.toMillis(5);

  private final List<DDAgentResponseListener> responseListeners = new ArrayList<>();

  private volatile long nextAllowedLogTime = 0;

  private static final JsonAdapter<Map<String, Map<String, Number>>> RESPONSE_ADAPTER =
      new Moshi.Builder()
          .build()
          .adapter(
              Types.newParameterizedType(
                  Map.class,
                  String.class,
                  Types.newParameterizedType(Map.class, String.class, Double.class)));
  private static final MediaType MSGPACK = MediaType.get("application/msgpack");

  private final String host;
  private final int port;
  private final String unixDomainSocketPath;
  private OkHttpClient httpClient;
  private HttpUrl tracesUrl;

  private File tracesDir;
  private final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss");

  public DDAgentApi(final String host, final int port, final String unixDomainSocketPath) {
    this.host = host;
    this.port = port;
    this.unixDomainSocketPath = unixDomainSocketPath;
  }

  public void addResponseListener(final DDAgentResponseListener listener) {
    if (!responseListeners.contains(listener)) {
      responseListeners.add(listener);
    }
  }

  Response sendSerializedTraces(final TraceBuffer traces) {

    if (Config.get().isSnapwatchEnabled()) {
      return sendSerializedTracesSW(traces);
    } else {
      return sendSerializedTracesDD(traces);
    }
  }

  private Response sendSerializedTracesSW(final TraceBuffer traces) {
    if (tracesDir == null) {
      tracesDir = new File(System.getenv("HOME") + "/.snapwatch/recordings");
      if (!tracesDir
          .exists()) { // todo: more error checking: what if exists but it's not a directory, etc.
        tracesDir.mkdirs();
      }
    }

    final String fileName =
        DATE_FORMAT.format(new Date()); // todo: it's insufficient to make a unique name
    final File file = new File(tracesDir, fileName);

    // todo: add data that DD adds in HTTP headers

    try (final FileOutputStream outputStream = new FileOutputStream(file)) {
      final FileChannel fileChannel = outputStream.getChannel();

      traces.writeTo(fileChannel);
      fileChannel.force(false); // todo: is it a good idea?

    } catch (final FileNotFoundException e) {
      e.printStackTrace(); // todo: better error handling
    } catch (final IOException e) {
      e.printStackTrace(); // todo: better error handling
    }

    return Response.success(200);
  }

  private Response sendSerializedTracesDD(final TraceBuffer traces) {
    if (httpClient == null) {
      detectEndpointAndBuildClient();
    }

    try {
      final Request request =
          prepareRequest(tracesUrl)
              .addHeader(X_DATADOG_TRACE_COUNT, Integer.toString(traces.representativeCount()))
              .put(new MsgPackRequestBody(traces))
              .build();
      try (final okhttp3.Response response = httpClient.newCall(request).execute()) {
        if (response.code() != 200) {
          if (log.isDebugEnabled()) {
            log.debug(
                "Error while sending {} of {} traces to the DD agent. Status: {}, Response: {}, Body: {}",
                traces.traceCount(),
                traces.representativeCount(),
                response.code(),
                response.message(),
                response.body().string());
          } else if (nextAllowedLogTime < System.currentTimeMillis()) {
            nextAllowedLogTime = System.currentTimeMillis() + MILLISECONDS_BETWEEN_ERROR_LOG;
            log.warn(
                "Error while sending {} of {} traces to the DD agent. Status: {} {} (going silent for {} minutes)",
                traces.traceCount(),
                traces.representativeCount(),
                response.code(),
                response.message(),
                TimeUnit.MILLISECONDS.toMinutes(MILLISECONDS_BETWEEN_ERROR_LOG));
          }
          return Response.failed(response.code());
        }
        if (log.isDebugEnabled()) {
          log.debug(
              "Successfully sent {} of {} traces to the DD agent.",
              traces.traceCount(),
              traces.representativeCount());
        }
        final String responseString = response.body().string().trim();
        try {
          if (!"".equals(responseString) && !"OK".equalsIgnoreCase(responseString)) {
            final Map<String, Map<String, Number>> parsedResponse =
                RESPONSE_ADAPTER.fromJson(responseString);
            final String endpoint = tracesUrl.toString();
            for (final DDAgentResponseListener listener : responseListeners) {
              listener.onResponse(endpoint, parsedResponse);
            }
          }
          return Response.success(response.code());
        } catch (final IOException e) {
          log.debug("Failed to parse DD agent response: " + responseString, e);
          return Response.success(response.code(), e);
        }
      }
    } catch (final IOException e) {
      if (log.isDebugEnabled()) {
        log.debug(
            "Error while sending "
                + traces.traceCount()
                + " of "
                + traces.representativeCount()
                + " (size="
                + (traces.sizeInBytes() / 1024)
                + "KB)"
                + " traces to the DD agent.",
            e);
      } else if (nextAllowedLogTime < System.currentTimeMillis()) {
        nextAllowedLogTime = System.currentTimeMillis() + MILLISECONDS_BETWEEN_ERROR_LOG;
        log.warn(
            "Error while sending {} of {} (size={}KB, bufferId={}) traces to the DD agent. {}: {} (going silent for {} minutes)",
            traces.traceCount(),
            traces.representativeCount(),
            (traces.sizeInBytes() / 1024),
            ((MsgPackStatefulSerializer.MsgPackTraceBuffer) traces).id,
            e.getClass().getName(),
            e.getMessage(),
            TimeUnit.MILLISECONDS.toMinutes(MILLISECONDS_BETWEEN_ERROR_LOG));
      }
      return Response.failed(e);
    }
  }

  private static final byte[] EMPTY_LIST = new byte[] {FIXARRAY_PREFIX};

  private static boolean endpointAvailable(
      final HttpUrl url, final String unixDomainSocketPath, final boolean retry) {
    try {
      final OkHttpClient client = buildHttpClient(unixDomainSocketPath);
      final RequestBody body = RequestBody.create(MSGPACK, EMPTY_LIST);
      final Request request = prepareRequest(url).put(body).build();

      try (final okhttp3.Response response = client.newCall(request).execute()) {
        return response.code() == 200;
      }
    } catch (final IOException e) {
      if (retry) {
        return endpointAvailable(url, unixDomainSocketPath, false);
      }
    }
    return false;
  }

  private static OkHttpClient buildHttpClient(final String unixDomainSocketPath) {
    OkHttpClient.Builder builder = new OkHttpClient.Builder();
    if (unixDomainSocketPath != null) {
      builder = builder.socketFactory(new UnixDomainSocketFactory(new File(unixDomainSocketPath)));
    }
    return builder
        .connectTimeout(HTTP_TIMEOUT, TimeUnit.SECONDS)
        .writeTimeout(HTTP_TIMEOUT, TimeUnit.SECONDS)
        .readTimeout(HTTP_TIMEOUT, TimeUnit.SECONDS)

        // We don't do async so this shouldn't matter, but just to be safe...
        .dispatcher(new Dispatcher(CommonTaskExecutor.INSTANCE))
        .build();
  }

  private static HttpUrl getUrl(final String host, final int port, final String endPoint) {
    return new HttpUrl.Builder()
        .scheme("http")
        .host(host)
        .port(port)
        .addEncodedPathSegments(endPoint)
        .build();
  }

  private static Request.Builder prepareRequest(final HttpUrl url) {
    final Request.Builder builder =
        new Request.Builder()
            .url(url)
            .addHeader(DATADOG_META_LANG, "java")
            .addHeader(DATADOG_META_LANG_VERSION, DDTraceCoreInfo.JAVA_VERSION)
            .addHeader(DATADOG_META_LANG_INTERPRETER, DDTraceCoreInfo.JAVA_VM_NAME)
            .addHeader(DATADOG_META_LANG_INTERPRETER_VENDOR, DDTraceCoreInfo.JAVA_VM_VENDOR)
            .addHeader(DATADOG_META_TRACER_VERSION, DDTraceCoreInfo.VERSION);

    final String containerId = ContainerInfo.get().getContainerId();
    if (containerId == null) {
      return builder;
    } else {
      return builder.addHeader(DATADOG_CONTAINER_ID, containerId);
    }
  }

  private synchronized void detectEndpointAndBuildClient() {
    if (httpClient == null) {
      final HttpUrl v4Url = getUrl(host, port, TRACES_ENDPOINT_V4);
      if (endpointAvailable(v4Url, unixDomainSocketPath, true)) {
        tracesUrl = v4Url;
      } else {
        log.debug("API v0.4 endpoints not available. Downgrading to v0.3");
        tracesUrl = getUrl(host, port, TRACES_ENDPOINT_V3);
      }
      httpClient = buildHttpClient(unixDomainSocketPath);
    }
  }

  @Override
  public String toString() {
    return "DDApi { tracesUrl=" + tracesUrl + " }";
  }

  /**
   * Encapsulates an attempted response from the Datadog agent.
   *
   * <p>If communication fails or times out, the Response will NOT be successful and will lack
   * status code, but will have an exception.
   *
   * <p>If an communication occurs, the Response will have a status code and will be marked as
   * success or fail in accordance with the code.
   *
   * <p>NOTE: A successful communication may still contain an exception if there was a problem
   * parsing the response from the Datadog agent.
   */
  public static final class Response {
    /** Factory method for a successful request with a trivial response body */
    public static Response success(final int status) {
      return new Response(true, status, null);
    }

    /** Factory method for a successful request will a malformed response body */
    public static Response success(final int status, final Throwable exception) {
      return new Response(true, status, exception);
    }

    /** Factory method for a request that receive an error status in response */
    public static Response failed(final int status) {
      return new Response(false, status, null);
    }

    /** Factory method for a failed communication attempt */
    public static Response failed(final Throwable exception) {
      return new Response(false, null, exception);
    }

    private final boolean success;
    private final Integer status;
    private final Throwable exception;

    private Response(final boolean success, final Integer status, final Throwable exception) {
      this.success = success;
      this.status = status;
      this.exception = exception;
    }

    public final boolean success() {
      return success;
    }

    // TODO: DQH - In Java 8, switch to OptionalInteger
    public final Integer status() {
      return status;
    }

    // TODO: DQH - In Java 8, switch to Optional<Throwable>?
    public final Throwable exception() {
      return exception;
    }
  }

  private static class MsgPackRequestBody extends RequestBody {
    private final TraceBuffer traces;

    private MsgPackRequestBody(final TraceBuffer traces) {
      this.traces = traces;
    }

    @Override
    public MediaType contentType() {
      return MSGPACK;
    }

    @Override
    public long contentLength() {
      return traces.headerSize() + traces.sizeInBytes();
    }

    @Override
    public void writeTo(final BufferedSink sink) throws IOException {
      traces.writeTo(sink);
      sink.flush();
    }
  }
}
