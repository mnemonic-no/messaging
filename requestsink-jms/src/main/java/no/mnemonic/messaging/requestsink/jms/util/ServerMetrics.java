package no.mnemonic.messaging.requestsink.jms.util;

import no.mnemonic.commons.metrics.MetricException;
import no.mnemonic.commons.metrics.Metrics;
import no.mnemonic.commons.metrics.MetricsData;

import java.util.concurrent.atomic.LongAdder;

public class ServerMetrics {
  private final LongAdder errorCount = new LongAdder();
  private final LongAdder reconnectCount = new LongAdder();
  private final LongAdder exceptionSignalCount = new LongAdder();
  private final LongAdder requestCount = new LongAdder();
  private final LongAdder requestTimeoutCount = new LongAdder();
  private final LongAdder replyCount = new LongAdder();
  private final LongAdder endOfStreamCounter = new LongAdder();
  private final LongAdder extendWaitCounter = new LongAdder();
  private final LongAdder fragmentedUploadRequestCounter = new LongAdder();
  private final LongAdder fragmentedUploadFragmentsCounter = new LongAdder();
  private final LongAdder fragmentedUploadCompletedCounter = new LongAdder();
  private final LongAdder fragmentedReplyCompletedCounter = new LongAdder();
  private final LongAdder fragmentedReplyFragmentCounter = new LongAdder();
  private final LongAdder incompatibleMessageCounter = new LongAdder();

  public Metrics metrics() throws MetricException {
    return new MetricsData()
            .addData("errors", errorCount)
            .addData("reconnections", reconnectCount)
            .addData("exceptionSignal", exceptionSignalCount)
            .addData("requests", requestCount)
            .addData("requestTimeoutCount", requestTimeoutCount)
            .addData("replies", replyCount)
            .addData("endOfStreams", endOfStreamCounter)
            .addData("extendWaits", extendWaitCounter)
            .addData("fragmentedUploadRequests", fragmentedUploadRequestCounter)
            .addData("fragmentedUploadFragments", fragmentedUploadFragmentsCounter)
            .addData("fragmentedUploadCompleted", fragmentedUploadCompletedCounter)
            .addData("fragmentedReplyCompleted", fragmentedReplyCompletedCounter)
            .addData("fragmentedReplyFragments", fragmentedReplyFragmentCounter)
            .addData("incompatibleMessages", incompatibleMessageCounter);
  }

  public void requestTimeout() {
    requestTimeoutCount.increment();
  }

  public void reconnected() {
    reconnectCount.increment();
  }

  public void request() {
    requestCount.increment();
  }

  public void error() {
    errorCount.increment();
  }

  public void exceptionSignal() {
    exceptionSignalCount.increment();
  }

  public void fragmentedUploadRequested() {
    fragmentedUploadRequestCounter.increment();
  }

  public void fragmentedUploadFragment() {
    fragmentedUploadFragmentsCounter.increment();
  }

  public void fragmentedUploadCompleted() {
    fragmentedUploadCompletedCounter.increment();
  }

  public void fragmentedReplyCompleted() {
    fragmentedReplyCompletedCounter.increment();
  }

  public void fragmentReplyFragment() {
    fragmentedReplyFragmentCounter.increment();
  }

  public void incompatibleMessage() {
    incompatibleMessageCounter.increment();
  }

  public void reply() {
    replyCount.increment();
  }

  public void endOfStream() {
    endOfStreamCounter.increment();
  }

  public void extendWait() {
    extendWaitCounter.increment();
  }
}
