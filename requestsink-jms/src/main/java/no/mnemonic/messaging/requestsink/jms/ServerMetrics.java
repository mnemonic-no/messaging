package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.metrics.MetricException;
import no.mnemonic.commons.metrics.Metrics;
import no.mnemonic.commons.metrics.MetricsData;

import java.util.concurrent.atomic.LongAdder;

class ServerMetrics {
  private final LongAdder errorCount = new LongAdder();
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

  Metrics metrics() throws MetricException {
    return new MetricsData()
            .addData("errors", errorCount)
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

  void requestTimeout() {
    requestTimeoutCount.increment();
  }

  void request() {
    requestCount.increment();
  }

  void error() {
    errorCount.increment();
  }

  void exceptionSignal() {
    exceptionSignalCount.increment();
  }

  void fragmentedUploadRequested() {
    fragmentedUploadRequestCounter.increment();
  }

  void fragmentedUploadFragment() {
    fragmentedUploadFragmentsCounter.increment();
  }

  void fragmentedUploadCompleted() {
    fragmentedUploadCompletedCounter.increment();
  }

  void fragmentedReplyCompleted() {
    fragmentedReplyCompletedCounter.increment();
  }

  void fragmentReplyFragment() {
    fragmentedReplyFragmentCounter.increment();
  }

  void incompatibleMessage() {
    incompatibleMessageCounter.increment();
  }

  void reply() {
    replyCount.increment();
  }

  void endOfStream() {
    endOfStreamCounter.increment();
  }

  void extendWait() {
    extendWaitCounter.increment();
  }
}
