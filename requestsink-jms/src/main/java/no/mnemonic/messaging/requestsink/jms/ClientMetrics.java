package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.metrics.MetricException;
import no.mnemonic.commons.metrics.Metrics;
import no.mnemonic.commons.metrics.MetricsData;

import java.util.concurrent.atomic.LongAdder;

class ClientMetrics {
  private final LongAdder errorCount = new LongAdder();
  private final LongAdder exceptionSignalCount = new LongAdder();
  private final LongAdder requestCount = new LongAdder();
  private final LongAdder replyCount = new LongAdder();
  private final LongAdder endOfStreamCounter = new LongAdder();
  private final LongAdder extendWaitCounter = new LongAdder();
  private final LongAdder fragmentedUploadRequestCounter = new LongAdder();
  private final LongAdder fragmentedUploadFragmentsCounter = new LongAdder();
  private final LongAdder fragmentedUploadCompletedCounter = new LongAdder();
  private final LongAdder fragmentedReplyCompletedCounter = new LongAdder();
  private final LongAdder fragmentedReplyFragmentCounter = new LongAdder();
  private final LongAdder incompatibleMessageCounter = new LongAdder();
  private final LongAdder unknownCallIDMessageCounter = new LongAdder();

  Metrics metrics() throws MetricException {
    return new MetricsData()
            .addData("errors", errorCount)
            .addData("exceptionSignal", exceptionSignalCount)
            .addData("requests", requestCount)
            .addData("replies", replyCount)
            .addData("endOfStreams", endOfStreamCounter)
            .addData("extendWaits", extendWaitCounter)
            .addData("fragmentedUploadRequests", fragmentedUploadRequestCounter)
            .addData("fragmentedUploadFragments", fragmentedUploadFragmentsCounter)
            .addData("fragmentedUploadCompleted", fragmentedUploadCompletedCounter)
            .addData("fragmentedReplyCompleted", fragmentedReplyCompletedCounter)
            .addData("fragmentedReplyFragments", fragmentedReplyFragmentCounter)
            .addData("incompatibleMessages", incompatibleMessageCounter)
            .addData("unknownCallIDMessages", unknownCallIDMessageCounter);
  }

  void request() {
    requestCount.increment();
  }

  void error() {
    errorCount.increment();
  }

  void exceptionSignal() { exceptionSignalCount.increment(); }

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

  void fragmentedReplyFragment() {
    fragmentedReplyFragmentCounter.increment();
  }

  void incompatibleMessage() {
    incompatibleMessageCounter.increment();
  }

  void unknownCallIDMessage() {
    unknownCallIDMessageCounter.increment();
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
