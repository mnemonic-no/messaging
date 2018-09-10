package no.mnemonic.messaging.requestsink.jms.util;

import no.mnemonic.commons.metrics.MetricException;
import no.mnemonic.commons.metrics.Metrics;
import no.mnemonic.commons.metrics.MetricsData;

import java.util.concurrent.atomic.LongAdder;

public class ClientMetrics {
  private final LongAdder errorCount = new LongAdder();
  private final LongAdder disconnectedCount = new LongAdder();
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
  private final LongAdder invalidatedResponseQueues = new LongAdder();

  public Metrics metrics() throws MetricException {
    return new MetricsData()
            .addData("invalidatedResponseQueues", invalidatedResponseQueues)
            .addData("errors", errorCount)
            .addData("disconnected", disconnectedCount)
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

  public void invalidatedResponseQueue() {
    invalidatedResponseQueues.increment();
  }

  public void request() {
    requestCount.increment();
  }

  public void disconnected() {
    disconnectedCount.increment();
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

  public void fragmentedReplyFragment() {
    fragmentedReplyFragmentCounter.increment();
  }

  public void incompatibleMessage() {
    incompatibleMessageCounter.increment();
  }

  public void unknownCallIDMessage() {
    unknownCallIDMessageCounter.increment();
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
