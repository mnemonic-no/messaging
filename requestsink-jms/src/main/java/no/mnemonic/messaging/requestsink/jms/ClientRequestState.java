package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.messaging.requestsink.RequestContext;

import javax.jms.JMSException;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import static no.mnemonic.commons.utilities.collections.CollectionUtils.isEmpty;

class ClientRequestState {

  private static final Logger LOGGER = Logging.getLogger(ClientRequestState.class);

  private final ClassLoader classLoader;
  private final Map<String, Collection<MessageFragment>> fragments = new ConcurrentHashMap<>();
  private final RequestContext requestContext;

  ClientRequestState(RequestContext requestContext, ClassLoader classLoader) {
    if (requestContext == null) throw new IllegalArgumentException("requestContext not set");
    if (classLoader == null) throw new IllegalArgumentException("classLoader not set");
    this.requestContext = requestContext;
    this.classLoader = classLoader;
  }

  RequestContext getRequestContext() {
    return requestContext;
  }

  ClassLoader getClassLoader() {
    return classLoader;
  }

  boolean addFragment(MessageFragment messageFragment) {
    if (messageFragment == null) {
      LOGGER.warning("Fragment was null");
      return false;
    }
    fragments
            .computeIfAbsent(messageFragment.getResponseID(), id -> new LinkedBlockingDeque<>())
            .add(messageFragment);
    return true;
  }

  boolean reassembleFragments(String responseID, int totalFragments, String checksum) {
    try {
      Collection<MessageFragment> responseFragments = this.fragments.remove(responseID);
      if (isEmpty(responseFragments)) {
        LOGGER.warning("Received fragment end-message without preceding fragments");
        return false;
      }
      byte[] reassembledData = JMSUtils.reassembleFragments(responseFragments, totalFragments, checksum);
      if (LOGGER.isDebug()) {
        LOGGER.debug("# addReassembledResponse [responseID=%s]", responseID);
      }
      return requestContext.addResponse(JMSUtils.unserialize(reassembledData, classLoader));
    } catch (JMSException | IOException | ClassNotFoundException e) {
      LOGGER.warning(e, "Error unpacking fragments");
      return false;
    }
  }
}
