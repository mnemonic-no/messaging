package no.mnemonic.messaging.documentchannel.kafka;

import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.configuration.ProjectName;
import com.palantir.docker.compose.connection.waiting.HealthChecks;
import no.mnemonic.commons.junit.docker.DockerTestUtils;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class KafkaTopicHelperTest {

  @ClassRule
  public static DockerComposeRule docker = DockerComposeRule.builder()
          .file("src/test/resources/docker-compose-kafka.yml")
          .projectName(ProjectName.fromString(UUID.randomUUID().toString().replace("-", "")))
          .waitingForService("kafka", HealthChecks.toHaveAllPortsOpen())
          .build();

  @Test
  public void createTopic() throws InterruptedException, ExecutionException {
    String topicName = UUID.randomUUID().toString();

    try (KafkaTopicHelper kafkaTopicHelper = new KafkaTopicHelper(String.format("%s:%d", kafkaHost(), kafkaPort()))) {
      kafkaTopicHelper.createMissingTopic(topicName);
      assertTrue(kafkaTopicHelper.listTopics().contains(topicName));
    }
  }

  @Test
  public void reCreateTopicIsIgnored() throws InterruptedException, ExecutionException {
    String topicName = UUID.randomUUID().toString();

    try (KafkaTopicHelper kafkaTopicHelper = new KafkaTopicHelper(String.format("%s:%d", kafkaHost(), kafkaPort()))) {
      kafkaTopicHelper.createMissingTopic(topicName);
      kafkaTopicHelper.createMissingTopic(topicName);
      assertTrue(kafkaTopicHelper.listTopics().contains(topicName));
    }
  }

  private String kafkaHost() {
    return DockerTestUtils.getDockerHost();
  }

  private int kafkaPort() {
    return docker.containers()
            .container("kafka")
            .port(9094)
            .getExternalPort();
  }

}