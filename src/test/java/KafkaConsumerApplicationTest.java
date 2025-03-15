import com.apache.kafka.ApacheKafkaConsumerApplication;
import com.apache.kafka.pojo.Customer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = ApacheKafkaConsumerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
public class KafkaConsumerApplicationTest {

    Logger Log = LoggerFactory.getLogger(KafkaConsumerApplicationTest.class);
    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

    @DynamicPropertySource
    public static void initKafkaProperties(DynamicPropertyRegistry registry)
    {
        registry.add("spring.kafka.bootstrap-servers",kafka::getBootstrapServers);
    }
    @Autowired
    private KafkaTemplate<String,Object> kafkaTemplate;

    @Test
     void testConsumerEvent(){
        Log.info("testConsumerEvents Execution Started...");
        Customer customer = new Customer(1,"prema","prema@gmail.com","9876576878");
        kafkaTemplate.send("beHappyPremaLatha-1",customer);
        Log.info("testConsumerEvents Execution Ended...");
        await().pollInterval(Duration.ofSeconds(3))
                .atMost(10, TimeUnit.SECONDS).untilAsserted(()->{

                });
    }
}
