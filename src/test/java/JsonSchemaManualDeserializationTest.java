import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;


@Testcontainers
public class JsonSchemaManualDeserializationTest {

    private static final Logger logger = LoggerFactory.getLogger(JsonSchemaManualDeserializationTest.class);

    private static final String TOPIC_NAME = "customers";
    private static final int NB_RECORDS = 10;

    private static String schemaRegistryUrl;

    private static final Network NETWORK = Network.newNetwork();

    @Container
    public static KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.8.0"))
            .withNetwork(NETWORK);

    @Container
    public static final GenericContainer<?> SCHEMA_REGISTRY =
            new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.8.0"))
                    .withNetwork(NETWORK)
                    .withExposedPorts(8081)
                    .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                    .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                            "PLAINTEXT://" + KAFKA.getNetworkAliases().get(0) + ":9092")
                    .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));

    @BeforeAll
    public static void setup() throws ExecutionException, InterruptedException, IOException, RestClientException {
        schemaRegistryUrl = String.format("http://%s:%d", SCHEMA_REGISTRY.getHost(), SCHEMA_REGISTRY.getFirstMappedPort());

        registerSchema();

        Map<String, Object> properties = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class,
                "schema.registry.url", schemaRegistryUrl
        );

        createTopic(properties);

        try (Producer<Integer, Customer> producer = new KafkaProducer<>(properties)) {
            for (int i = 0; i < NB_RECORDS; i++) {
                var customer = new Customer("Firstname" + i, "Lastname" + i);
                ProducerRecord<Integer, Customer> record = new ProducerRecord<>(TOPIC_NAME, i, customer);
                logger.info("Sending Key = {}, Value = {}", record.key(), record.value());
                producer.send(record, (recordMetadata, e) -> {
                    if (e != null) {
                        logger.error("Could not set the message {}", record, e);
                    } else {
                        logger.info("Successfully sent Key = {}, Value = {}", record.key(), record.value());
                    }
                }).get();
            }
        }
    }

    @Test
    public void testAutomaticallyDeserializeToJavaMap() {

        Map<String, Object> properties = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "testAutomaticallyDeserializeToJavaMap",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class,
                "schema.registry.url", schemaRegistryUrl
        );

        try (KafkaConsumer<Integer, Object> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            List<ConsumerRecord<Integer, Object>> loaded = new ArrayList<>();

            await().atMost(20, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        ConsumerRecords<Integer, Object> records = consumer.poll(Duration.of(1, ChronoUnit.SECONDS));
                        records.forEach((record) -> {
                            loaded.add(record);
                        });
                        assertThat(loaded).isNotEmpty();
                        assertThat(loaded.size()).isEqualTo(NB_RECORDS);
                    });

            ConsumerRecord<Integer, Object> record = loaded.get(0);

            Map<String, Object> map = (Map<String, Object>) record.value();
            assertThat(map.get("firstname")).isInstanceOf(String.class);
        }
    }

    @Test
    public void testAutomaticallyDeserializeToCustomer() {

        Map<String, Object> properties = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "testAutomaticallyDeserializeToCustomer",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class,
                "schema.registry.url", schemaRegistryUrl,

                KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, Customer.class.getName()
        );

        try (KafkaConsumer<Integer, Customer> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            List<ConsumerRecord<Integer, Customer>> loaded = new ArrayList<>();

            await().atMost(20, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        ConsumerRecords<Integer, Customer> records = consumer.poll(Duration.of(1, ChronoUnit.SECONDS));
                        records.forEach((record) -> {
                            loaded.add(record);
                        });
                        assertThat(loaded).isNotEmpty();
                        assertThat(loaded.size()).isEqualTo(NB_RECORDS);
                    });

            ConsumerRecord<Integer, Customer> record = loaded.get(0);

            Customer customer = record.value();
            assertThat(customer.getFirstname()).isInstanceOf(String.class);

        }
    }

    @Test
    public void testManuallyDeserializeToJavaMap() {

        Map<String, Object> properties = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "testManuallyDeserializeToJavaMap",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class
        );

        try (KafkaConsumer<Integer, byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            List<ConsumerRecord<Integer, byte[]>> loaded = new ArrayList<>();

            await().atMost(20, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        ConsumerRecords<Integer, byte[]> records = consumer.poll(Duration.of(1, ChronoUnit.SECONDS));
                        records.forEach((record) -> {
                            loaded.add(record);
                        });
                        assertThat(loaded).isNotEmpty();
                        assertThat(loaded.size()).isEqualTo(NB_RECORDS);
                    });

            ConsumerRecord<Integer, byte[]> record = loaded.get(0);

            KafkaJsonSchemaDeserializer deserializer = new KafkaJsonSchemaDeserializer();
            deserializer.configure(Map.of(
                    "schema.registry.url", schemaRegistryUrl
            ), false);

            Map<String, Object> map = (Map<String, Object>) deserializer.deserialize(TOPIC_NAME, record.value());
            assertThat(map.get("firstname")).isInstanceOf(String.class);
        }
    }

    @Test
    public void testManuallyDeserializeToCustomer() {

        Map<String, Object> properties = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "testManuallyDeserializeToCustomer",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class
        );

        try (KafkaConsumer<Integer, byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            List<ConsumerRecord<Integer, byte[]>> loaded = new ArrayList<>();

            await().atMost(20, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        ConsumerRecords<Integer, byte[]> records = consumer.poll(Duration.of(1, ChronoUnit.SECONDS));
                        records.forEach((record) -> {
                            loaded.add(record);
                        });
                        assertThat(loaded).isNotEmpty();
                        assertThat(loaded.size()).isEqualTo(NB_RECORDS);
                    });

            ConsumerRecord<Integer, byte[]> record = loaded.get(0);

            KafkaJsonSchemaDeserializer deserializer = new KafkaJsonSchemaDeserializer();
            deserializer.configure(Map.of(
                    "schema.registry.url", schemaRegistryUrl,
                    KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, Customer.class.getName()

            ), false);

            Customer customer = (Customer) deserializer.deserialize(TOPIC_NAME, record.value());
            assertThat(customer.getFirstname()).isInstanceOf(String.class);
        }
    }


    private static void registerSchema() throws IOException, RestClientException {
        InputStream is = JsonSchemaManualDeserializationTest.class.getResourceAsStream("/customer.json");
        String rawSchema = new String(is.readAllBytes());
        ParsedSchema customerJsonSchema = new JsonSchema(rawSchema);

        var schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
        schemaRegistryClient.register(TOPIC_NAME + "-value", customerJsonSchema);
    }

    private static void createTopic(Map<String, Object> properties) throws InterruptedException, ExecutionException {
        AdminClient adminClient = KafkaAdminClient.create(properties);
        createTopic(adminClient, TOPIC_NAME);
    }

    private static void createTopic(@NotNull AdminClient adminClient, String topicName) throws InterruptedException, ExecutionException {
        if (!adminClient.listTopics().names().get().contains(topicName)) {
            logger.info("Creating topic {}", topicName);
            final NewTopic newTopic = new NewTopic(topicName, 2, (short) 1);
            try {
                CreateTopicsResult topicsCreationResult = adminClient.createTopics(Collections.singleton(newTopic));
                topicsCreationResult.all().get();
            } catch (ExecutionException e) {
                //silent ignore if topic already exists
            }
        }
    }

    public static class Customer {
        private String firstname;
        private String lastname;

        public Customer() {
        }

        public Customer(String firstname, String lastname) {
            this.firstname = firstname;
            this.lastname = lastname;
        }

        public String getFirstname() {
            return firstname;
        }

        public void setFirstname(String firstname) {
            this.firstname = firstname;
        }

        public String getLastname() {
            return lastname;
        }

        public void setLastname(String lastname) {
            this.lastname = lastname;
        }

        @Override
        public String toString() {
            return "Customer{" +
                    "firstname='" + firstname + '\'' +
                    ", lastname='" + lastname + '\'' +
                    '}';
        }
    }
}
