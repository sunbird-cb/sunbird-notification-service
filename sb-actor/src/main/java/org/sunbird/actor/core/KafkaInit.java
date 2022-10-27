package org.sunbird.actor.core;

import com.google.gson.Gson;
import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.sunbird.notification.beans.EmailConfig;
import org.sunbird.notification.email.service.IEmailService;
import org.sunbird.notification.email.service.impl.IEmailProviderFactory;
import org.sunbird.notification.sms.provider.ISmsProvider;
import org.sunbird.request.LoggerUtil;
import org.sunbird.util.ConfigUtil;
import org.sunbird.util.KafkaClient;

import java.util.Collections;

/**
 * This class contains method to instantiate actor system and actors.
 * @author Amit Kumar
 */
public class KafkaInit {

    private static LoggerUtil logger = new LoggerUtil(KafkaInit.class);
    private Producer<Long, String> producer = null;
    private Consumer<Long, String> consumer = null;

    public static final String SUNBIRD_NOTIFICATION_KAFKA_SERVICE_CONFIG =
            "sunbird_notification_kafka_servers_config";
    public static final String SUNBIRD_NOTIFICATION_KAFKA_TOPIC = "sunbird_notification_kafka_topic";
    public static final String KAFKA_CLIENT_NOTIFICATION_PRODUCER = "KafkaClientNotificationProducer";
    public static final String KAFKA_CLIENT_NOTIFICATION_CONSUMER = "KafkaClientNotificationConsumer";
    private String topic = null;
    Gson gson = new Gson();

    private IEmailService emailservice;
    private ISmsProvider smsProvider;

    // static variable instance of type KafkaInit
    private static KafkaInit instance = null;

    // private constructor restricted to this class itself
    private KafkaInit() { }

    // static method to create instance of KafkaInit class
    public static KafkaInit getInstance()
    {
        if (instance == null)
            instance = new KafkaInit();

        return instance;
    }

    // instantiate actor system and actors
    public void init() {
        //initKafkaClientProducer();;
        initKafkaClientConsumer();

    }

    public void processMessage() {
        int noRecordsCount = 0;
        while (true) {
            try {
                final ConsumerRecords<Long, String> consumerRecords =
                        consumer.poll(1000);

                noRecordsCount++;
                consumerRecords.forEach(record -> {
                    System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                            record.key(), record.value(),
                            record.partition(), record.offset());
                    // UserRegistration userRegistration = gson.fromJson(data.value(), UserRegistration.class);
                });
                consumer.commitAsync();
            } catch (Exception e) {
                logger.error("KafkaIntialization:processMessage: An exception occurred.", e);
            }
        }
    }
    private void initKafkaClientConsumer() {
        if (consumer == null) {
            String BOOTSTRAP_SERVERS = System.getenv(SUNBIRD_NOTIFICATION_KAFKA_SERVICE_CONFIG);
            topic = System.getenv(SUNBIRD_NOTIFICATION_KAFKA_TOPIC);
            logger.info(
                    "KafkaIntialization:initKafkaClient: Bootstrap servers = "
                            + BOOTSTRAP_SERVERS);
            logger.info("KafkaIntialization:initKafkaClient: topic = " + topic);
            try {
                consumer = KafkaClient.createConsumer(
                        BOOTSTRAP_SERVERS, KAFKA_CLIENT_NOTIFICATION_CONSUMER);
                consumer.subscribe(Collections.singletonList(topic));
                processMessage();
            } catch (Exception e) {
                logger.error("KafkaIntialization:initKafkaClient: An exception occurred.", e);
            }
            consumer.close();
            System.out.println("DONE");
        }
    }

    /** Initialises Kafka producer required for dispatching messages on Kafka. */
    private void initKafkaClientProducer() {
        if (producer == null) {
            Config config = ConfigUtil.getConfig();
            String BOOTSTRAP_SERVERS = config.getString(SUNBIRD_NOTIFICATION_KAFKA_SERVICE_CONFIG);
            topic = config.getString(SUNBIRD_NOTIFICATION_KAFKA_TOPIC);

            logger.info(
                    "KafkaIntialization:initKafkaClient: Bootstrap servers = "
                            + BOOTSTRAP_SERVERS);
            logger.info("KafkaIntialization:initKafkaClient: topic = " + topic);
            try {
                producer =
                        KafkaClient.createProducer(
                                BOOTSTRAP_SERVERS, KAFKA_CLIENT_NOTIFICATION_PRODUCER);
            } catch (Exception e) {
                logger.error("KafkaIntialization:initKafkaClient: An exception occurred.", e);
            }
        }
    }

    private IEmailService getEmailInstance() {
        if (emailservice == null) {
            IEmailProviderFactory factory = new IEmailProviderFactory();
            EmailConfig config = new EmailConfig();
            emailservice = factory.create(config);
        }
        return emailservice;
    }

}
