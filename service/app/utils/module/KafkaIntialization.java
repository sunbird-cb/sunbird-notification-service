package utils.module;

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
import org.sunbird.util.Constant;
import org.sunbird.util.kafka.KafkaClient;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;

@Singleton
public class KafkaIntialization {
    private static LoggerUtil logger = new LoggerUtil(KafkaIntialization.class);
    private Producer<Long, String> producer = null;
    private Consumer<Long, String> consumer = null;
    private String topic = null;
    Gson gson = new Gson();


    private IEmailService emailservice;
    private ISmsProvider smsProvider;


    @Inject
    public KafkaIntialization() {
        logger.info("Starting Kafka Initialiser");
        initKafkaClientProducer();;
        initKafkaClientConsumer();
//        if (consumer == null) {
//            String BOOTSTRAP_SERVERS = System.getenv(Constant.SUNBIRD_NOTIFICATION_KAFKA_SERVICE_CONFIG);
//            topic = System.getenv(Constant.SUNBIRD_NOTIFICATION_KAFKA_TOPIC);
//            logger.info(
//                    "FCMNotificationDispatcher:initKafkaClient: Bootstrap servers = "
//                            + BOOTSTRAP_SERVERS);
//            logger.info("FCMNotificationDispatcher:initKafkaClient: topic = " + topic);
//            try {
//                consumer = KafkaClient.createConsumer(
//                        BOOTSTRAP_SERVERS, Constant.KAFKA_CLIENT_NOTIFICATION_CONSUMER);
//                consumer.subscribe(Collections.singletonList(topic));
//                int noRecordsCount = 0;
//
//                while (true) {
//                    final ConsumerRecords<Long, String> consumerRecords =
//                            consumer.poll(1000);
//
//                    noRecordsCount++;
//                    consumerRecords.forEach(record -> {
//                        System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
//                                record.key(), record.value(),
//                                record.partition(), record.offset());
//                    });
//
//                    consumer.commitAsync();
//                    getEmailInstance().sendEmail();
//
//                }
//            } catch (Exception e) {
//                logger.error("FCMNotificationDispatcher:initKafkaClient: An exception occurred.", e);
//            }
//            consumer.close();
//            System.out.println("DONE");
//        }
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
            String BOOTSTRAP_SERVERS = System.getenv(Constant.SUNBIRD_NOTIFICATION_KAFKA_SERVICE_CONFIG);
            topic = System.getenv(Constant.SUNBIRD_NOTIFICATION_KAFKA_TOPIC);
            logger.info(
                    "KafkaIntialization:initKafkaClient: Bootstrap servers = "
                            + BOOTSTRAP_SERVERS);
            logger.info("KafkaIntialization:initKafkaClient: topic = " + topic);
            try {
                consumer = KafkaClient.createConsumer(
                        BOOTSTRAP_SERVERS, Constant.KAFKA_CLIENT_NOTIFICATION_CONSUMER);
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
            String BOOTSTRAP_SERVERS = config.getString(Constant.SUNBIRD_NOTIFICATION_KAFKA_SERVICE_CONFIG);
            topic = config.getString(Constant.SUNBIRD_NOTIFICATION_KAFKA_TOPIC);

            logger.info(
                    "KafkaIntialization:initKafkaClient: Bootstrap servers = "
                            + BOOTSTRAP_SERVERS);
            logger.info("KafkaIntialization:initKafkaClient: topic = " + topic);
            try {
                producer =
                        KafkaClient.createProducer(
                                BOOTSTRAP_SERVERS, Constant.KAFKA_CLIENT_NOTIFICATION_PRODUCER);
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
