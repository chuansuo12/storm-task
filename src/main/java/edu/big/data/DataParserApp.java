package edu.big.data;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @author tengyj
 * @since 2020/6/6
 */
public class DataParserApp {
    private static final String KAFKA_BOOTSTRAP_SERVER =
            "172.17.0.1:9092,172.17.0.1:9093,172.17.0.1:9094";
    private static final String TOPIC = "user_behavior";
    private static final String KAFKA_GROUP_ID = "storm";

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();
        KafkaSpout<String, String> kafkaSpout = getKafkaSpout();
        builder.setSpout("user-behavior-spout", kafkaSpout);
        builder.setBolt("save-data-bolt", new SaveDataBolt());
        Config conf = new Config();
        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }

    private static KafkaSpout<String, String> getKafkaSpout() {
        KafkaSpoutConfig<String, String> kafkaSpoutConfig = KafkaSpoutConfig
                .builder(KAFKA_BOOTSTRAP_SERVER, TOPIC)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_GROUP_ID)
                .build();
        return new KafkaSpout<>(kafkaSpoutConfig);
    }
}
