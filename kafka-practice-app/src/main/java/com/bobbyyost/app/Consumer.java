package com.bobbyyost.app;

import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

/**
 * Created by robertyost on 6/26/16.
 */
public class Consumer {
    public static void main(String[] args) throws IOException
    {
        KafkaConsumer<String, String> consumer;
        try (InputStream props = Resources.getResource("consumer.props").openStream())
        {
            Properties properties = new Properties();
            properties.load(props);
            if (properties.getProperty("group.id") == null)
            {
                properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
            }
            consumer =  new KafkaConsumer<>(properties);
        }

        consumer.subscribe(Arrays.asList("kafka-with-java-practice"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(200);
            for (ConsumerRecord<String, String> record : records)
            {
                System.out.println(record.value());
            }
        }

    }
}
