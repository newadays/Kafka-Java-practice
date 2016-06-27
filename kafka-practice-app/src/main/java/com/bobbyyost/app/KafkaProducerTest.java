package com.bobbyyost.app;

/**
 * Created by robertyost on 6/25/16.
 */

import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

public class KafkaProducerTest {
    public static void main(String[] args) throws IOException
    {
        KafkaProducer<String, String> producer;
        try (InputStream props = Resources.getResource("producer.props").openStream())
        {
            Properties properties = new Properties();
            properties.load(props);
            producer = new KafkaProducer<>(properties);
        }

        File file = new File("/Users/robertyost/Documents/Maven Projects/kafka-sample-programs/src/main/java/com/mapr/examples/kafka-practice-data-file.txt");
        String line = null;
        try
        {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            ArrayList<String> data = new ArrayList<>();
            while ((line = reader.readLine()) != null)
            {
                data.add(line);
            }
            for (int i = 0; i < data.size(); i++)
            {
                producer.send(new ProducerRecord<>("kafka-with-java-practice",
                        data.get(i)));
            }
            System.out.println("Message sent successfully.");
        }

        catch (Throwable throwable)
        {
            System.out.println(throwable.getStackTrace());
        }

        finally
        {
            producer.close();
        }
    }
}
