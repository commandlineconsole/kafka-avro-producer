package com.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.codec.DecoderException;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class AvroProducer {

    void producer(Schema schema) throws IOException {

    	// define producer properties
        Properties props = new Properties();
        props.put("metadata.broker.list", "0:9092");
        props.put("serializer.class", "kafka.serializer.DefaultEncoder");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, byte[]> producer = new Producer<String, byte[]>(config);
        
        // construct GenericRecord object and put data 
        GenericRecord payload = new GenericData.Record(schema);
        payload.put("name", "Java Programming");
        payload.put("id", 1234);
        payload.put("category","Programming ");
        System.out.println("Original Message : "+ payload);
        
        // serialize the object to a bytearray
        DatumWriter<GenericRecord>writer = new SpecificDatumWriter<GenericRecord>(schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(payload, encoder);
        encoder.flush();
        out.close();

        byte[] serializedBytes = out.toByteArray();
        System.out.println("Sending message in bytes : " + serializedBytes );
        KeyedMessage<String, byte[]> message = new KeyedMessage<String, byte[]>("kafkatopic", serializedBytes);
        producer.send(message);
        producer.close();
    }


    public static void main(String[] args) throws IOException, DecoderException {
        AvroProducer test = new AvroProducer();

        ClassLoader loader = AvroProducer.class.getClassLoader();
        
        File file = new File(loader.getResource("schema/book.avsc").getFile());
        
        String path = file.getAbsolutePath();
        		
        Schema schema = new Schema.Parser().parse(new File(path));
               
        test.producer(schema);
    }
}

