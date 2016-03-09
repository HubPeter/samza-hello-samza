package samza.examples.sql.test;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.Serializable;
import java.util.Properties;
import java.util.Random;

/**
 * Generate test data.
 * 1. create topic
 * kafka-topics --create --topic samza-3cols_parts30 --replication-factor 2 --partitions 20 --zookeeper m106:2181/kafka
 * 2. run application
 * tar -xzf hello-samza-0.10.0-dist.tar.gz
 * export CLASSPATH=lib/*
 * java samza.examples.sql.test.DataFactory samza-3cols_parts30 15 100000000
 */


public class DataFactory extends Thread implements Serializable {
    private static String dataSet;
    private String[][] msgs;
    private kafka.javaapi.producer.Producer<Integer, byte[]> producer;
    private static final int MODE = 100001;
    private static String topic;
    private static Integer partitions;
    private static Integer threadCount;
    private static Integer msgCount;

    //    DataFactory() {
    public DataFactory() {
//
//        URL urls[] = new URL[]{};l`
//        URLClassLoader loader = new URLClassLoader(urls, ClassLoader.getSystemClassLoader():
//    );
        Properties props = new Properties();
//        props.put("serializer.class", "iie.udps.example.spark.streaming.JavaEncoder");
        props.put("metadata.broker.list", "m106:9092");
        props.put("producer.type", "async");
        props.put("batch.size", "200");
        props.put("zk.connect", "172.16.8.106:2181/kafka");
        // Use random partitioner. Don't need the key type. Just set it to Integer.
        // The message is of type String.
        producer = new kafka.javaapi.producer.Producer<Integer, byte[]>(
                new ProducerConfig(props)
        );
    }

    public DataFactory(String[][] msgs) {
        this.msgs = msgs;
    }

    public static void main(String args[]) throws InterruptedException {

        if (args.length != 5) {
            System.out.println(args.length);
            System.out.println("Usage: java samza.examples.sql.test.DataFactory <topic> <dataset>" +
                    " <partitions> <threads> <msg-count>\n");
            System.exit(1);
        }
        topic = args[0];
        dataSet = args[1];
        partitions = Integer.parseInt(args[2]);
        threadCount = Integer.parseInt(args[3]);
        msgCount = Integer.parseInt(args[4]);
        System.out.println("topic: " + topic);
        System.out.println("partitions: " + partitions);
        System.out.println("msg count: " + msgCount);

        DataFactory[] factories = new DataFactory[threadCount];
        for (int i = 0; i < threadCount; i++) {

            if (dataSet.equals("3cols")) {
                factories[i] = new DataFactory(Datas.msgs3Cols);
            } else if (dataSet.equals("url_rz")) {
                factories[i] = new DataFactory(Datas.msgsUrlRz);
            } else {
                System.out.println("dataset not found: " + dataSet);
                System.exit(1);
            }
        }
        for (int i = 0; i < threadCount; i++) {
            factories[i].start();
            System.out.println("data factory " + i + " started.");
        }
        for (int i = 0; i < threadCount; i++) {
            factories[i].join();
        }
    }

    private static byte[] getBody(int len) {
        Random random = new Random();
        byte[] msg = new byte[len];
        random.nextBytes(msg);
        return msg;
    }

    public void sendDebug(String topic, String msg) {
        sendDebug(topic, msg.getBytes(), 0);
        System.out.println(msg);
    }

    private void sendDebug(String topic, byte[] body, int partKey) {
        producer.send(new KeyedMessage<Integer, byte[]>(topic, null, partKey,
                body));
    }

    @Deprecated
    public void sendDebug(String msg) {
        producer.send(new KeyedMessage<Integer, byte[]>("hive.founder.app1t1", null, 0,
                msg.getBytes()));
        System.out.println(msg);
    }

    public Producer<Integer, byte[]> getProducer() {
        return this.producer;
    }


    @Override
    public void run() {
        DataFactory producer = new DataFactory();


        String fieldSplit = ",";
        int batch = msgs.length;

        int partKey = 0;
        for (int loop = 0; loop < msgCount / batch; loop++) {
            if ((loop * batch) % 10000 == 0)
                System.out.println(loop * batch);
            for (int iMsg = 0; iMsg < msgs.length; iMsg++) {
                String[] msg = msgs[iMsg];
                byte[] body = getBytes(msg, fieldSplit);
                producer.sendDebug(topic, body, partKey % partitions);
                partKey++;
            }
        }
    }

    /**
     * get bytes from fields
     *
     * @param fields
     * @param split
     * @return
     */
    private static byte[] getBytes(String[] fields, String split) {
        if (fields == null || fields.length == 0) {
            return null;
        } else {
            String str = "";
            for(int i=0; i<fields.length; i++){
                str = str + fields[i] ;
                if(i<fields.length-1){
                    str = str + split;
                }
            }
            return str.getBytes();
        }
    }

    public static void main2(String args[]){


    }
}