package samza.examples.sql.case2_1;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;

import java.text.SimpleDateFormat;
import java.util.*;

public class OrderByStreamTask implements StreamTask, InitableTask, WindowableTask {

    private static final SimpleDateFormat sdf = new SimpleDateFormat();

    private static final String fieldSplit = ",";

    // order column
    private static int orderColId;

    private static final SystemStream LIMIT_OUTPUT_STREAM = new SystemStream("kafka", "samza-2_1-limit-output");
    private static final SystemStream DEBUG_STREAM = new SystemStream("kafka", "samza-debug");

    // group hashmap
    private static Map<String, Integer> stats = new HashMap();

    public void init(Config config, TaskContext context) {
        // store
//      this.store = (KeyValueStore<String, Integer>) context.getStore("samza-2_1");
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        String msg = (String) envelope.getMessage();
        System.out.println(msg);
        if (msg != null) {
            String[] values = msg.split(fieldSplit);
            System.out.println(Arrays.toString(values));
            // increment for count(*)
            System.out.println("values.length is " + values.length);
            if (values != null && values.length == 2) {
                String key = values[0] + fieldSplit + values[1];
                Integer cnt = Integer.parseInt(values[2]);
                Integer sum = stats.get(key);
                if (sum == null) {
                    stats.put(key, cnt);
                    System.out.println(Arrays.toString(values) + " init with cnt: 1");
                } else {
                    stats.put(key, sum + cnt);
                    System.out.println(Arrays.toString(values) + " incr with cnt: " + sum);
                }
            } else {
                // invalid message - msg is not formatted well
            }
        } else {
            // invalid message - msg is empty
        }
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) {

        // order by count(*)
        //  reverse key,value
        long clock = System.currentTimeMillis();
        Map<Integer, String> inversedStats = new HashMap<>();
        for (Map.Entry<String, Integer> entry : stats.entrySet()) {
            inversedStats.put(entry.getValue(), entry.getKey());
            System.out.println("put k,v into inversed map: " + entry.getKey() + " " + entry.getValue());
        }
        System.out.println("size of inversed map " + inversedStats.size());
        collector.send(new OutgoingMessageEnvelope(DEBUG_STREAM, "k,v reverse consumed:  " +
                (System.currentTimeMillis() - clock)));
        clock = System.currentTimeMillis();
        // put in tree map
        Map<Integer, String> insersedStatsInTreeMap = new TreeMap<>(inversedStats);
        Set set = insersedStatsInTreeMap.entrySet();
        Iterator iterator = set.iterator();
        collector.send(new OutgoingMessageEnvelope(DEBUG_STREAM, "put to treemap and get " +
                "iterator consumed: " +
                (System.currentTimeMillis() - clock)));
        clock = System.currentTimeMillis();
        int limit = 100;
        while (iterator.hasNext() && limit > 0) {
            Map.Entry<Integer, String> msg = (Map.Entry) iterator.next();
            collector.send(new OutgoingMessageEnvelope(LIMIT_OUTPUT_STREAM,
                    msg.getValue() + fieldSplit
                            + msg.getKey()
            ));
            System.out.println(" limit is " + limit);
            limit--;
        }
        collector.send(new OutgoingMessageEnvelope(DEBUG_STREAM, "send limit 100 result consumed: " +
                (System.currentTimeMillis() + 1 - clock)));
        // clean stas
        stats.clear();
        // limit 100
    }

    public static void main(String[] args) {

        HashMap<Integer, String> hmap = new HashMap<Integer, String>();

        long clock = System.currentTimeMillis();


        stats.put("1_1", 1);
        stats.put("2_2", 10);
        stats.put("3_3", 11);
        stats.put("4_4", 5);

        for (int i = 0; i < 100; i++) {
            hmap.put(i % 1000001, i % 10000 + "");
        }
        // order by count(*)
        //  reverse key,value
        clock = System.currentTimeMillis();
        Map<Integer, String> inversedStats = new HashMap<>();
        for (Map.Entry<String, Integer> entry : stats.entrySet()) {
            inversedStats.put(entry.getValue(), entry.getKey());
        }
        System.out.println("k,v reverse consumed:  " +
                (System.currentTimeMillis() - clock));
        clock = System.currentTimeMillis();
        // put in tree map
        Map<Integer, String> insersedStatsInTreeMap = new TreeMap<>(inversedStats);
        Set set = insersedStatsInTreeMap.entrySet();
        Iterator iterator = set.iterator();
        System.out.println("put to treemap and get " +
                "iterator consumed: " +
                (System.currentTimeMillis() - clock));
        clock = System.currentTimeMillis();
        int limit = 100;
        while (iterator.hasNext() && limit > 0) {
            Map.Entry<Integer, String[]> msg = (Map.Entry) iterator.next();
            if (msg != null) {
                System.out.println(
                        msg.getValue()[0] + fieldSplit + fieldSplit + msg.getValue()[1] +
                                fieldSplit + msg.getKey()
                );
            }
            limit--;
        }
        System.out.println("send limit 100 result consumed: " +
                (System.currentTimeMillis() - clock));
        // limit 100
    }
}
