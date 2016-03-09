package samza.examples.sql.case2_3;

import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;

import java.text.SimpleDateFormat;
import java.util.*;

public class CountStreamTask implements StreamTask, InitableTask, WindowableTask {

    private static final SimpleDateFormat sdf = new SimpleDateFormat();

    private static final String fieldSplit = ",";

    // order column
    private static int orderColId;

    private static final SystemStream LIMIT_OUTPUT_STREAM = new SystemStream("kafka", "samza-2_1-limit-output");
    private static final SystemStream DEBUG_STREAM = new SystemStream("kafka", "samza-debug");

    // group hashmap
    private static Map<String[], Integer> stats = new HashMap();

    public void init(Config config, TaskContext context) {
        // store
//      this.store = (KeyValueStore<String, Integer>) context.getStore("samza-2_1");
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {

        int pz_idColId = 0;
        int ydz_ipColId = 1;

        String msg = (String) envelope.getMessage();
        if (msg != null) {
            String[] values = msg.split(fieldSplit);
            // increment for count(*)
            System.out.println("values.length is " + values.length);
            if (values != null && values.length == 2) {
                Integer cnt = stats.get(values);
                System.out.println(Arrays.toString(values) + " cnt:" + cnt);
                if (cnt == null) {
                    stats.put(values, 0);
                }
                stats.put(values, cnt + 1);
            }
        } else {
            // invalid message
        }
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) {

        // order by count(*)
        //  reverse key,value
        long clock = System.currentTimeMillis();
        Map<Integer, String[]> inversedStats = new HashMap<>();
        for (Map.Entry<String[], Integer> entry : stats.entrySet()) {
            inversedStats.put(entry.getValue(), entry.getKey());
        }
        collector.send(new OutgoingMessageEnvelope(DEBUG_STREAM, "k,v reverse consumed:  " +
                (System.currentTimeMillis() - clock)));
        clock = System.currentTimeMillis();
        // put in tree map
        Map<Integer, String[]> insersedStatsInTreeMap = new TreeMap<>(inversedStats);
        Set set = insersedStatsInTreeMap.entrySet();
        Iterator iterator = set.iterator();
        collector.send(new OutgoingMessageEnvelope(DEBUG_STREAM, "put to treemap and get " +
                "iterator consumed: " +
                (System.currentTimeMillis() - clock)));
        clock = System.currentTimeMillis();
        int limit = 100;
        while (iterator.hasNext() && limit > 0) {
            Map.Entry<Integer, String[]> msg = (Map.Entry) iterator.next();
            if (msg != null) {
                collector.send(new OutgoingMessageEnvelope(LIMIT_OUTPUT_STREAM,
                        msg.getValue()[0] + fieldSplit + fieldSplit + msg.getValue()[1] +
                                fieldSplit + msg.getKey()
                ));
            }
            limit--;
        }
        collector.send(new OutgoingMessageEnvelope(DEBUG_STREAM, "send limit 100 result consumed: " +
                (System.currentTimeMillis() - clock)));

        // limit 100
    }

    public static void main(String[] args) {

        HashMap<Integer, String> hmap = new HashMap<Integer, String>();

        long clock = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            hmap.put(i % 1000001, i % 10000 + "");
        }
        System.out.println("insert consumed: " + (System.currentTimeMillis() - clock));

        System.out.println("Before Sorting:");
        Set set = hmap.entrySet();
        Iterator iterator = set.iterator();
        int i = 0;
        while (iterator.hasNext() && i < 10) {
            Map.Entry me = (Map.Entry) iterator.next();
            System.out.print(me.getKey() + ": ");
            System.out.println(me.getValue());
            i++;
        }
        clock = System.currentTimeMillis();
        Map<Integer, String> map = new TreeMap<Integer, String>(hmap);
        System.out.println("After Sorting:");
        Set set2 = map.entrySet();
        Iterator iterator2 = set2.iterator();
        System.out.println("put to treemap consumed: " + (System.currentTimeMillis() - clock));
        clock = System.currentTimeMillis();
        i = 0;
        int cnt = 0;
        while (iterator2.hasNext() && i < 10) {
            Map.Entry me2 = (Map.Entry) iterator2.next();
            if (me2 != null)
                cnt++;
//            System.out.print(me2.getKey() + ": ");
//            System.out.println(me2.getValue());
            i++;
        }
        System.out.println(cnt);
        System.out.println("iterator 10 consumed: " + (System.currentTimeMillis() - clock));
    }
}
