package samza.examples.sql.case2_3;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Shuffle write in group by.
 * Created by liudp on 2016/2/26.
 */
public class JoinStreamTask implements WindowableTask, StreamTask, InitableTask {
    private int count = 0;
    private Set<String> titles = new HashSet<String>();
    private KeyValueStore<String, Integer> store;

    // counter
    private static int counterProcessed = 0;
    private static int counterMatched = 0;
    private static Long lastClock = System.currentTimeMillis();
    private static final int COUNTER_BATCH = 100000;
    private static final SystemStream COUNTER_OUTPUT_STREAM = new SystemStream("kafka", "samza-filter-counter-output");
    private static final SystemStream GROUPBY_OUTPUT_STREAM = new SystemStream("kafka", "samza-2_1-groupby-output");

    // input
    // output
    // groupby
    private static final String fieldSplit = ",";
    private int groupByOutputStreamPartitions;

    public void init(Config config, TaskContext context) {
        groupByOutputStreamPartitions = Integer.parseInt(config.get(
                "systems.kafka.groupby.outputstream.partitions"));
//        this.store = (KeyValueStore<String, Integer>) context.getStore("samza-sql-countstar-count");
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        String msg = (String) envelope.getMessage();
        if (msg == null) {
            return;
        }
        // NOTE: filter
        int pz_idColId = 1;
        int ydz_ipColId = 2;
        String[] values = msg.split(fieldSplit, 2 + 2);
        int pz_id = Integer.parseInt(values[pz_idColId]);
        int ydz_ip = Integer.parseInt(values[ydz_ipColId]);
        if (values != null && values.length > pz_idColId) {
            if (values[pz_idColId] != null &&
                    (pz_id == 1111110
                            || pz_id == 1111111
                            || pz_id == 1111112)
                    ) {
                // shuffle, use ydz_ip to ignore data skew
                int targetPart = ydz_ip % groupByOutputStreamPartitions;
                collector.send(new OutgoingMessageEnvelope(GROUPBY_OUTPUT_STREAM,
                        targetPart, null,
                        // only send fields we need
                        values[pz_idColId] + fieldSplit + values[ydz_ipColId]));
                counterMatched++;
            }
        } else {
            // invalid msg
        }
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        collector.send(new OutgoingMessageEnvelope(COUNTER_OUTPUT_STREAM,
                "filter process:" + counterProcessed
                        + "    match:" + counterMatched + "   "
                        + counterProcessed * 1000 / (System.currentTimeMillis() - lastClock)
                        + " msg/s"));
        lastClock = System.currentTimeMillis();
        counterProcessed = 0;
        counterMatched = 0;
    }

    public static void main(String args[]) {
        String str = "helloa1a2a3a4a5askdjfalksef";
        System.out.println(Arrays.toString(str.split("a")));
        System.out.println(Arrays.toString(str.split("a", 2)));
        System.out.println(Arrays.toString(str.split("a", 3)));
        System.out.println(Arrays.toString(str.split("a", 4)));

        long clock = System.currentTimeMillis();
//        for(int i=0; i<10000000; i++){
            str.split("a", 5);
//        }
        System.out.println(System.currentTimeMillis() - clock);


    }
//
//    /**
//     *
//     * @param str
//     * @param ch
//     * @param limit result length or occur times of ch
//     * @return "a,b,c,d" ',' 2  => [a,b]
//     */
//    public static String[] splitLeft(String str, char ch, int limit){
//
//        int off = 0;
//        int preNext = -1;
//        int next = str.indexOf(ch, off);
//
//        while( limit > 0
//                && (
//                (next > 0)
//                        || ()
//                )
//                ){
//
//
//
//
//            limit --;
//        }
//
//
//
//    }

}
