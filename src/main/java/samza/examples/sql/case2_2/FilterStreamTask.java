package samza.examples.sql.case2_2;

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
public class FilterStreamTask implements WindowableTask, StreamTask, InitableTask {
    private int count = 0;
    private Set<String> titles = new HashSet<String>();
    private KeyValueStore<String, Integer> store;

    // counter
    private int counterProcessed = 0;
    private int counterMatched = 0;
    private Long lastClock = System.currentTimeMillis();
    private final int COUNTER_BATCH = 100000;
    private final SystemStream DEBUG_STREAM = new SystemStream("kafka", "samza-debug");
    private final SystemStream FILTER_OUTPUT_STREAM = new SystemStream("kafka", "samza-2_2-output");

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

        // empty loop
//        counterProcessed ++;
        // end of empty loop

        // my rela code
        counterProcessed++;
        String msg = (String) envelope.getMessage();
        if (msg == null) {
            return;
        }
        // NOTE: filter
        int ydz_ipColId = 2;
        int md_dzColId = 6;
        int fd_ljColId = 7;
        String[] values = msg.split(fieldSplit, 9);
        int ydz_ip = Integer.parseInt(values[ydz_ipColId]);
        if (values != null && values.length == fd_ljColId + 1) {
            if (values[ydz_ipColId] != null &&
                    // real is 3658617496
                    ydz_ip == 1111110
                    ) {
//                collector.send(new OutgoingMessageEnvelope(FILTER_OUTPUT_STREAM,
//                        // concat(md_dz 6,fd_lj 7)
//                        values[md_dzColId] + fieldSplit + values[fd_ljColId]));
                counterMatched++;
            }
        } else {
            // invalid msg
        }
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        Long newClock = System.currentTimeMillis();
        collector.send(new OutgoingMessageEnvelope(DEBUG_STREAM,
                "filter process:" + counterProcessed
                        + "    match:" + counterMatched + "   "
                        + counterProcessed * 1000 / (newClock + 1 - lastClock)
                        + " msg/s"));
        counterProcessed = 0;
        counterMatched = 0;
        lastClock = newClock;
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
