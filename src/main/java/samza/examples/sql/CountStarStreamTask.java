package samza.examples.sql;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;

/**
 * Created by liudp on 2016/2/26.
 */
public class CountStarStreamTask implements StreamTask, InitableTask, WindowableTask {
  private int count = 0;
  private Set<String> titles = new HashSet<String>();
  private KeyValueStore<String, Integer> store;

  public void init(Config config, TaskContext context) {
    this.store = (KeyValueStore<String, Integer>) context.getStore("samza-sql-countstar-count");
  }

  @SuppressWarnings("unchecked")
  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    String msg = (String) envelope.getMessage();

    Integer msgsAllTime = store.get("count-msgs-all-time");
    if (msgsAllTime == null) msgsAllTime = 0;

    if(msg!=null) {
      store.put("count-msgs-all-time", msgsAllTime + 1);
    }

    count += 1;
  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) {

    // just send count
    collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "samza-sql-countstar-count"), count));

    // Reset counts after windowing.
    count = 0;
  }

}
