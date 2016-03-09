package samza.examples.sql;

import org.apache.samza.job.JobRunner;

import java.io.File;

/**
 * Created by liudp on 2016/2/29.
 */
public class Test {
    public static void main(String args[]) {

        JobRunner.main(
                ("--config-factory=org.apache.samza.config.factories.PropertiesConfigFactory" +
                        " --config-path=D:\\git\\samza-hello-samza\\src\\main\\config\\samza-sql-countstar.properties").split(" "));

    }
}
