package samza.examples.sql.test;

/**
 * Created by liudp on 2016/3/2.
 */
public class Test {
    public static void main(String args[]) throws InterruptedException {

        DataFactory.main(new String[]{"samza-3cols_parts100_2", "3cols", "100", "1", "100000000"});

    }
}
