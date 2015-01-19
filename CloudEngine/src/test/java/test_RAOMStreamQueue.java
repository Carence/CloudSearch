import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

/**
 * Created by Clarence_Pz on 2015/1/19 @11:33.
 */
public class test_RAOMStreamQueue {
    @Before
    public void setproperty(){
        Properties properties = new Properties();
        properties.setProperty("log4j.rootLogger","DEBUG, Console");
        properties.setProperty("log4j.appender.Console","org.apache.log4j.ConsoleAppender");
        properties.setProperty("log4j.appender.Console.layout","org.apache.log4j.PatternLayout");
        properties.setProperty("log4j.appender.Console.layout.ConversionPattern", "(%r ms) [%t] %-5p: %c#%M %x: %m%n");
        PropertyConfigurator.configure(properties);
    }

    @Test
    public void do_test() {
        try {
            RAOMStreamQueue<String> raomStreamQueue = RAOMStreamQueue.getInstance();
            raomStreamQueue.push("hello");
            raomStreamQueue.push("world");
            raomStreamQueue.push("clarence_pz");
            String res = raomStreamQueue.pull();
            while (res != null) {
                System.out.println(res);
                res = raomStreamQueue.pull();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
