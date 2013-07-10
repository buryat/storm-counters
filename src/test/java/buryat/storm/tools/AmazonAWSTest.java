package buryat.storm.tools;

import org.junit.*;
import java.util.List;

public class AmazonAWSTest {
    @Test
    public void getInstances() {
        AmazonAWS aws = new AmazonAWS("key", "secret");
        aws.setElbs(new String[]{"elb1"});
        List<String> instances = aws.getInstances();
        System.out.println(instances.toArray().length);
        System.out.println(instances);

        aws.setElbs(new String[]{"elb2"});
        instances = aws.getInstances();
        System.out.println(instances.toArray().length);
        System.out.println(instances);

        aws.setElbs(new String[]{"elb1", "elb2"});
        instances = aws.getInstances();
        System.out.println(instances.toArray().length);
        System.out.println(instances);
    }
}
