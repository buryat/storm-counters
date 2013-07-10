package buryat.storm.tools;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.elasticloadbalancing.model.LoadBalancerDescription;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancing;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient;
import com.amazonaws.services.elasticloadbalancing.model.DescribeLoadBalancersRequest;
import com.amazonaws.services.elasticloadbalancing.model.DescribeLoadBalancersResult;
import com.amazonaws.services.elasticloadbalancing.model.Instance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AmazonAWS {
    static AmazonElasticLoadBalancing elb;
    static AmazonEC2 ec2;

    public AmazonAWS(String key, String secret) {
        BasicAWSCredentials credentials = new BasicAWSCredentials(key, secret);

        elb = new AmazonElasticLoadBalancingClient(credentials);
        ec2 = new AmazonEC2Client(credentials);
    }

    private List<String> elbs;

    public void setElbs(String[] elbs) {
        this.elbs = Arrays.asList(elbs);
    }

    public List<String> getInstances() {
        DescribeLoadBalancersResult response = elb.describeLoadBalancers(new DescribeLoadBalancersRequest(this.elbs));

        List<String> allInstances = new ArrayList<String>();

        for (LoadBalancerDescription loadBalancer : response.getLoadBalancerDescriptions()) {
            List<Instance> instances = loadBalancer.getInstances();

            for (Instance instance : instances) {
                allInstances.add(instance.getInstanceId());
            }
        }

        List<String> hosts = new ArrayList<String>();

        DescribeInstancesRequest request = new DescribeInstancesRequest();
        request.setInstanceIds(allInstances);
        DescribeInstancesResult result = ec2.describeInstances(request);
        for (Reservation reservation : result.getReservations()) {
            for (com.amazonaws.services.ec2.model.Instance instance : reservation.getInstances()) {
                hosts.add(instance.getPublicDnsName());
            }
        }

        return hosts;
    }
}
