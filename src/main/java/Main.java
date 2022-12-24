import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

import java.util.UUID;
import utilities.Names;


public class Main {

    public static void main (String[] args) {
        String randomID = UUID.randomUUID().toString();
        AWSCredentialsProvider credentials = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        AmazonElasticMapReduce awsMapReduce = AmazonElasticMapReduceClientBuilder
                .standard()
                .withRegion(Names.REGION)
                .withCredentials(credentials)
                .build();

        HadoopJarStepConfig round1Config = config.hadoopStepConfig(Names.BUCKET + "Count.jar", new String[] {randomID});
        StepConfig count = config.stepConfig("count", round1Config, "TERMINATE_JOB_FLOW");

        HadoopJarStepConfig round2Config = config.hadoopStepConfig(Names.BUCKET + "NTCount.jar", new String[] {randomID});
        StepConfig ntCount = config.stepConfig("ntCount", round2Config, "TERMINATE_JOB_FLOW");

        HadoopJarStepConfig round3Config = config.hadoopStepConfig(Names.BUCKET + "Formula.jar", new String[] {randomID});
        StepConfig formula = config.stepConfig("formula", round3Config, "TERMINATE_JOB_FLOW");

        HadoopJarStepConfig round4Config = config.hadoopStepConfig(Names.BUCKET + "Sort.jar", new String[] {randomID});
        StepConfig sort = config.stepConfig("sort", round4Config, "TERMINATE_JOB_FLOW");



        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(10)
                .withMasterInstanceType(Names.INSTANCE_TYPE)
                .withSlaveInstanceType(Names.INSTANCE_TYPE)
                .withHadoopVersion("2.10.1")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withEc2KeyName(Names.KEY)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runJobFlowRequest = new RunJobFlowRequest()
                .withName("Ass2")
                .withReleaseLabel("emr-5.20.0")
                .withInstances(instances)
                .withSteps(count, ntCount, formula, sort)
                .withLogUri(Names.BUCKET + randomID + "/log/")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withServiceRole("EMR_DefaultRole");

        RunJobFlowResult runJobFlowResult = awsMapReduce.runJobFlow(runJobFlowRequest);
        String jobID = runJobFlowResult.getJobFlowId();
        System.out.println("Finished Job with id: " + jobID + "and Random ID: " + randomID);
    }
}
