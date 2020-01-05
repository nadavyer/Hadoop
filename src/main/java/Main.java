

import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

public class Main {

    public static void main(String[] args) {

        AWSCredentials credentialsProfile;
        try {
            credentialsProfile = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load credentials from .aws/credentials file. " +
                            "Make sure that the credentials file exists and the profile name is specified within it.",
                    e);
        }

        AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentialsProfile))
                .withRegion(Regions.US_EAST_1)
                .build();
        String rand = UUID.randomUUID().toString();

     //STEP 1
        HadoopJarStepConfig step1cfg = new HadoopJarStepConfig()
                .withJar("s3://hadoopassignment2/stepJars/step1.jar")
                .withMainClass("Step1")
                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/")
                .withArgs("s3://hadoopassignment2/output/"+rand+"-1/");

        StepConfig step1 = new StepConfig()
                .withName("Step 1")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(step1cfg);


        RunJobFlowRequest request = new RunJobFlowRequest()
                .withName("Distributed-Ass2")
                .withReleaseLabel("emr-5.3.1")
                .withSteps(step1)
                .withLogUri("s3://hadoopassignment2/logs/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withInstances(new JobFlowInstancesConfig()
                        .withEc2KeyName("Admin")
                        .withInstanceCount(20) // CLUSTER SIZE
                        .withKeepJobFlowAliveWhenNoSteps(false)
                        .withMasterInstanceType("m1.small")
                        .withSlaveInstanceType("m1.small"));



        RunJobFlowResult result = emr.runJobFlow(request);
        System.out.println("JobFlow id: "+result.getJobFlowId());
    }
}