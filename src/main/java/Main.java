

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


        //STEP 1 - N3
        HadoopJarStepConfig step1cfg = new HadoopJarStepConfig()
                .withJar("s3://hadoopassignment2/stepsJars/step1.jar")
                .withMainClass("Step1")
                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data")
                .withArgs("s3://hadoopassignment2/output/"+rand+"-1/");

        StepConfig step1 = new StepConfig()
                .withName("Step 1")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(step1cfg);


        //STEP 2 - N2
        HadoopJarStepConfig step2cfg = new HadoopJarStepConfig()
                .withJar("s3://hadoopassignment2/stepsJars/step2.jar")
                .withMainClass("Step2")
                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data")
                .withArgs("s3://hadoopassignment2/output/"+rand+"-2/");

        StepConfig step2 = new StepConfig()
                .withName("Step 2")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(step2cfg);


        //STEP 3 - N1
        HadoopJarStepConfig step3cfg = new HadoopJarStepConfig()
                .withJar("s3://hadoopassignment2/stepsJars/step3.jar")
                .withMainClass("Step3")
                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data")
                .withArgs("s3://hadoopassignment2/output/"+rand+"-3/");
        StepConfig step3 = new StepConfig()
                .withName("Step 3")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(step3cfg);


        //STEP 4 - N3 N2
        HadoopJarStepConfig step4cfg = new HadoopJarStepConfig()
                .withJar("s3://hadoopassignment2/stepsJars/step4.jar")
                .withMainClass("Step4")
                .withArgs("s3://hadoopassignment2/output/"+rand+"-1/") //N3
                .withArgs("s3://hadoopassignment2/output/"+rand+"-2/") //N2
                .withArgs("s3://hadoopassignment2/output/"+rand+"-4/"); //output
        StepConfig step4 = new StepConfig()
                .withName("Step 4")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(step4cfg);

        //STEP 5 - N3 N2 N1
        HadoopJarStepConfig step5cfg = new HadoopJarStepConfig()
                .withJar("s3://hadoopassignment2/stepsJars/step5.jar")
                .withMainClass("Step5")
                .withArgs("s3://hadoopassignment2/output/"+rand+"-4/") //N3 N2
                .withArgs("s3://hadoopassignment2/output/"+rand+"-3/") //N1
                .withArgs("s3://hadoopassignment2/output/"+rand+"-5/"); //output
        StepConfig step5 = new StepConfig()
                .withName("Step 5")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(step5cfg);

        //STEP 6 - N3 N2 N1 C2
        HadoopJarStepConfig step6cfg = new HadoopJarStepConfig()
                .withJar("s3://hadoopassignment2/stepsJars/step6.jar")
                .withMainClass("Step6")
                .withArgs("s3://hadoopassignment2/output/"+rand+"-5/") //N3 N2 N1
                .withArgs("s3://hadoopassignment2/output/"+rand+"-2/") //C2
                .withArgs("s3://hadoopassignment2/output/"+rand+"-6/"); //output
        StepConfig step6 = new StepConfig()
                .withName("Step 6")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(step6cfg);

        //STEP 7 - N3 N2 N1 C2 C1
        HadoopJarStepConfig step7cfg = new HadoopJarStepConfig()
                .withJar("s3://hadoopassignment2/stepsJars/step7.jar")
                .withMainClass("Step7")
                .withArgs("s3://hadoopassignment2/output/"+rand+"-6/") //N3 N2 N1 C2
                .withArgs("s3://hadoopassignment2/output/"+rand+"-3/") //C1
                .withArgs("s3://hadoopassignment2/output/"+rand+"-7/"); //output
        StepConfig step7 = new StepConfig()
                .withName("Step 7")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(step7cfg);

        //STEP 8 - C0
        HadoopJarStepConfig step8cfg = new HadoopJarStepConfig()
                .withJar("s3://hadoopassignment2/stepsJars/step8.jar")
                .withMainClass("Step8")
                .withArgs("s3://hadoopassignment2/output/"+rand+"-3/") //N1
                .withArgs("s3://hadoopassignment2/output/"+rand+"-8/"); //output
        StepConfig step8 = new StepConfig()
                .withName("Step 8")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(step8cfg);

        //STEP 9 - N3 N2 N1 C2 C1 C0
        HadoopJarStepConfig step9cfg = new HadoopJarStepConfig()
                .withJar("s3://hadoopassignment2/stepsJars/step9.jar")
                .withMainClass("Step9")
                .withArgs("s3://hadoopassignment2/output/"+rand+"-7/") //N3 N2 N1 C2 C1
                .withArgs("s3://hadoopassignment2/output/"+rand+"-8/") //C0
                .withArgs("s3://hadoopassignment2/output/"+rand+"-9/"); //output
        StepConfig step9 = new StepConfig()
                .withName("Step 9")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(step9cfg);

        //STEP 10 - calc prob
        HadoopJarStepConfig step10cfg = new HadoopJarStepConfig()
                .withJar("s3://hadoopassignment2/stepsJars/step10.jar")
                .withMainClass("Step10")
                .withArgs("s3://hadoopassignment2/output/"+rand+"-9/") //N3 N2 N1 C2 C1
                .withArgs("s3://hadoopassignment2/output/"+rand+"-10/"); //output
        StepConfig step10 = new StepConfig()
                .withName("Step 10")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(step10cfg);

        RunJobFlowRequest request = new RunJobFlowRequest()
                .withName("Distributed-Ass2")
                .withReleaseLabel("emr-6.0.0-beta")
                .withSteps(step1, step2, step3, step4, step5, step6, step7, step8, step9, step10)
                .withLogUri("s3://hadoopassignment2/logs/")
                .withServiceRole("NE2")
                .withJobFlowRole("NE")
                .withInstances(new JobFlowInstancesConfig()
                        .withEc2KeyName("Admin")
                        .withInstanceCount(5) // CLUSTER SIZE
                        .withKeepJobFlowAliveWhenNoSteps(false)
                        .withMasterInstanceType("m3.xlarge")
                        .withSlaveInstanceType("m3.xlarge"));

        RunJobFlowResult result = emr.runJobFlow(request);
        System.out.println("JobFlow id: "+result.getJobFlowId());
    }
}