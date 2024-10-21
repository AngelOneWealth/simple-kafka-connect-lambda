package in.ionic.kafka.connect;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import static in.ionic.kafka.connect.LogToService.log;


public class AWSLambda {
    private final LambdaClient lambdaClient;
    String functionName;

    public AWSLambda(String accessKey, String secretKey, String region, String functionName) {
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessKey, secretKey);
        this.lambdaClient = LambdaClient.builder()
                .region(Region.of(region)) // Set your region
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build();
        this.functionName = functionName;
    }

    public void invoke(String payload) {
        InvokeRequest invokeRequest = InvokeRequest.builder()
                .functionName(functionName)
                .payload(SdkBytes.fromUtf8String(payload))
                .build();

        InvokeResponse invokeResponse = lambdaClient.invoke(invokeRequest);
        log(String.valueOf(invokeResponse.statusCode()));
        log(invokeResponse.payload().asUtf8String());

    }

    public void close() {
        lambdaClient.close();
    }

}
