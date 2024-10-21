package in.ionic.kafka.connect;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static in.ionic.kafka.connect.LogToService.log;

public class SimpleLambdaSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(SimpleLambdaSinkTask.class);
    AWSLambda awsLambda;
    private static final ObjectMapper objectMapper = new ObjectMapper();


    @Override
    public void start(Map<String, String> props) {
        this.awsLambda = new AWSLambda(props.get("aws.access.key"), props.get("aws.secret.key"),
                props.get("aws.lambda.function.region"), props.get("aws.lambda.function.name"));
        log("Starting task with settings: ");
        log(props.toString());
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        Map<String, ArrayList<HashMap<String, Object>>> recordsMap = new HashMap<>();
        ArrayList<HashMap<String, Object>> recordList = new ArrayList<>();
        for (SinkRecord record : records) {
            HashMap<String, Object> recordMap = new HashMap<>();
            recordMap.put("key", record.key());
            recordMap.put("value", record.value());
            recordMap.put("topic", record.topic());
            recordMap.put("partition", record.kafkaPartition());
            recordMap.put("offset", record.kafkaOffset());
            recordList.add(recordMap);
            log(String.format("Processing record: key= %s, value= %s", record.key(), record.value().toString()));
        }
        recordsMap.put("records", recordList);
        try {
            String jsonString = objectMapper.writeValueAsString(recordsMap);
            log(jsonString);
            awsLambda.invoke(jsonString);
        } catch (JsonProcessingException e) {
            log(e.toString());
            log(e.getMessage());
            throw new RuntimeException(e);
        }

    }

    @Override
    public void stop() {
        awsLambda.close();
    }

    @Override
    public String version() {
        return "1.0.1";
    }
}
