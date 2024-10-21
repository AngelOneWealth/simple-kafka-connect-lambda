package in.ionic.kafka.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static in.ionic.kafka.connect.LogToService.log;
import static in.ionic.kafka.connect.LogToService.setUrl;

@SuppressWarnings("unused")
public class SimpleLambdaSinkConnector extends SinkConnector {

    private static final HashMap<String, String> settings = new HashMap<>();
    @Override
    public String version() {
        return "1.0.1";
    }

    @Override
    public void start(Map<String, String> props) {
        if (props.containsKey("log.uri")) {
            setUrl(props.get("log.uri"));
        }
        log("Starting connector with settings: ");
        settings.putAll(props);
        log(settings.toString());

    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define("log.uri", ConfigDef.Type.STRING, ConfigDef.Importance.LOW, "URI to send logs to")
                .define("aws.lambda.function.name", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "AWS Lambda function name")
                .define("aws.lambda.function.region", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "AWS Lambda function region")
                .define("aws.access.key", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "AWS Access Key")
                .define("aws.secret.key", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "AWS Secret");
    }


    @Override
    public Class<? extends Task> taskClass() {
        return SimpleLambdaSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            taskConfigs.add(settings);
        }
        return taskConfigs;
    }
}
