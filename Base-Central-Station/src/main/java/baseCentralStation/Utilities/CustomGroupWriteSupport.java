package baseCentralStation.Utilities;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.util.Map;
import java.util.Objects;

import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;

public class CustomGroupWriteSupport extends WriteSupport<Group> {

    public static final String PARQUET_EXAMPLE_SCHEMA = "parquet.example.schema";
    private MessageType schema;
    private GroupWriter groupWriter;
    private final Map<String, String> extraMetaData;
    CustomGroupWriteSupport(MessageType schema, Map<String, String> extraMetaData) {
        this.schema = schema;
        this.extraMetaData = extraMetaData;
    }

    public static MessageType getSchema(Configuration configuration) {
        return parseMessageType(Objects.requireNonNull(configuration.get(PARQUET_EXAMPLE_SCHEMA), PARQUET_EXAMPLE_SCHEMA));
    }

    @Override
    public String getName() {
        return "custom";
    }

    @Override
    public org.apache.parquet.hadoop.api.WriteSupport.WriteContext init(Configuration configuration) {
        // if present, prefer the schema passed to the constructor
        if (schema == null) {
            schema = getSchema(configuration);
        }
        return new WriteContext(schema, this.extraMetaData);
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        groupWriter = new GroupWriter(recordConsumer, schema);
    }

    @Override
    public void write(Group record) {
        groupWriter.write(record);
    }

}
