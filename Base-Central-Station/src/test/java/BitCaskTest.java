import bitCask.exception.DirectoryNotFoundException;
import bitCask.storage.BitCask;
import bitCask.util.DirectoryConstants;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BitCaskTest {
    @Before
    public void tearDown() {
        Arrays.stream(Objects.requireNonNull(new File(DirectoryConstants.dbDirectory).listFiles()))
                .forEach(File::delete);
    }

    @Test
    public void read() throws IOException, DirectoryNotFoundException {
        BitCask bitCask = new BitCask();
        bitCask.open(DirectoryConstants.dbDirectory);

        int key = 1;
        String value = "{station_id:1, s_no:6, battery_status:'medium', status_timestamp:1685210887, weather:{humidity:83.0, temperature:14.7, wind_speed:19.5}}";
        bitCask.put(ByteBuffer.allocate(Integer.BYTES).putInt(key).array(), value.getBytes());

        byte[] retrievedValue = bitCask.get(ByteBuffer.allocate(Integer.BYTES).putInt(key).array());
        assertEquals(Arrays.toString(value.getBytes()), Arrays.toString(retrievedValue));
    }

    @Test
    public void delete() throws IOException, DirectoryNotFoundException {
        BitCask bitCask = new BitCask();
        bitCask.open(DirectoryConstants.dbDirectory);

        int key = 1;
        String value = "{station_id:1, s_no:6, battery_status:'medium', status_timestamp:1685210887, weather:{humidity:83.0, temperature:14.7, wind_speed:19.5}}";
        bitCask.put(ByteBuffer.allocate(Integer.BYTES).putInt(key).array(), value.getBytes());

        bitCask.delete(ByteBuffer.allocate(Integer.BYTES).putInt(key).array());
        byte[] retrievedValue = bitCask.get(ByteBuffer.allocate(Integer.BYTES).putInt(key).array());
        assertNull(retrievedValue);
    }

    @Test
    public void readingAfterCompaction() throws IOException, DirectoryNotFoundException {
        BitCask bitCask = new BitCask();
        bitCask.open(DirectoryConstants.dbDirectory);
        bitCask.put(ByteBuffer.allocate(Integer.BYTES).putInt(1).array(), "{station_id:1, s_no:6, battery_status:'medium', status_timestamp:1685210887, weather:{humidity:83.0, temperature:14.7, wind_speed:19.5}}".getBytes());

        bitCask = new BitCask();
        bitCask.open(DirectoryConstants.dbDirectory);
        bitCask.put(ByteBuffer.allocate(Integer.BYTES).putInt(1).array(), "{station_id:1, s_no:16, battery_status:'medium', status_timestamp:1685210897, weather:{humidity:90.0, temperature:14.7, wind_speed:19.5}}".getBytes());
        bitCask.put(ByteBuffer.allocate(Integer.BYTES).putInt(2).array(), "{station_id:2, s_no:62, battery_status:'medium', status_timestamp:1685210900, weather:{humidity:83.2321, temperature:14.7, wind_speed:119.5}}".getBytes());

        bitCask = new BitCask();
        bitCask.open(DirectoryConstants.dbDirectory);
        bitCask.put(ByteBuffer.allocate(Integer.BYTES).putInt(1).array(), "{station_id:1, s_no:126, battery_status:'medium', status_timestamp:1685210940, weather:{humidity:813.0, temperature:141.7, wind_speed:1239.5}}".getBytes());
        bitCask.put(ByteBuffer.allocate(Integer.BYTES).putInt(3).array(), "{station_id:3, s_no:63, battery_status:'medium', status_timestamp:1685210950, weather:{humidity:833.0, temperature:1421.7, wind_speed:192.5}}".getBytes());

        bitCask.mergeAndCompaction();

        bitCask = new BitCask();
        bitCask.open(DirectoryConstants.dbDirectory);

        byte[] retrievedValue = bitCask.get(ByteBuffer.allocate(Integer.BYTES).putInt(1).array());
        assertEquals(Arrays.toString(("{station_id:1, s_no:126, battery_status:'medium', " +
                        "status_timestamp:1685210940, weather:{humidity:813.0, temperature:141.7," +
                        " wind_speed:1239.5}}").getBytes()),
                Arrays.toString(retrievedValue));
    }

    @Test
    public void deleteAfterCompaction() throws IOException, DirectoryNotFoundException {
        BitCask bitCask = new BitCask();
        bitCask.open(DirectoryConstants.dbDirectory);
        bitCask.put(ByteBuffer.allocate(Integer.BYTES).putInt(1).array(), "{station_id:1, s_no:6, battery_status:'medium', status_timestamp:1685210887, weather:{humidity:83.0, temperature:14.7, wind_speed:19.5}}".getBytes());

        bitCask.delete(ByteBuffer.allocate(Integer.BYTES).putInt(1).array());

        bitCask = new BitCask();
        bitCask.open(DirectoryConstants.dbDirectory);

        byte[] retrievedValue = bitCask.get(ByteBuffer.allocate(Integer.BYTES).putInt(1).array());
        assertNull(retrievedValue);

        bitCask.put(ByteBuffer.allocate(Integer.BYTES).putInt(1).array(), "{station_id:1, s_no:16, battery_status:'medium', status_timestamp:1685210897, weather:{humidity:90.0, temperature:14.7, wind_speed:19.5}}".getBytes());
        bitCask.put(ByteBuffer.allocate(Integer.BYTES).putInt(2).array(), "{station_id:2, s_no:62, battery_status:'medium', status_timestamp:1685210900, weather:{humidity:83.2321, temperature:14.7, wind_speed:119.5}}".getBytes());

        retrievedValue = bitCask.get(ByteBuffer.allocate(Integer.BYTES).putInt(1).array());
        assertEquals(Arrays.toString(("{station_id:1, s_no:16, battery_status:'medium', status_timestamp:1685210897, " +
                        "weather:{humidity:90.0, temperature:14.7, wind_speed:19.5}}").getBytes()),
                Arrays.toString(retrievedValue));

        bitCask = new BitCask();
        bitCask.open(DirectoryConstants.dbDirectory);
        bitCask.put(ByteBuffer.allocate(Integer.BYTES).putInt(1).array(), "{station_id:1, s_no:126, battery_status:'medium', status_timestamp:1685210940, weather:{humidity:813.0, temperature:141.7, wind_speed:1239.5}}".getBytes());
        bitCask.put(ByteBuffer.allocate(Integer.BYTES).putInt(3).array(), "{station_id:3, s_no:63, battery_status:'medium', status_timestamp:1685210950, weather:{humidity:833.0, temperature:1421.7, wind_speed:192.5}}".getBytes());

        retrievedValue = bitCask.get(ByteBuffer.allocate(Integer.BYTES).putInt(1).array());
        assertEquals(Arrays.toString(("{station_id:1, s_no:126, battery_status:'medium', status_timestamp:1685210940, " +
                        "weather:{humidity:813.0, temperature:141.7, wind_speed:1239.5}}").getBytes()),
                Arrays.toString(retrievedValue));

        retrievedValue = bitCask.get(ByteBuffer.allocate(Integer.BYTES).putInt(3).array());
        assertEquals(Arrays.toString(("{station_id:3, s_no:63, battery_status:'medium', status_timestamp:1685210950," +
                        " weather:{humidity:833.0, temperature:1421.7, wind_speed:192.5}}").getBytes()),
                Arrays.toString(retrievedValue));

        bitCask.delete(ByteBuffer.allocate(Integer.BYTES).putInt(1).array());

        retrievedValue = bitCask.get(ByteBuffer.allocate(Integer.BYTES).putInt(3).array());
        assertEquals(Arrays.toString(("{station_id:3, s_no:63, battery_status:'medium', status_timestamp:1685210950," +
                        " weather:{humidity:833.0, temperature:1421.7, wind_speed:192.5}}").getBytes()),
                Arrays.toString(retrievedValue));

        bitCask.mergeAndCompaction();

        retrievedValue = bitCask.get(ByteBuffer.allocate(Integer.BYTES).putInt(3).array());
        assertEquals(Arrays.toString(("{station_id:3, s_no:63, battery_status:'medium', status_timestamp:1685210950," +
                        " weather:{humidity:833.0, temperature:1421.7, wind_speed:192.5}}").getBytes()),
                Arrays.toString(retrievedValue));

        retrievedValue = bitCask.get(ByteBuffer.allocate(Integer.BYTES).putInt(1).array());
        assertNull(retrievedValue);

        bitCask = new BitCask();
        bitCask.open(DirectoryConstants.dbDirectory);

        retrievedValue = bitCask.get(ByteBuffer.allocate(Integer.BYTES).putInt(1).array());
        assertNull(retrievedValue);
    }
}
