import org.junit.Before;
import org.junit.Test;
import bitCask.storage.BitCask;
import bitCask.util.Constants;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.junit.Assert.assertEquals;

public class BitCaskTest {
    @Before
    public void tearDown() {
        Arrays.stream(Objects.requireNonNull(new File(Constants.dbDirectory).listFiles()))
                .forEach(File::delete);
    }

    @Test
    public void get() throws IOException {
        BitCask bitCask = new BitCask(Constants.dbDirectory);

        String key = "A";
        String value = "1";
        bitCask.put(key, value);
        key = "B";
        value = "2";
        bitCask.put(key, value);

        String retrievedValue = bitCask.get("A");
        assertEquals("1", retrievedValue);
    }

    @Test
    public void get_afterCompaction() throws IOException {
        BitCask bitCask = new BitCask(Constants.dbDirectory);
        bitCask.put("A", "1");

        bitCask = new BitCask(Constants.dbDirectory);
        bitCask.put("A", "2");
        bitCask.put("AC", "3");

        bitCask = new BitCask(Constants.dbDirectory);
        bitCask.put("A", "3");
        bitCask.put("B", "3");

        bitCask = new BitCask(Constants.dbDirectory);
        bitCask.mergeAndCompaction();

        bitCask = new BitCask(Constants.dbDirectory);

        String retrievedValue = bitCask.get("A");
        assertEquals("3", retrievedValue);
    }

    @Test
    public void put() {
    }

    @Test
    public void mergeAndCompaction() throws IOException {
        BitCask bitCask = new BitCask(Constants.dbDirectory);
        bitCask.put("A", "1");
        bitCask = new BitCask(Constants.dbDirectory);
        bitCask.put("A", "2");
        bitCask = new BitCask(Constants.dbDirectory);
        bitCask.put("A", "3");
        bitCask = new BitCask(Constants.dbDirectory);

        bitCask.mergeAndCompaction();

        assertEquals(5, Objects.requireNonNull(new File(Constants.dbDirectory).listFiles()).length);

        String retrievedValue = bitCask.get("A");
        assertEquals("3", retrievedValue);
    }
}