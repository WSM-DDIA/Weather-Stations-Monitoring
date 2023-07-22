package bitCask.storage;

import java.io.File;
import java.io.IOException;

public interface IBitCask {
    int open(File directory) throws IOException;
    void put(byte[] key, byte[] value) throws IOException;

    byte[] get(byte[] key) throws IOException;

    void delete(byte[] key);

    void mergeAndCompaction() throws IOException;
}
