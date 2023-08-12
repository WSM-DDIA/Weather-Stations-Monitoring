package bitCask.storage;

import bitCask.exception.DirectoryNotFoundException;
import bitCask.exception.KeyNotFoundException;

import java.io.FileNotFoundException;
import java.io.IOException;

public interface IBitCask {
    int open(String directory) throws FileNotFoundException;

    void put(byte[] key, byte[] value) throws IOException, DirectoryNotFoundException;

    byte[] get(byte[] key) throws IOException, DirectoryNotFoundException;

    void delete(byte[] key) throws IOException, DirectoryNotFoundException, KeyNotFoundException;

    void mergeAndCompaction() throws IOException;
}
