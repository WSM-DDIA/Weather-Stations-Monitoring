package bitCask.storage;

import bitCask.exception.DirectoryNotFoundException;

import java.io.FileNotFoundException;
import java.io.IOException;

public interface IBitCask {
    /**
     * Opens the database within the specified directory and recovers the data from the disk.
     *
     * @param directory directory of the database
     * @return status of the operation
     * @throws FileNotFoundException if the directory is not found
     */
    int open(String directory) throws FileNotFoundException;

    /**
     * Puts the key value pair in the database.
     *
     * @param key   key to be inserted
     * @param value value to be inserted
     * @throws IOException                if there is an error in writing to the disk
     * @throws DirectoryNotFoundException if the directory is not found
     */
    void put(byte[] key, byte[] value) throws IOException, DirectoryNotFoundException;

    /**
     * Gets the value of the key from the database.
     *
     * @param key key to be searched
     * @return value of the key
     * @throws IOException                if there is an error in reading from the disk
     * @throws DirectoryNotFoundException if the directory is not found
     */
    byte[] get(byte[] key) throws IOException, DirectoryNotFoundException;

    /**
     * Deletes the key from the database.
     *
     * @param key key to be deleted
     * @throws IOException                if there is an error in writing to the disk
     * @throws DirectoryNotFoundException if the directory is not found
     */
    void delete(byte[] key) throws IOException, DirectoryNotFoundException;

    /**
     * Merges the data files and compacts them to a single file.
     *
     * @throws IOException if there is an error in reading the database
     */
    void mergeAndCompaction() throws IOException;
}
