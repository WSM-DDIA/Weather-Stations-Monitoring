package bitCask.util;

import bitCask.storage.BitCaskEntry;
import bitCask.storage.EntryMetaData;
import com.google.common.primitives.Ints;

import java.io.*;

public class DiskWriter {
    String directoryPath, fileName, fileNameReplica;
    FileOutputStream fileOutputStream, fileOutputStreamReplica;
    File file, fileReplica;

    public DiskWriter(String directoryPath) throws FileNotFoundException {
        this.directoryPath = directoryPath;
        createNewFile();
    }

    /**
     * Writes the entry to the disk in the given hint file.
     *
     * @param fileName name of the file
     * @param entryMetaData entry meta-data of the entry
     * @param key key of the entry
     * @throws IOException if the file is not found
     */
    public void writeEntryToHintFile(String fileName, EntryMetaData entryMetaData, byte[] key) throws IOException {
        String hintFilePath = getHintFilePath(fileName);
        File hintFile = new File(hintFilePath);

        if (!hintFile.exists())
            hintFile.createNewFile();

        FileOutputStream hintFileOutputStream = new FileOutputStream(hintFile, true);
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(hintFileOutputStream);

        try {
            byte[] bytesToWrite = entryMetaData.toBytes(key, entryMetaData.getValuePosition());
            bufferedOutputStream.write(Ints.toByteArray(bytesToWrite.length));
            bufferedOutputStream.write(bytesToWrite);
            bufferedOutputStream.flush();
        } catch (IOException e) {
            e.getCause();
        }
    }

    /**
     * Gets the hint file path for the given file name
     * @param fileName name of the file
     * @return hint file path
     */
    private String getHintFilePath(String fileName) {
        String path = directoryPath + DirectoryConstants.getFileTimeStamp(fileName) + DirectoryConstants.HintExtension;

        if (fileName.endsWith("m"))
            path += "m";

        return path;
    }

    /**
     * Writes the entry to the disk in the given file which is the compacted version.
     * @param bitCaskEntry entry to be written
     * @param file file to write the entry to
     * @return DiskResponse object containing the file name and the value position
     * @throws IOException if the file is not found
     */
    public DiskResponse writeCompacted(BitCaskEntry bitCaskEntry, File file) throws IOException {
        File fileReplica = new File(getReplicaFilePath(file.getName()));

        FileOutputStream compactFileOutputStream = new FileOutputStream(file, true);
        FileOutputStream compactFileOutputStreamReplica = new FileOutputStream(fileReplica, true);

        long valuePosition = writeToTheDisk(bitCaskEntry, file, compactFileOutputStream, compactFileOutputStreamReplica);

        return new DiskResponse(file.getName(), valuePosition);
    }

    /**
     * Gets the replica file path for the given file name
     * @param fileName name of the file
     * @return replica file path
     */
    private String getReplicaFilePath(String fileName) {
        String path = directoryPath + DirectoryConstants.getFileTimeStamp(fileName) + DirectoryConstants.ReplicaExtension;

        if (fileName.endsWith("m"))
            path += "m";

        return path;
    }

    /**
     * Writes the entry to the disk in the active file.
     * @param bitCaskEntry entry to be written
     * @return DiskResponse object containing the file name and the value position
     * @throws IOException if the file is not found
     */
    public DiskResponse writeEntryToActiveFile(BitCaskEntry bitCaskEntry) throws IOException {
        checkIfFileExceededSize();
        return writeEntry(bitCaskEntry);
    }

    /**
     * Writes the entry to the disk in the specified file when the writer is constructed.
     * @param bitCaskEntry entry to be written
     * @return DiskResponse object containing the file name and the value position
     * @throws IOException if the file is not found
     */
    private DiskResponse writeEntry(BitCaskEntry bitCaskEntry) throws IOException {
        long valuePosition = writeToTheDisk(bitCaskEntry, this.file, fileOutputStream, fileOutputStreamReplica);

        return new DiskResponse(fileName, valuePosition);
    }

    /**
     * Writes the entry to the disk in the specified file.
     * @param bitCaskEntry entry to be written
     * @return DiskResponse object containing the file name and the value position
     * @throws IOException if the file is not found
     */
    private long writeToTheDisk(BitCaskEntry bitCaskEntry, File file, FileOutputStream fileOutputStream, FileOutputStream fileOutputStreamReplica) throws IOException {
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
        BufferedOutputStream bufferedOutputStreamReplica = new BufferedOutputStream(fileOutputStreamReplica);

        byte[] bytesToWrite = bitCaskEntry.toBytes();
        bufferedOutputStream.write(Ints.toByteArray(bytesToWrite.length));
        bufferedOutputStreamReplica.write(Ints.toByteArray(bytesToWrite.length));

        bufferedOutputStream.flush();
        bufferedOutputStreamReplica.flush();

        long valuePosition = file.length() + 8 + 4 + bitCaskEntry.getKeySize() + 4;
        writeEntryToHintFile(file.getName(), new EntryMetaData(bitCaskEntry.getValueSize(), valuePosition,
                bitCaskEntry.getTimestamp(), file.getName()), bitCaskEntry.getKey());

        bufferedOutputStream.write(bytesToWrite);
        bufferedOutputStreamReplica.write(bytesToWrite);

        bufferedOutputStream.flush();
        bufferedOutputStreamReplica.flush();

        return valuePosition;
    }

    /**
     * Checks if the file has exceeded the memory limit and creates a new file if it has.
     * @throws IOException if the file is not found
     */
    private void checkIfFileExceededSize() throws IOException {
        if (file.length() >= DirectoryConstants.MEMORY_LIMIT)
            createNewFile();
    }

    /**
     * Creates a new file with the current timestamp.
     * @throws FileNotFoundException if the file is not found
     */
    private void createNewFile() throws FileNotFoundException {
        long timestamp = System.currentTimeMillis();
        fileName = timestamp + DirectoryConstants.DataExtension;
        fileNameReplica = timestamp + DirectoryConstants.ReplicaExtension;

        String filePath = directoryPath + fileName;
        String replicaFilePath = directoryPath + fileNameReplica;

        fileOutputStream = new FileOutputStream(filePath, true);
        fileOutputStreamReplica = new FileOutputStream(replicaFilePath, true);
        this.file = new File(filePath);
        this.fileReplica = new File(replicaFilePath);
    }
}
