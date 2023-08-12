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
            e.printStackTrace();
        }
    }

    private String getHintFilePath(String fileName) {
        String path = directoryPath + DirectoryConstants.getFileTimeStamp(fileName) + DirectoryConstants.HintExtension;

        if (fileName.endsWith("m"))
            path += "m";

        return path;
    }

    public DiskResponse writeCompacted(BitCaskEntry bitCaskEntry, File file) throws IOException {
        File fileReplica = new File(getReplicaFilePath(file.getName()));

        FileOutputStream compactFileOutputStream = new FileOutputStream(file, true);
        FileOutputStream compactFileOutputStreamReplica = new FileOutputStream(fileReplica, true);

        long valuePosition = writeToTheDisk(bitCaskEntry, file, compactFileOutputStream, compactFileOutputStreamReplica);

        return new DiskResponse(file.getName(), valuePosition);
    }

    private String getReplicaFilePath(String fileName) {
        String path = directoryPath + DirectoryConstants.getFileTimeStamp(fileName) + DirectoryConstants.ReplicaExtension;

        if (fileName.endsWith("m"))
            path += "m";

        return path;
    }

    public DiskResponse writeEntryToActiveFile(BitCaskEntry bitCaskEntry) throws IOException {
        checkIfFileExceededSize();
        return writeEntry(bitCaskEntry);
    }

    private DiskResponse writeEntry(BitCaskEntry bitCaskEntry) throws IOException {
        long valuePosition = writeToTheDisk(bitCaskEntry, this.file, fileOutputStream, fileOutputStreamReplica);

        return new DiskResponse(fileName, valuePosition);
    }

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

    private void checkIfFileExceededSize() throws IOException {
        if (file.length() >= DirectoryConstants.MEMORY_LIMIT)
            createNewFile();
    }

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
