package bitCask.handler;

import bitCask.exception.DirectoryNotFoundException;
import bitCask.storage.BitCask;
import com.google.common.primitives.Ints;

import java.io.IOException;

public record SetHandler(byte[] key, byte[] value) implements MessageHandler {
    @Override
    public byte[] execute(BitCask bitCask) {
        try {
            bitCask.put(key, value);
            return Ints.toByteArray(200);
        } catch (IOException | DirectoryNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
