package bitCask.handler;

import bitCask.exception.DirectoryNotFoundException;
import bitCask.storage.BitCask;

import java.io.IOException;

public record GetHandler(byte[] key) implements MessageHandler {
    @Override
    public byte[] execute(BitCask bitCask) {
        try {
            return bitCask.get(key);
        } catch (IOException | DirectoryNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
