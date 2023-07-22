package bitCask.handler;

import bitCask.storage.BitCask;

public interface MessageHandler {
    byte[] execute(BitCask bitCask);
}
