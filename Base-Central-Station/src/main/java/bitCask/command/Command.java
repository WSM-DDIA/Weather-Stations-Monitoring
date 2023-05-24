package bitCask.command;

import bitCask.storage.BitCask;

public interface Command {

    void execute(BitCask keyValueStore);
}
