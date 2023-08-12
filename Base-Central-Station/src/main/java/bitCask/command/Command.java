package bitCask.command;

import bitCask.storage.BitCask;

public interface Command {

    String execute(BitCask keyValueStore);
}
