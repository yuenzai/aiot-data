package cn.ecosync.aiot.data.apiserver.command;

/**
 * @author yan
 * @since 2024
 */
public interface CommandHandler<T extends Command> {
    void handle(T command);
}
