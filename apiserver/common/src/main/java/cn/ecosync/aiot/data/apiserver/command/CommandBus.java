package cn.ecosync.aiot.data.apiserver.command;

import org.springframework.core.ResolvableType;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author yan
 * @since 2024
 */
public class CommandBus {
    private final List<CommandHandler<?>> commandHandlers;
    private final List<ResolvableType> commandHandlerGenerics;
    private final Map<Class<?>, CommandHandler<?>> commandHandlerMap;

    public CommandBus(List<CommandHandler<?>> commandHandlers) {
        this.commandHandlers = commandHandlers.stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        this.commandHandlerGenerics = this.commandHandlers.stream()
                .map(this::getCommandType)
                .collect(Collectors.toList());
        this.commandHandlerMap = new HashMap<>();
    }

    public void execute(Command command) {
        Assert.notNull(command, "command is null");
        CommandHandler<?> commandHandler = getCommandHandler(command);
        Assert.notNull(commandHandler, "command handler not found: " + command.getClass().getCanonicalName());
        Method handleMethod = ReflectionUtils.findMethod(commandHandler.getClass(), "handle", (Class<?>[]) null);
        Assert.notNull(handleMethod, "handle method not found: " + commandHandler.getClass().getCanonicalName());
        ReflectionUtils.invokeMethod(handleMethod, commandHandler, command);
    }

    private CommandHandler<?> getCommandHandler(Command command) {
        Class<? extends Command> commandClass = command.getClass();
        CommandHandler<?> commandHandler = commandHandlerMap.get(commandClass);
        if (commandHandler == null) {
            for (int i = 0; i < commandHandlerGenerics.size(); i++) {
                ResolvableType commandHandlerGeneric = commandHandlerGenerics.get(i);
                if (commandHandlerGeneric.isAssignableFrom(commandClass)) {
                    commandHandler = commandHandlers.get(i);
                    commandHandlerMap.put(commandClass, commandHandler);
                    break;
                }
            }
        }
        return commandHandler;
    }

    private ResolvableType getCommandType(CommandHandler<?> commandHandler) {
        return ResolvableType.forClass(CommandHandler.class, commandHandler.getClass())
                .getGeneric(0);
    }
}
