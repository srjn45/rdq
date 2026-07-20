package code.srjn.retry.async.registry;

public class FunctionNotRegisteredException extends RuntimeException {
    FunctionNotRegisteredException(String funcName) {
        super(funcName);
    }
}
