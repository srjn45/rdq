package code.srjn.retry.async.registry;

import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class FunctionRegistry {

    private final Map<String, SerializedConsumer<?>> funcMap = new HashMap<>();
    private final Map<String, String> idMap = new HashMap<>();

    public <T> void register(String funcId, SerializedConsumer<T> consumer) {
        funcMap.put(funcId, consumer);
        idMap.put(getLambdaName(consumer), funcId);
    }

    public <T> String getId(SerializedConsumer<T> func) {
        String funcName = getLambdaName(func);
        String id = idMap.get(funcName);
        if (id == null) {
            throw new FunctionNotRegisteredException(funcName);
        }
        return id;
    }

    public <T> void invoke(String funcId, T arg) {
        ((SerializedConsumer<T>) funcMap.get(funcId)).accept(arg);
    }

    private static String getLambdaName(Serializable lambda) {
        try {
            Method writeReplace = lambda.getClass().getDeclaredMethod("writeReplace");
            writeReplace.setAccessible(true);
            Object serializedForm = writeReplace.invoke(lambda);
            if (serializedForm instanceof SerializedLambda sl) {
                // Build a stable ID
                return sl.getImplClass().replace('/', '.') + "::" + sl.getImplMethodName();
            }
        } catch (Exception e) {
            throw new FailedToExtractLambdaNameException(e);
        }
        throw new FailedToExtractLambdaNameException("not a SerializedLambda");
    }
}
