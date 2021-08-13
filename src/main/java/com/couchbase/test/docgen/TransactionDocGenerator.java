package com.couchbase.test.docgen;

import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;

import com.couchbase.test.key.RandomKey;
import com.couchbase.test.key.RandomSizeKey;
import com.couchbase.test.key.ReverseKey;
import com.couchbase.test.key.SimpleKey;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
import com.couchbase.test.val.SimpleValue;

abstract class TransactionKVGenerator{
    public TransactionalWorkLoadSettings ws;
    String padding = "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            + "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000";
    protected Object keys;
    protected Object vals;
    private String keyClass;
    private String valClass;
    private Class<?> keyInstance;
    private Class<?> valInstance;
    protected Method keyMethod;
    protected Method valMethod;

    public TransactionKVGenerator(TransactionalWorkLoadSettings ws, String keyClass, String valClass) throws ClassNotFoundException {
        super();
        this.ws = ws;
        if(keyClass.equals(RandomKey.class.getSimpleName()))
            this.keyInstance = RandomKey.class;
        else if(keyClass == ReverseKey.class.getName())
            this.keyInstance = ReverseKey.class;
        else if(keyClass == RandomSizeKey.class.getName())
            this.keyInstance = RandomSizeKey.class;
        else
            this.keyInstance = SimpleKey.class;

        this.valInstance = SimpleValue.class;
        try {
            this.keys = keyInstance.getConstructor(WorkLoadSettings.class).newInstance(ws);
            this.vals = valInstance.getConstructor(WorkLoadSettings.class).newInstance(ws);
            this.keyMethod = this.keyInstance.getDeclaredMethod("next", int.class);
            this.valMethod = this.valInstance.getDeclaredMethod("next", String.class);
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        }
    }

    public boolean has_next() {
        return this.ws.itr.get() < this.ws.end;
    }

    abstract Tuple2<String, Object> next();
}

public class TransactionDocGenerator extends TransactionKVGenerator {
    public TransactionDocGenerator(
            TransactionalWorkLoadSettings ws,
            String keyClass, String valClass) throws ClassNotFoundException {
        super(ws, keyClass, valClass);
    }

    public Tuple2<String, Object> next() {
        int temp = this.ws.itr.incrementAndGet();
        String k = null;
        Object v = null;
            try {
                k = (String) this.keyMethod.invoke(this.keys, temp);
                v = (Object) this.valMethod.invoke(this.vals, k);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e1) {
                e1.printStackTrace();
            }
        return Tuples.of(k, v);
    }

}
