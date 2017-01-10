package org.xsl.common.model;

/**
 * Created by xiongsenlin on 15/7/27.
 */
public class Pair<T, Z> {
    private T value1;
    private Z value2;

    public Pair() {}

    public Pair(T a, Z b) {
        this.value1 = a;
        this.value2 = b;
    }

    public T getValue1() {
        return value1;
    }

    public void setValue1(T value1) {
        this.value1 = value1;
    }

    public Z getValue2() {
        return value2;
    }

    public void setValue2(Z value2) {
        this.value2 = value2;
    }
}
