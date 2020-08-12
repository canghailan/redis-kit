package cc.whohow.redis;

import java.util.Date;
import java.util.Objects;

public class Data {
    public String a;
    public int b;
    public long c;
    public Date d;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Data)) return false;
        Data data = (Data) o;
        return b == data.b &&
                c == data.c &&
                Objects.equals(a, data.a) &&
                Objects.equals(d, data.d);
    }

    @Override
    public int hashCode() {
        return Objects.hash(a, b, c, d);
    }

    @Override
    public String toString() {
        return "Data{" +
                "a='" + a + '\'' +
                ", b=" + b +
                ", c=" + c +
                ", d=" + d +
                '}';
    }
}
