package cc.whohow.redis.rpc;

public interface RpcServer {
    <T> T newProxy(Class<T> stub);
}
