package cc.whohow.redis.distributed;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class RedisDistributedNode {
    private String uuid;
    private long uptime;
    private String ip;

    public RedisDistributedNode() {
    }

    public RedisDistributedNode(String uuid) {
        this.uuid = uuid;
        this.uptime = System.currentTimeMillis();
        try {
            this.ip = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            this.ip = "0.0.0.0";
        }
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public long getUptime() {
        return uptime;
    }

    public void setUptime(long uptime) {
        this.uptime = uptime;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }
}
