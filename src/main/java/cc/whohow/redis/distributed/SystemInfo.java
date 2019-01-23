package cc.whohow.redis.distributed;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class SystemInfo implements Supplier<Map<String, String>> {
    @Override
    public Map<String, String> get() {
        Map<String, String> system = new TreeMap<>();
        Properties systemProperties = System.getProperties();
        for (String name : systemProperties.stringPropertyNames()) {
            system.put(name, systemProperties.getProperty(name));
        }
        system.putAll(getEnv());
        system.putAll(getNetwork());
        return system;
    }

    protected Map<String, String> getEnv() {
        StringBuilder env = new StringBuilder();
        for (Map.Entry<String, String> e : System.getenv().entrySet()) {
            env.append(e.getKey()).append("=").append(e.getValue()).append("\n");
        }
        return Collections.singletonMap("system.env", env.toString());
    }

    protected Map<String, String> getNetwork() {
        try {
            StringBuilder info = new StringBuilder();
            for (NetworkInterface networkInterface : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                List<InetAddress> inetAddresses = Collections.list(networkInterface.getInetAddresses()).stream()
                        .filter(this::isUsefulInetAddress)
                        .collect(Collectors.toList());
                if (inetAddresses.isEmpty()) {
                    continue;
                }

                info.append("Name: ").append(networkInterface.getName()).append("\n");
                info.append("DisplayName: ").append(networkInterface.getDisplayName()).append("\n");
                for (InetAddress inetAddress : inetAddresses) {
                    info.append("InetAddress: ").append(inetAddress).append("\n");
                }
                info.append("\n");
            }
            return Collections.singletonMap("network", info.toString());
        } catch (Throwable e) {
            return Collections.emptyMap();
        }
    }

    protected boolean isUsefulInetAddress(InetAddress inetAddress) {
        return !inetAddress.isLoopbackAddress() &&
                !inetAddress.isLinkLocalAddress() &&
                !inetAddress.isAnyLocalAddress() &&
                !inetAddress.isMulticastAddress();
    }
}
