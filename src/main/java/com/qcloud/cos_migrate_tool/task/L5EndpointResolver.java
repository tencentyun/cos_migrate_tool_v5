package com.qcloud.cos_migrate_tool.task;

import java.util.concurrent.ThreadLocalRandom;

import com.qcloud.cos.endpoint.EndpointResolver;
import com.tencent.jungle.lb2.L5API;
import com.tencent.jungle.lb2.L5API.L5QOSPacket;
import com.tencent.jungle.lb2.L5APIException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class L5EndpointResolver implements EndpointResolver {
    private static final Logger log = LoggerFactory.getLogger(IDCL5EndpointResolver.class);
    private int modId;
    private int cmdId;

    public L5EndpointResolver(int modId, int cmdId) {
        super();
        this.modId = modId;
        this.cmdId = cmdId;
    }

    @Override
    public String resolveGeneralApiEndpoint(String endpoint) {
        float timeout = 0.2F;
        String cgiIpAddr = null;
        L5QOSPacket packet = new L5QOSPacket();
        packet.modid = this.modId;
        packet.cmdid = this.cmdId;

        for (int i = 0; i < 5; ++i) {
            try {
                packet = L5API.getRoute(packet, timeout);
                if (!packet.ip.isEmpty() && packet.port > 0) {
                    cgiIpAddr = String.format("%s:%d", packet.ip, packet.port);
                    break;
                }
            } catch (L5APIException e) {
                log.error("Get l5 modid: {} cmdid: {} failed.", this.modId, this.cmdId, e);
                try {
                    Thread.sleep(ThreadLocalRandom.current().nextLong(10L, 1000L));
                } catch (InterruptedException var) {
                }
            }
        }
        log.info("resolve endpoirt:{}", cgiIpAddr);
        return cgiIpAddr;
    }

    @Override
    public String resolveGetServiceApiEndpoint(String arg0) {
        return "service.cos.myqcloud.com";
    }
}
