package com.variocube.vcmp.contract;

import com.variocube.vcmp.VcmpSession;
import com.variocube.vcmp.VcmpSessionConnected;
import com.variocube.vcmp.VcmpSessionDisconnected;
import com.variocube.vcmp.server.VcmpEndpoint;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;

/**
 * The server-side contract peer: a real {@code @VcmpEndpoint} at {@code /peer/{name}} that
 * initiates the heartbeat per session and prints the lifecycle events the Rust harness waits for.
 */
@Slf4j
@VcmpEndpoint(path = "${vcmp.path:/peer/{name}}")
public class ServerPeer extends ContractHandlers {

	@Value("${vcmp.heartbeat:20000}")
	private int heartbeatInterval;

	@VcmpSessionConnected
	public void onConnected(VcmpSession session) {
		log.info("Session connected: {}", session.getId());
		session.initiateHeartbeat(heartbeatInterval);
		System.out.println("CONNECTED");
	}

	@VcmpSessionDisconnected
	public void onDisconnected(VcmpSession session) {
		log.info("Session disconnected: {}", session.getId());
		System.out.println("DISCONNECTED");
	}
}
