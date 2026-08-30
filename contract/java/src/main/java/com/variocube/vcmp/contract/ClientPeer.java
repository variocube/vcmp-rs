package com.variocube.vcmp.contract;

import com.variocube.vcmp.VcmpSession;
import com.variocube.vcmp.VcmpSessionConnected;
import com.variocube.vcmp.VcmpSessionDisconnected;
import lombok.extern.slf4j.Slf4j;

/**
 * The client-side contract peer: the same handlers, driven by a {@code VcmpConnectionManager}. The
 * server initiates the heartbeat, so this side only echoes it — it must not initiate one.
 */
@Slf4j
public class ClientPeer extends ContractHandlers {

	@VcmpSessionConnected
	public void onConnected(VcmpSession session) {
		log.info("Connected: {}", session.getId());
		System.out.println("CONNECTED");
	}

	@VcmpSessionDisconnected
	public void onDisconnected(VcmpSession session) {
		log.info("Disconnected");
		System.out.println("DISCONNECTED");
	}
}
