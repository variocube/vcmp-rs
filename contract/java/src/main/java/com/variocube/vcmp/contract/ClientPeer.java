package com.variocube.vcmp.contract;

import com.variocube.vcmp.VcmpSession;
import com.variocube.vcmp.VcmpSessionConnected;
import com.variocube.vcmp.VcmpSessionDisconnected;
import com.variocube.vcmp.contract.Messages.Echo;
import lombok.extern.slf4j.Slf4j;

/**
 * The client-side contract peer: the same handlers, driven by a {@code VcmpConnectionManager}. The
 * server initiates the heartbeat, so this side only echoes it — it must not initiate one.
 * <p>
 * With {@code hello}, it sends a {@code contract:Echo} as soon as it is connected, like an old client that sends right
 * away; the outcome is printed as {@code HELLO_OK} or {@code HELLO_FAILED}.
 */
@Slf4j
public class ClientPeer extends ContractHandlers {

	private final boolean hello;

	public ClientPeer() {
		this(false);
	}

	public ClientPeer(boolean hello) {
		this.hello = hello;
	}

	@VcmpSessionConnected
	public void onConnected(VcmpSession session) {
		log.info("Connected: {}", session.getId());
		System.out.println("CONNECTED");
		if (hello) {
			session.send(new Echo("hello"), String.class).toCompletableFuture().whenComplete((result, error) ->
					System.out.println(error == null ? "HELLO_OK" : "HELLO_FAILED"));
		}
	}

	@VcmpSessionDisconnected
	public void onDisconnected(VcmpSession session) {
		log.info("Disconnected");
		System.out.println("DISCONNECTED");
	}
}
