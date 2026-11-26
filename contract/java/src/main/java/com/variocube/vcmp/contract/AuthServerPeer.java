package com.variocube.vcmp.contract;

import com.variocube.vcmp.VcmpAuthentication;
import com.variocube.vcmp.VcmpAuthenticator;
import com.variocube.vcmp.VcmpHandshake;
import com.variocube.vcmp.VcmpHandshakeAuthenticator;
import com.variocube.vcmp.VcmpSession;
import com.variocube.vcmp.VcmpSessionConnected;
import com.variocube.vcmp.VcmpSessionDisconnected;
import com.variocube.vcmp.server.VcmpEndpoint;
import java.security.Principal;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

/**
 * The authenticated contract endpoint ({@code AUT}, vcmp-rs#16) at {@code /auth/{name}}, with the contract credential
 * scheme shared by all peers (see {@code tests/contract/mod.rs}):
 *
 * <ul>
 *   <li>{@code AUT} credential {@code {"token": "<principal>[:<n>]", "ttl": <ms>?}}: the principal is the token up to
 *   {@code :}; {@code bad} (or none) is rejected with 401; {@code ttl} sets the expiry (≤ 0: already over); the ACK
 *   carries {@code user}.</li>
 *   <li>Handshake credential (transition mode): query parameter {@code token}; {@code bad} closes the session.</li>
 * </ul>
 *
 * The auth timeout is 1.5 s, so the "no AUT within the timeout" case is quick. Prints
 * {@code AUTH_CONNECTED <principal>} and {@code AUTH_DISCONNECTED}.
 */
@Slf4j
@VcmpEndpoint(path = "/auth/{name}", authenticated = true, authTimeoutMillis = 1500)
public class AuthServerPeer extends ContractHandlers {

	public record Credential(String token, Long ttl) {
	}

	public record User(String name) implements Principal {
		@Override
		public String getName() {
			return name;
		}
	}

	@Value("${vcmp.heartbeat:20000}")
	private int heartbeatInterval;

	@VcmpAuthenticator
	public VcmpAuthentication authenticate(Credential credential) {
		return decide(credential.token(), credential.ttl());
	}

	@VcmpHandshakeAuthenticator
	public VcmpAuthentication authenticateHandshake(VcmpHandshake handshake) {
		var query = handshake.uri().getRawQuery();
		if (query == null) {
			return null;
		}
		var token = Arrays.stream(query.split("&"))
				.filter(parameter -> parameter.startsWith("token="))
				.map(parameter -> parameter.substring("token=".length()))
				.findFirst();
		if (token.isEmpty()) {
			return null;
		}
		var authentication = decide(token.get(), null);
		if (authentication == null) {
			throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
		}
		return authentication;
	}

	static VcmpAuthentication decide(String token, Long ttl) {
		if (token == null || token.isEmpty() || token.equals("bad")) {
			return null;
		}
		var user = new User(token.split(":", 2)[0]);
		var authentication = ttl == null
				? VcmpAuthentication.of(user)
				: VcmpAuthentication.of(user, Instant.now().plusMillis(ttl));
		return authentication.withDetails(Map.of("user", user.name()));
	}

	@VcmpSessionConnected
	public void onConnected(VcmpSession session) {
		session.initiateHeartbeat(heartbeatInterval);
		System.out.println("AUTH_CONNECTED " + session.getPrincipal().getName());
	}

	@VcmpSessionDisconnected
	public void onDisconnected(VcmpSession session) {
		System.out.println("AUTH_DISCONNECTED");
	}
}
