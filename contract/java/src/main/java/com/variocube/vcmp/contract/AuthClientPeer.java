package com.variocube.vcmp.contract;

import com.variocube.vcmp.client.VcmpAuthenticationFailed;
import com.variocube.vcmp.client.VcmpCredential;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.http.ProblemDetail;

/**
 * A client peer that authenticates with {@code AUT}: the credential provider returns the given credentials in turn
 * (the last one repeated), so refresh sequences (same principal, other principal, rejected) are scriptable. Prints
 * {@code AUTH_ERROR <status>} for every authentication error.
 */
public class AuthClientPeer extends ClientPeer {

	private final List<Map<String, Object>> credentials;
	private final AtomicInteger calls = new AtomicInteger();

	public AuthClientPeer(List<Map<String, Object>> credentials, boolean hello) {
		super(hello);
		this.credentials = credentials;
	}

	@VcmpCredential
	public Map<String, Object> credential() {
		return credentials.get(Math.min(calls.getAndIncrement(), credentials.size() - 1));
	}

	@VcmpAuthenticationFailed
	public void onAuthenticationFailed(ProblemDetail problem) {
		System.out.println("AUTH_ERROR " + problem.getStatus());
	}
}
