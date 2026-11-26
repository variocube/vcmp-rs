package com.variocube.vcmp.contract;

import com.variocube.vcmp.client.VcmpConnectionManager;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;

/**
 * The vcmp-spring contract peer, driven by the Rust contract tests (`tests/contract_java.rs`).
 *
 * <pre>
 *   ContractPeer server &lt;port&gt; [heartbeatIntervalMs]   run the /peer/{name} endpoint
 *   ContractPeer client &lt;url&gt; [--hello]                connect to url (an old client, no AUT)
 *   ContractPeer client-auth &lt;url&gt; &lt;credentials-json&gt; [--hello]   connect and authenticate with AUT
 * </pre>
 *
 * The server listens on {@code port}, {@code 0} picks a free one; either way it prints it as LISTENING &lt;port&gt;.
 * It runs {@code /peer/{name}} (no authentication) and {@code /auth/{name}} ({@link AuthServerPeer}).
 * {@code credentials-json} is a JSON array of credentials the provider returns in turn (the last one repeated).
 * {@code --hello} sends a {@code contract:Echo} once connected.
 * <p>
 * Prints one lifecycle event per line to stdout: LISTENING &lt;port&gt; (server, before READY), READY, CONNECTED,
 * DISCONNECTED, AUTH_CONNECTED &lt;principal&gt;, AUTH_DISCONNECTED, AUTH_ERROR &lt;status&gt;, HELLO_OK,
 * HELLO_FAILED, and for the stdin command {@code REAUTH} (the client roles' application calls
 * {@code reauthenticate()}): REAUTH_OK, REAUTH_NONE, REAUTH_ERROR &lt;status&gt;.
 */
@Slf4j
@SpringBootApplication
public class ContractPeer {

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("usage: ContractPeer server <port> [heartbeatMs] | client <url>");
			System.exit(2);
		}
		switch (args[0]) {
			case "server" -> server(args);
			case "client" -> client(new ClientPeer(hasHello(args)), args[1]);
			case "client-auth" -> client(new AuthClientPeer(credentials(args[2]), hasHello(args)), args[1]);
			default -> {
				System.err.println("unknown role: " + args[0]);
				System.exit(2);
			}
		}
	}

	private static void server(String[] args) {
		System.setProperty("server.port", args[1]);
		if (args.length > 2) {
			System.setProperty("vcmp.heartbeat", args[2]);
		}
		// Quiet the banner; the harness parses stdout line by line.
		var application = new SpringApplication(ContractPeer.class);
		application.setLogStartupInfo(false);
		application.run(args);
	}

	private static void printReauthentication(java.util.Map<String, Object> ack, Throwable error) {
		if (error == null) {
			System.out.println(ack == null ? "REAUTH_NONE" : "REAUTH_OK");
			return;
		}
		var cause = error instanceof java.util.concurrent.CompletionException && error.getCause() != null
				? error.getCause()
				: error;
		var status = cause instanceof org.springframework.web.ErrorResponseException failure
				? failure.getBody().getStatus()
				: 0;
		System.out.println("REAUTH_ERROR " + status);
	}

	private static boolean hasHello(String[] args) {
		return java.util.Arrays.asList(args).contains("--hello");
	}

	private static java.util.List<java.util.Map<String, Object>> credentials(String json) throws Exception {
		return new com.fasterxml.jackson.databind.ObjectMapper().readValue(json,
				new com.fasterxml.jackson.core.type.TypeReference<>() {
				});
	}

	private static void client(Object target, String url) throws Exception {
		var manager = new VcmpConnectionManager(target, url);
		// The minimum, so timeout scenarios finish quickly (default 10 s).
		manager.setAuthTimeout(Duration.ofSeconds(1));
		// Fast reconnects so the harness' restart scenario finishes quickly (defaults are 20-40s).
		manager.setDisconnectTimeout(Duration.ofMillis(200));
		manager.setReconnectTimeoutMin(Duration.ofMillis(200));
		manager.setReconnectTimeoutMax(Duration.ofMillis(400));
		manager.start();
		System.out.println("READY");
		// The test drives the application: on `REAUTH` it re-authenticates (the client schedules nothing itself).
		// Keep the process alive until killed by the harness.
		var commands = new java.io.BufferedReader(new java.io.InputStreamReader(System.in));
		for (String line; (line = commands.readLine()) != null; ) {
			if (line.trim().equals("REAUTH")) {
				manager.reauthenticate().whenComplete(ContractPeer::printReauthentication);
			}
		}
		Thread.currentThread().join();
	}

	/** The server endpoint bean. */
	@Bean
	ServerPeer serverPeer() {
		return new ServerPeer();
	}

	/** The authenticated endpoint bean. */
	@Bean
	AuthServerPeer authServerPeer() {
		return new AuthServerPeer();
	}

	@EventListener(ApplicationReadyEvent.class)
	public void onReady(ApplicationReadyEvent event) {
		// The bound port, also when started with port 0.
		var port = event.getApplicationContext().getEnvironment().getProperty("local.server.port");
		System.out.println("LISTENING " + port);
		System.out.println("READY");
	}
}
