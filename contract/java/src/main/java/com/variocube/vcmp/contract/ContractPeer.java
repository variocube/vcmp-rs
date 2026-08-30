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
 *   ContractPeer client &lt;url&gt;                          connect BasicVcmpClient to url
 * </pre>
 *
 * Prints one lifecycle event per line to stdout: READY, CONNECTED, DISCONNECTED.
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
			case "client" -> client(args[1]);
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

	private static void client(String url) throws Exception {
		var target = new ClientPeer();
		var manager = new VcmpConnectionManager(target, url);
		// Fast reconnects so the harness' restart scenario finishes quickly (defaults are 20-40s).
		manager.setDisconnectTimeout(Duration.ofMillis(200));
		manager.setReconnectTimeoutMin(Duration.ofMillis(200));
		manager.setReconnectTimeoutMax(Duration.ofMillis(400));
		manager.start();
		System.out.println("READY");
		// Keep the process alive until killed by the harness.
		Thread.currentThread().join();
	}

	/** The server endpoint bean. */
	@Bean
	ServerPeer serverPeer() {
		return new ServerPeer();
	}

	@EventListener(ApplicationReadyEvent.class)
	public void onReady() {
		System.out.println("READY");
	}
}
