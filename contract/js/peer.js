#!/usr/bin/env node
/**
 * The vcmp-js contract peer: a server or a client built on the real `@variocube/vcmp` packages,
 * driven by the Rust contract tests (`tests/contract_js.rs`).
 *
 *   node peer.js server <port> [heartbeatIntervalMs]
 *   node peer.js server-auth <port> [heartbeatIntervalMs]       every path requires AUT (auth timeout 1.5 s)
 *   node peer.js client <url> [--hello]                          an old client, no AUT
 *   node peer.js client-auth <url> <credentials-json> [--hello]  authenticates with AUT
 *
 * The server roles listen on `port`, `0` picks a free one; either way they print it as `LISTENING <port>`.
 *
 * Contract credential scheme (vcmp-rs#16, shared with the Java peer and tests/contract/mod.rs): the AUT credential is
 * `{token: "<principal>[:<n>]", ttl?: <ms>}`; the principal is the token up to `:`; `bad` (or none) is rejected with
 * 401; `ttl` sets the expiry (≤ 0: already over); the ACK carries `user`. The handshake credential (transition
 * mode) is the query parameter `token`. `credentials-json` is a JSON array the provider returns in turn (the last
 * one repeated). `--hello` sends a `contract:Echo` once connected (HELLO_OK / HELLO_FAILED).
 *
 * Both roles register the same handlers:
 *
 *   contract:Echo   {payload}                 → ACK with `payload`
 *   contract:Void                             → ACK without payload
 *   contract:Fail   {status, title, detail}   → NAK with that problem detail
 *   contract:Never                            → never acknowledges
 *   contract:Run    {scenario, ...}           → performs a send *to the sender* and ACKs with the
 *                                               outcome: {ok: true, result} or {ok: false, error}
 *
 * Lifecycle events are printed to stdout, one per line: LISTENING <port> (server roles, before READY), READY,
 * CONNECTED, DISCONNECTED, AUTH_CONNECTED <principal>, AUTH_DISCONNECTED, AUTH_ERROR <status>, HELLO_OK, HELLO_FAILED,
 * and for the stdin command `REAUTH` (the client roles' application calls `client.reauthenticate()`): REAUTH_OK,
 * REAUTH_NONE or REAUTH_ERROR <status>.
 */
const {VcmpClient, VcmpError} = require("@variocube/vcmp");
const {VcmpServer} = require("@variocube/vcmp-server");
const {WebSocketServer} = require("ws");
const WebSocket = require("ws");

const debug = process.env.VCMP_DEBUG ? console : undefined;

function register(on) {
	on("contract:Echo", message => message.payload);
	on("contract:Void", () => undefined);
	on("contract:Fail", message => {
		throw new VcmpError({title: message.title, status: message.status, detail: message.detail});
	});
	on("contract:Never", () => new Promise(() => {}));
	on("contract:Run", run);
}

async function run(message, session) {
	try {
		switch (message.scenario) {
			case "echo":
				return {ok: true, result: await session.send({"@type": "contract:Echo", payload: message.payload})};
			case "void":
				return {ok: true, result: (await session.send({"@type": "contract:Void"})) ?? null};
			case "fail":
				await session.send({
					"@type": "contract:Fail",
					status: message.status,
					title: message.title,
					detail: message.detail,
				});
				return {ok: true};
			case "unknown":
				await session.send({"@type": "contract:Unknown"});
				return {ok: true};
			case "big": {
				const result = await session.send({"@type": "contract:Echo", payload: "x".repeat(message.size)});
				return {ok: true, result: result.length};
			}
			case "concurrent": {
				const sends = Array.from(
					{length: message.count},
					(_, i) => session.send({"@type": "contract:Echo", payload: String(i)}),
				);
				const results = await Promise.all(sends);
				return {ok: true, result: results.filter((result, i) => result === String(i)).length};
			}
			case "malformed":
				return await malformed(session);
			default:
				return {ok: false, error: {title: "Unknown scenario", status: 400, detail: message.scenario}};
		}
	}
	catch (error) {
		return {ok: false, error: {title: error.title, status: error.status, detail: error.detail}};
	}
}

/**
 * Sends a MSG frame with an unparsable payload straight on the socket and captures the NAK.
 * (The session ignores NAKs for ids it does not know, so the raw socket is observed directly.)
 */
function malformed(session) {
	const webSocket = session.webSocket;
	const id = "malformed000";
	return new Promise(resolve => {
		const original = webSocket.onmessage;
		const timer = setTimeout(() => {
			webSocket.onmessage = original;
			resolve({ok: false, error: {title: "Timeout", status: 0, detail: "no NAK received"}});
		}, 5000);
		webSocket.onmessage = event => {
			const data = String(event.data);
			if (data.startsWith("NAK" + id)) {
				clearTimeout(timer);
				webSocket.onmessage = original;
				resolve({ok: false, error: JSON.parse(data.slice(15))});
			}
			else {
				original(event);
			}
		};
		webSocket.send("MSG" + id + "{oops");
	});
}

function server(port, heartbeatInterval) {
	const webSocketServer = new WebSocketServer({port});
	const server = new VcmpServer({webSocketServer, heartbeatInterval, debug});
	register((type, handler) => server.on(type, handler));
	server.onSessionConnected = () => console.log("CONNECTED");
	server.onSessionDisconnected = () => console.log("DISCONNECTED");
	webSocketServer.on("listening", () => listening(webSocketServer));
}

function listening(webSocketServer) {
	console.log("LISTENING " + webSocketServer.address().port);
	console.log("READY");
}

function decide(token, ttl) {
	if (!token || token === "bad") {
		throw new VcmpError({title: "Unauthorized", status: 401});
	}
	const principal = token.split(":")[0];
	return {
		principal,
		expiresAt: ttl === undefined || ttl === null ? undefined : new Date(Date.now() + ttl),
		ack: {user: principal},
	};
}

function serverAuth(port, heartbeatInterval) {
	const webSocketServer = new WebSocketServer({port});
	const server = new VcmpServer({
		webSocketServer,
		heartbeatInterval,
		debug,
		authentication: {
			authenticate: credential => decide(credential.token, credential.ttl),
			authenticateHandshake: request => {
				const token = new URL(request.url, "http://peer").searchParams.get("token");
				return token === null ? undefined : decide(token);
			},
			timeout: 1500,
		},
	});
	register((type, handler) => server.on(type, handler));
	server.onSessionConnected = session => console.log("AUTH_CONNECTED " + session.principal);
	server.onSessionDisconnected = () => console.log("AUTH_DISCONNECTED");
	webSocketServer.on("listening", () => listening(webSocketServer));
}

function client(url, {credentials, hello} = {}) {
	let calls = 0;
	const client = new VcmpClient(url, {
		customWebSocket: WebSocket,
		autoStart: false,
		reconnectTimeout: 500,
		debug,
		...(credentials
			? {
				authenticate: async () => credentials[Math.min(calls++, credentials.length - 1)],
				// The minimum vcmp-js accepts, so timeout scenarios finish quickly (default 10 s).
				authenticationTimeout: 1000,
			}
			: {}),
	});
	register((type, handler) => client.on(type, handler));
	client.onOpen = () => {
		console.log("CONNECTED");
		if (hello) {
			client.send({"@type": "contract:Echo", payload: "hello"}).then(
				() => console.log("HELLO_OK"),
				() => console.log("HELLO_FAILED"),
			);
		}
	};
	client.onClose = () => console.log("DISCONNECTED");
	client.onAuthenticationError = error => console.log("AUTH_ERROR " + error.status);
	// The test drives the application: on `REAUTH` it re-authenticates (the client schedules nothing itself).
	require("readline").createInterface({input: process.stdin}).on("line", line => {
		if (line.trim() === "REAUTH") {
			client.reauthenticate().then(
				ack => console.log(ack === undefined ? "REAUTH_NONE" : "REAUTH_OK"),
				error => console.log("REAUTH_ERROR " + error.status),
			);
		}
	});
	client.start();
	console.log("READY");
}

const [role, target, interval] = process.argv.slice(2);
const hello = process.argv.includes("--hello");
if (role === "server") {
	server(Number.parseInt(target), interval ? Number.parseInt(interval) : 20000);
}
else if (role === "server-auth") {
	serverAuth(Number.parseInt(target), interval ? Number.parseInt(interval) : 20000);
}
else if (role === "client") {
	client(target, {hello});
}
else if (role === "client-auth") {
	client(target, {credentials: JSON.parse(interval), hello});
}
else {
	console.error("usage: peer.js server|server-auth <port> [heartbeatIntervalMs] | client <url> [--hello]"
		+ " | client-auth <url> <credentials-json> [--hello]");
	process.exit(2);
}
