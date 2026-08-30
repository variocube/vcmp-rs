#!/usr/bin/env node
/**
 * The vcmp-js contract peer: a server or a client built on the real `@variocube/vcmp` packages,
 * driven by the Rust contract tests (`tests/contract_js.rs`).
 *
 *   node peer.js server <port> [heartbeatIntervalMs]
 *   node peer.js client <url>
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
 * Lifecycle events are printed to stdout, one per line: READY, CONNECTED, DISCONNECTED.
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
	webSocketServer.on("listening", () => console.log("READY"));
}

function client(url) {
	const client = new VcmpClient(url, {
		customWebSocket: WebSocket,
		autoStart: false,
		reconnectTimeout: 500,
		debug,
	});
	register((type, handler) => client.on(type, handler));
	client.onOpen = () => console.log("CONNECTED");
	client.onClose = () => console.log("DISCONNECTED");
	client.start();
	console.log("READY");
}

const [role, target, interval] = process.argv.slice(2);
if (role === "server") {
	server(Number.parseInt(target), interval ? Number.parseInt(interval) : 20000);
}
else if (role === "client") {
	client(target);
}
else {
	console.error("usage: peer.js server <port> [heartbeatIntervalMs] | client <url>");
	process.exit(2);
}
