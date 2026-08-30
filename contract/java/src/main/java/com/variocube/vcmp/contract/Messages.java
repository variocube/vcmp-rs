package com.variocube.vcmp.contract;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.variocube.vcmp.VcmpMessage;

/**
 * The message types of the contract test suite. Their {@code @type} ids (the {@link JsonTypeName})
 * must match the Rust {@code contract} module and {@code contract/js/peer.js} exactly.
 */
final class Messages {
	private Messages() {
	}

	@JsonTypeName("contract:Echo")
	record Echo(String payload) implements VcmpMessage {
	}

	@JsonTypeName("contract:Void")
	record Void() implements VcmpMessage {
	}

	@JsonTypeName("contract:Fail")
	record Fail(int status, String title, String detail) implements VcmpMessage {
	}

	@JsonTypeName("contract:Never")
	record Never() implements VcmpMessage {
	}

	@JsonTypeName("contract:Unknown")
	record Unknown() implements VcmpMessage {
	}

	@JsonTypeName("contract:Run")
	record Run(String scenario, Integer status, String title, String detail, Integer size, Integer count,
			String payload) implements VcmpMessage {
	}

	@JsonTypeName("contract:Outcome")
	record Outcome(boolean ok, Object result, Problem error) implements VcmpMessage {
		static Outcome ok(Object result) {
			return new Outcome(true, result, null);
		}

		static Outcome failed(Problem error) {
			return new Outcome(false, null, error);
		}
	}

	record Problem(String title, Integer status, String detail) {
	}
}
