package com.variocube.vcmp.contract;

import com.variocube.vcmp.VcmpCallback;
import com.variocube.vcmp.VcmpListener;
import com.variocube.vcmp.VcmpSession;
import com.variocube.vcmp.contract.Messages.Echo;
import com.variocube.vcmp.contract.Messages.Fail;
import com.variocube.vcmp.contract.Messages.Never;
import com.variocube.vcmp.contract.Messages.Outcome;
import com.variocube.vcmp.contract.Messages.Problem;
import com.variocube.vcmp.contract.Messages.Run;
import com.variocube.vcmp.contract.Messages.Unknown;
import com.variocube.vcmp.contract.Messages.Void;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.web.ErrorResponseException;

import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

/**
 * The message handlers shared by the server endpoint and the client peer. Mirror
 * {@code contract/js/peer.js} and the Rust {@code contract} module: the same handlers on both
 * sides let the same scenarios run in both directions.
 */
@Slf4j
public abstract class ContractHandlers {

	@VcmpListener
	public String handleEcho(Echo echo) {
		return echo.payload();
	}

	@VcmpListener
	public void handleVoid(Void ignored) {
		// no result → ACK without payload
	}

	@VcmpListener
	public void handleFail(Fail fail) {
		var problem = ProblemDetail.forStatusAndDetail(HttpStatusCode.valueOf(fail.status()), fail.detail());
		problem.setTitle(fail.title());
		throw new ErrorResponseException(HttpStatusCode.valueOf(fail.status()), problem, null);
	}

	@VcmpListener
	public VcmpCallback<java.lang.Void> handleNever(Never ignored) {
		// a callback that is never completed → the sender's send stays pending
		return new VcmpCallback<>();
	}

	/**
	 * Performs a scenario against the sender and reports its outcome — the "peer sends" half of the
	 * contract. Returns a future so the several sends run without blocking a handler thread.
	 */
	@VcmpListener
	public CompletableFuture<Outcome> handleRun(Run run, VcmpSession session) {
		return switch (run.scenario()) {
			case "echo" -> session.send(new Echo(run.payload()), String.class)
					.toCompletableFuture()
					.thenApply(Outcome::ok)
					.exceptionally(this::failed);
			case "void" -> session.send(new Void())
					.toCompletableFuture()
					.thenApply(result -> Outcome.ok(null))
					.exceptionally(this::failed);
			case "fail" -> session.send(new Fail(run.status(), run.title(), run.detail()))
					.toCompletableFuture()
					.thenApply(Outcome::ok)
					.exceptionally(this::failed);
			case "unknown" -> session.send(new Unknown())
					.toCompletableFuture()
					.thenApply(Outcome::ok)
					.exceptionally(this::failed);
			case "big" -> session.send(new Echo("x".repeat(run.size())), String.class)
					.toCompletableFuture()
					.thenApply(result -> Outcome.ok(result.length()))
					.exceptionally(this::failed);
			case "concurrent" -> concurrent(session, run.count());
			default -> CompletableFuture.completedFuture(
					Outcome.failed(new Problem("Unknown scenario", 400, run.scenario())));
		};
	}

	private CompletableFuture<Outcome> concurrent(VcmpSession session, int count) {
		var futures = IntStream.range(0, count)
				.mapToObj(i -> session.send(new Echo(String.valueOf(i)), String.class)
						.toCompletableFuture()
						.thenApply(result -> result.equals(String.valueOf(i))))
				.toList();
		return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
				.thenApply(ignored -> Outcome.ok(futures.stream().filter(CompletableFuture::join).count()))
				.exceptionally(this::failed);
	}

	private Outcome failed(Throwable throwable) {
		var cause = throwable instanceof java.util.concurrent.CompletionException && throwable.getCause() != null
				? throwable.getCause()
				: throwable;
		if (cause instanceof ErrorResponseException error) {
			var body = error.getBody();
			return Outcome.failed(new Problem(body.getTitle(), body.getStatus(), body.getDetail()));
		}
		return Outcome.failed(new Problem(cause.getClass().getSimpleName(), 500, cause.getMessage()));
	}
}
