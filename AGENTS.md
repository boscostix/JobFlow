# JobFlow agent workflow

## Repository context

JobFlow is a distributed job execution engine planned in Go and PostgreSQL. Correctness-critical areas include transactional job claiming, worker concurrency, leases, fencing tokens, retry scheduling, idempotency, and crash recovery.

Read [README.md](README.md) for scope, architecture, roadmap, and MVP acceptance criteria, and [CLAUDE.md](CLAUDE.md) for detailed implementation guidance. This file defines the development and review workflow.

Milestone 1 now has Go entry points under `cmd/api` and `cmd/worker`, backend packages under `internal/`, schema changes under `migrations/`, sqlc source queries under `queries/`, and real PostgreSQL tests under `tests/integration/`. Docker Compose provides local PostgreSQL, explicit migrations, API, and one worker. The Python client, dashboard, recovery/reliability harness, load tests, and CI remain planned.

## Agent roles

Use one primary implementation agent and, when justified, one independent review subagent.

- **Planner:** The primary agent briefly identifies scope, acceptance criteria, and important failure scenarios before substantial changes.
- **Implementer:** The primary agent writes production code and appropriate tests.
- **Reviewer:** For nontrivial changes, delegate one independent, read-only review focused on correctness and regressions. The reviewer must not modify files.

Do not create additional review agents unless the task involves an especially complex concurrency or architectural change. Any additional reviewers remain subject to the same two-pass limit. Do not spawn agents recursively: review subagents must not delegate further.

Trivial documentation or similarly low-impact changes do not require a review subagent. Use judgment based on correctness and regression risk.

## Development process

For every task:

1. Understand the requested behavior and inspect relevant existing code and guidance.
2. Define clear acceptance criteria and identify relevant failure scenarios.
3. Implement the smallest coherent solution.
4. Add or update meaningful tests appropriate to the change.
5. Run relevant automated verification.
6. Perform one independent review when warranted.
7. Fix confirmed, in-scope defects.
8. Rerun affected tests and finish with a concise completion report.

Avoid speculative abstractions and unrelated changes. For documentation-only work, check accuracy, links, and formatting; do not create application tests solely to test prose.

## Review rules

Prioritize findings as follows:

| Severity | Meaning | Required response |
| --- | --- | --- |
| P0 | Data loss, corruption, critical security problems, or broken correctness guarantees | Address before completion |
| P1 | Reproducible bugs, race conditions, broken recovery, or serious reliability issues | Address before completion |
| P2 | Meaningful edge cases or missing tests that could cause realistic failures | Address when directly related to the task and reasonably testable |
| P3 | Style, naming, minor cleanup, speculative optimization, or subjective design preferences | Do not modify code solely to satisfy the comment |

Every finding must include:

- File and location.
- Concrete failure scenario.
- Severity.
- Evidence or reproduction steps.
- Minimal recommended fix.

Do not treat speculation as a confirmed defect. Do not reopen dismissed suggestions without new evidence. Do not debate subjective style choices between agents.

## Strict iteration limits

Allow a maximum of **two review passes per task**:

1. **Pass 1:** Independent review of the implementation.
2. **Fix pass:** The primary agent implements confirmed, in-scope fixes. This is an implementation step, not an additional review pass.
3. **Pass 2:** Verify that the fixes work and did not introduce regressions. Reuse the reviewer when available; keep this pass focused on the fixes and affected behavior.

Pass 2 is only needed when there are fixes to verify. Do not initiate a third autonomous review pass, create new reviewers to reset the limit, or use recursive delegation to extend review.

If unresolved P0/P1 defects remain after the second pass, stop, clearly report the blocker and evidence, and request human direction. Do not falsely mark the task complete.

## JobFlow correctness requirements

When changing distributed execution, verify the relevant invariants:

- Accepted jobs are durably persisted before successful submission responses.
- Two workers cannot successfully claim the same eligible job simultaneously.
- Expired leases permit recovery.
- Stale attempts cannot overwrite newer attempts or their committed results.
- Retry attempts obey configured limits and backoff.
- Terminal jobs cannot transition into invalid states; intentional operations such as manual retry must use explicitly defined, valid transitions.
- Worker crashes do not permanently strand recoverable jobs.
- External task side effects are not falsely described as exactly-once. JobFlow provides at-least-once execution; handlers must handle duplicate external effects safely.
- Database state transitions are atomic where correctness requires it.

Prefer integration tests using real PostgreSQL transactions for distributed coordination logic. Mock-only tests cannot establish database locking, transactional claiming, or fencing correctness.

## Testing and available commands

The Go module, Makefile, Docker Compose, migrations, and integration harness are established. See the README for setup. `make generate` regenerates checked-in sqlc code. Do not present pending frontend, recovery, load, or CI commands as working checks.

Use the checks relevant to changed code once the corresponding infrastructure exists:

| Check | Current status |
| --- | --- |
| Go formatting and compilation | `make fmt` and `make build` |
| Go unit tests | `make test` |
| Go race detector for concurrency-sensitive changes | `make race` |
| PostgreSQL integration tests | `TEST_DATABASE_URL=... make integration`; isolated schemas, schema-creation permission required |
| Targeted worker crash and lease-expiration tests | Pending worker/recovery implementation and fault-injection harness |
| Frontend lint, type checking, and tests | Pending `web/` and configured package scripts |
| Load tests and benchmarks | Pending application and k6 harness setup |
| Broader automated regression checks | Run unit/race and PostgreSQL integration suites; CI pending |

For documentation changes, available Git checks include `git diff --check` for whitespace errors and `git diff -- <file>` to inspect tracked edits. Newly created files must also be inspected directly because unstaged, untracked files do not appear in a normal Git diff.

As infrastructure is introduced, document actual setup and verification commands in the README and update the status here. Run broader regression tests before completing substantial milestones. Never claim tests passed unless they actually ran successfully. If tests cannot run, report why and distinguish unverified behavior from verified results.

## Avoid unnecessary engineering

Do not:

- Rewrite working components without a concrete need.
- Introduce infrastructure for speculative future requirements.
- Add dependencies without justification.
- Perform unrelated refactoring.
- Generate excessive tests for trivial implementation details.
- Optimize performance without evidence of a bottleneck.
- Expand feature scope during bug fixing.

Prefer clear, maintainable, idiomatic Go over elaborate abstractions. Keep work within the MVP scope described in the README unless the user changes that scope.

## Completion report

At the end of each task, briefly summarize:

1. What was implemented.
2. Tests executed and their results, or why applicable tests could not run.
3. Significant review findings and their resolution; state when independent review was not warranted.
4. Remaining known risks or blockers.
5. Whether all acceptance criteria were met.

The objective is high-confidence, production-minded engineering without unnecessary agent iterations or review churn.
