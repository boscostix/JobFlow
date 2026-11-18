import { check } from 'k6';
import execution from 'k6/execution';
import http from 'k6/http';
import { Counter } from 'k6/metrics';

const accepted = new Counter('jobflow_accepted');
const jobs = Number(__ENV.BENCH_JOBS);

export const options = {
  scenarios: {
    submit: {
      executor: 'shared-iterations',
      vus: Number(__ENV.BENCH_VUS),
      iterations: jobs,
      maxDuration: '5m',
    },
  },
  thresholds: {
    checks: ['rate==1'],
    jobflow_accepted: [`count==${jobs}`],
  },
  summaryTrendStats: ['p(50)', 'p(95)', 'p(99)'],
};

export default function () {
  const sequence = execution.scenario.iterationInTest;
  const body = JSON.stringify({ type: 'add_numbers', payload: { a: sequence, b: 1 } });
  const response = http.post('http://api:8080/api/v1/jobs', body, {
    headers: { 'Content-Type': 'application/json' },
    redirects: 0,
    timeout: '10s',
  });
  let id;
  if (response.status === 202) {
    try {
      id = response.json('job_id');
    } catch (_) {
      // Invalid response bodies fail the acceptance check below.
    }
  }
  if (check(response, {
    'acknowledged with a JobFlow job ID': (r) => r.status === 202 && /^job_[a-f0-9]{32}$/.test(id),
  })) {
    accepted.add(1);
    console.log(`JF_ACK ${sequence} ${id}`);
  }
}

export function handleSummary(data) {
  return {
    stdout: JSON.stringify({
      accepted: data.metrics.jobflow_accepted?.values.count ?? 0,
      submission_ms: data.metrics.http_req_duration?.values,
      submission_seconds: data.state.testRunDurationMs / 1000,
    }) + '\n',
  };
}
