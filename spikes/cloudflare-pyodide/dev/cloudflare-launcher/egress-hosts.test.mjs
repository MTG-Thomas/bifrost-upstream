import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import {egressHostAllowed, isHttpsEgressUrl, normalizeEgressPattern} from '../executor/egress-hosts.mjs';
import psl from '../executor/egress-psl.generated.mjs';

const api = new URL('../../../../api/', import.meta.url);
const vectors = JSON.parse(readFileSync(new URL('tests/unit/services/execution/cloudflare_host_vectors.json', api)));

test('API and Worker ship the same generated Public Suffix List', () => {
  const apiPsl = JSON.parse(readFileSync(new URL('src/services/execution/cloudflare_psl.json', api)));
  assert.deepEqual(psl, apiPsl);
});

for (const [input, expected] of vectors.patterns) {
  test(`approval pattern ${JSON.stringify(input)}`, () => assert.equal(normalizeEgressPattern(input), expected));
}

for (const [patterns, host, allowed] of vectors.matches) {
  test(`host ${JSON.stringify(host)} in ${JSON.stringify(patterns)}`, () => {
    assert.equal(egressHostAllowed(host, new Set(patterns)), allowed);
  });
}

test('gateway URL parsing supplies the A-label and allows only HTTPS on the default port', () => {
  const allowed = new Set(['*.xn--bcher-kva.de']);
  for (const [input, expected] of [
    ['https://münchen.bücher.de', true],
    ['https://münchen.bücher.de:443', true],
    ['https://münchen.bücher.de:8443', false],
    ['http://münchen.bücher.de', false],
    ['https://münchen.bücher.de.', false],
    ['https://user@münchen.bücher.de', false],
  ]) {
    const url = new URL(input);
    assert.equal(isHttpsEgressUrl(url) && egressHostAllowed(url.hostname, allowed), expected, input);
  }
});
