import {readFileSync, writeFileSync} from 'node:fs';

const policy = new URL('./policy.mjs', import.meta.url);
const data = JSON.parse(readFileSync(new URL('../../../../api/bifrost/cloudflare_sdk_routes.json', import.meta.url), 'utf8'));
const original = readFileSync(policy, 'utf8');
const generated = /^export const SDK_ROUTE_POLICY = [\s\S]*?;$/m;
if (!generated.test(original)) throw new Error('Missing generated route policy declaration');
writeFileSync(policy, original.replace(generated, () => `export const SDK_ROUTE_POLICY = ${JSON.stringify(data, null, 2)};`));
