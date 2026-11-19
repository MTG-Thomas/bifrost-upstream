import {WorkerEntrypoint} from 'cloudflare:workers';
import {openAgentStream} from '../agent-provider.mjs';

export class AgentProvider extends WorkerEntrypoint {
  stream(grant, cancellation) {
    return openAgentStream(this.env.BIFROST_CLOUDFLARE_AGENT_ENDPOINT, grant, cancellation);
  }
}
