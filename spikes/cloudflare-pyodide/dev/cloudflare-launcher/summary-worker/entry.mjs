import {WorkerEntrypoint} from 'cloudflare:workers';
import {completeSummary} from '../summary-provider.mjs';

export class SummaryProvider extends WorkerEntrypoint {
  complete(context, cancellation) {
    return completeSummary(this.env.BIFROST_CLOUDFLARE_SUMMARY_ENDPOINT, context, cancellation);
  }
}
