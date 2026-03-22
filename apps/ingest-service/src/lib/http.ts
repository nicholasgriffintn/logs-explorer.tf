import type { IngestConfig } from "./types";
import { sleep } from "./time";

export async function fetchJsonWithRetry(
  fetchFn: typeof fetch,
  url: string,
  retries: number,
  delayMs: number,
): Promise<unknown> {
  let attempt = 0;

  while (true) {
    attempt += 1;

    try {
      const response = await fetchFn(url, {
        method: "GET",
        headers: {
          accept: "application/json",
        },
      });

      if (!response.ok) {
        const isRetriable = response.status === 429 || response.status >= 500;

        if (!isRetriable || attempt >= retries) {
          const body = await response.text();
          throw new Error(`HTTP ${response.status} for ${url}: ${body.slice(0, 400)}`);
        }

        await sleep(delayMs * attempt);
        continue;
      }

      return (await response.json()) as unknown;
    } catch (error) {
      if (attempt >= retries) {
        throw error;
      }

      await sleep(delayMs * attempt);
    }
  }
}

export function toListUrl(config: IngestConfig, offset: number): string {
  const search = new URLSearchParams({
    limit: String(config.pageSize),
    offset: String(offset),
  });

  return `${config.apiBase}/log?${search.toString()}`;
}

export function toDetailUrl(config: IngestConfig, logId: number): string {
  return `${config.apiBase}/log/${logId}`;
}
