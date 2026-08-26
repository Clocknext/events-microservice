/** Read-only ClickHouse client for the dispatcher.
 *
 *  The edge does NOT use this — it produces to Kafka and ClickHouse ingests on
 *  its own. This is the dispatcher's window onto the archive.
 *
 *  Read-only is a rule, not a coincidence: `signal_log` has exactly ONE writer,
 *  the materialized view over the Kafka table. Everything else reads. That is
 *  what makes the archive rebuildable by replaying the topic — a second writer
 *  would put rows in it that no replay could reproduce.
 *
 *  Queries go over HTTP with `JSONEachRow`, so there is no native-protocol
 *  driver to depend on. Parameters are bound server-side (`param_<name>` +
 *  `{name:Type}` in the SQL), never interpolated, so a signal id out of the
 *  archive cannot become SQL. */
import { config } from '../config.js'

export interface ClickHouseReader {
  /** Runs one SELECT and returns its rows. `params` are bound, not interpolated. */
  query<T>(sql: string, params?: Record<string, string | number>): Promise<T[]>
}

export function createClickHouseReader(): ClickHouseReader {
  return {
    async query<T>(sql: string, params: Record<string, string | number> = {}): Promise<T[]> {
      const url = new URL(config.clickhouseUrl)
      url.searchParams.set('database', config.clickhouseDatabase)
      // ISO-8601 with a `T` and a `Z` is what the edge stamps, and ClickHouse's
      // default `date_time_input_format=basic` rejects both characters. Every
      // timestamp this client sends or parses is ISO, so it is set once here.
      url.searchParams.set('date_time_input_format', 'best_effort')
      url.searchParams.set('default_format', 'JSONEachRow')
      for (const [key, value] of Object.entries(params)) {
        url.searchParams.set(`param_${key}`, String(value))
      }

      // Global fetch, so the dispatcher pulls in no HTTP dependency of its own.
      const res = await fetch(url, {
        method: 'POST',
        body: sql,
        headers: {
          'content-type': 'text/plain',
          // Basic auth rather than user/password query params, which would land
          // the password in the server's query log.
          authorization: `Basic ${Buffer.from(
            `${config.clickhouseUser}:${config.clickhousePassword}`,
          ).toString('base64')}`,
        },
      })

      const text = await res.text()
      if (!res.ok) {
        throw new Error(`clickhouse ${res.status}: ${text.slice(0, 400)}`)
      }
      // JSONEachRow is newline-delimited objects, and an empty result is an
      // empty body rather than `[]`.
      return text
        .split('\n')
        .filter((line: string) => line.trim().length > 0)
        .map((line: string) => JSON.parse(line) as T)
    },
  }
}
