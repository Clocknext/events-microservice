/** ClickHouse over HTTP: a read-only client for the dispatcher, and a write-only
 *  client for the consumer.
 *
 *  The edge uses NEITHER — it produces to Kafka and nothing else.
 *
 *  ─────────────────────────────────────────────────────────────────────────────
 *  ONE WRITER IS STILL THE RULE — THE WRITER CHANGED IDENTITY
 *
 *  `signal_log` has exactly ONE writer. It used to be the materialized view over
 *  the `ENGINE = Kafka` table; it is now `src/workers/consume/`, which pulls the
 *  topic itself so it can resolve each signal against payments before archiving
 *  it. A materialized view cannot make an HTTP call, which is the whole reason
 *  the engine had to go.
 *
 *  The dispatcher still only READS, and nothing else in the service touches
 *  ClickHouse at all. Keeping the two capabilities on two separate exports is
 *  what makes "who can write to the archive" answerable by grep.
 *
 *  What this costs, stated where someone will find it: the archive is no longer
 *  reproducible by replaying the topic. A replay re-runs the resolve calls, and a
 *  key that has since expired answers differently.
 *
 *  Queries go over HTTP with `JSONEachRow`, so there is no native-protocol driver
 *  to depend on. SELECT parameters are bound server-side (`param_<name>` +
 *  `{name:Type}` in the SQL), never interpolated, so a signal id out of the
 *  archive cannot become SQL. */
import { config } from '../config.js'

export interface ClickHouseReader {
  /** Runs one SELECT and returns its rows. `params` are bound, not interpolated. */
  query<T>(sql: string, params?: Record<string, string | number>): Promise<T[]>
}

export interface ClickHouseWriter {
  /** ONE insert, however many rows, as `JSONEachRow`.
   *
   *  Batching is the CALLER's job and is not optional: ClickHouse punishes
   *  per-row inserts (each one becomes a part that then has to be merged), and
   *  this is precisely the batching the Kafka engine used to do on our behalf.
   *
   *  `table` is a literal from our own source, never anything read out of a
   *  request or the archive — it is the one identifier here that cannot be a
   *  bound parameter. */
  insert<T extends object>(table: string, rows: readonly T[]): Promise<void>
}

/** The request URL, shared by both clients.
 *
 *  `date_time_input_format=best_effort` is set here rather than per query because
 *  every timestamp this pipeline moves is ISO-8601 with a `T` and a `Z`, and
 *  ClickHouse's default `basic` format rejects both characters. It matters on the
 *  write path too, now that the consumer sends `received_at` straight off the
 *  Kafka message instead of a materialized view parsing it. */
function endpoint(params: Record<string, string | number> = {}): URL {
  const url = new URL(config.clickhouseUrl)
  url.searchParams.set('database', config.clickhouseDatabase)
  url.searchParams.set('date_time_input_format', 'best_effort')
  url.searchParams.set('default_format', 'JSONEachRow')
  for (const [key, value] of Object.entries(params)) {
    url.searchParams.set(`param_${key}`, String(value))
  }
  return url
}

/** Basic auth rather than user/password query params, which would land the
 *  password in the server's query log. */
function headers(): Record<string, string> {
  return {
    'content-type': 'text/plain',
    authorization: `Basic ${Buffer.from(
      `${config.clickhouseUser}:${config.clickhousePassword}`,
    ).toString('base64')}`,
  }
}

export function createClickHouseReader(): ClickHouseReader {
  return {
    async query<T>(sql: string, params: Record<string, string | number> = {}): Promise<T[]> {
      // Global fetch, so neither worker pulls in an HTTP dependency of its own.
      const res = await fetch(endpoint(params), { method: 'POST', body: sql, headers: headers() })
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

export function createClickHouseWriter(): ClickHouseWriter {
  return {
    async insert<T extends object>(table: string, rows: readonly T[]): Promise<void> {
      // An empty batch is a normal outcome, not an error: a batch whose every
      // message was unresolvable writes nothing and commits nothing.
      if (rows.length === 0) return

      const body = `INSERT INTO ${table} FORMAT JSONEachRow\n${rows
        .map((row) => JSON.stringify(row))
        .join('\n')}`

      const res = await fetch(endpoint(), { method: 'POST', body, headers: headers() })
      if (!res.ok) {
        const text = await res.text()
        // Thrown, never swallowed: the caller must not commit an offset for a row
        // that did not land, or the signal is lost with the broker believing it
        // was handled.
        throw new Error(`clickhouse insert ${res.status}: ${text.slice(0, 400)}`)
      }
    },
  }
}
