/** HTTP client for ClickHouse, used by the SQS consumers — never by the edge.
 *
 *  One method, `insert`, because that is all a consumer does: take a batch off
 *  a queue and bulk-insert it. The rows are handed over already column-shaped
 *  (the queue message IS the row), so there is no mapping here.
 *
 *  `undici` rather than global fetch, to match `payments-client.ts` and to have
 *  request/body timeouts that a stalled ClickHouse cannot outlast. */
import { request } from 'undici'
import { config } from '../config.js'

export interface ClickHouseClient {
  /** Bulk-inserts rows into one table via `FORMAT JSONEachRow`. Throws on any
   *  non-2xx, so the caller can decide the message's fate (retry vs DLQ). A
   *  zero-row call is a no-op and makes no request. */
  insert(table: string, rows: object[]): Promise<void>
}

export interface ClickHouseOptions {
  url?: string
  database?: string
  user?: string
  password?: string
  /** Ceiling for one insert. A consumer that hangs here holds its Lambda open
   *  and, past the queue's visibility timeout, gets the batch redelivered. */
  timeoutMs?: number
}

export function createClickHouseClient(options: ClickHouseOptions = {}): ClickHouseClient {
  const url = (options.url ?? config.clickhouseUrl).replace(/\/$/, '')
  const database = options.database ?? config.clickhouseDatabase
  const user = options.user ?? config.clickhouseUser
  const password = options.password ?? config.clickhousePassword
  const timeoutMs = options.timeoutMs ?? 10_000

  return {
    async insert(table, rows) {
      if (rows.length === 0) return

      // One JSON object per line — the JSONEachRow wire format. Column shape is
      // the row's own shape, so a missing Nullable column defaults to null and
      // a null into a non-Nullable column takes the column default.
      const body = rows.map((row) => JSON.stringify(row)).join('\n')

      const params = new URLSearchParams({
        database,
        query: `INSERT INTO ${table} FORMAT JSONEachRow`,
        // ClickHouse's default `basic` format rejects the ISO-8601 `received_at`
        // the edge writes — the `T` and the `Z`. `best_effort` accepts it.
        date_time_input_format: 'best_effort',
      })

      const res = await request(`${url}/?${params.toString()}`, {
        method: 'POST',
        body,
        headers: {
          'content-type': 'text/plain; charset=utf-8',
          // Credentials as headers, not in the query string, so they never land
          // in a ClickHouse query_log entry.
          'x-clickhouse-user': user,
          ...(password ? { 'x-clickhouse-key': password } : {}),
        },
        headersTimeout: timeoutMs,
        bodyTimeout: timeoutMs,
      })

      if (res.statusCode >= 300) {
        // ClickHouse returns the reason as plain text. Surface it — a schema or
        // parse error here is a bug to read, and the message will DLQ after its
        // retries rather than vanish.
        const detail = await res.body.text()
        throw new Error(
          `ClickHouse insert into ${table} failed (${res.statusCode}): ${detail.slice(0, 500)}`,
        )
      }
      // Drain the body so the socket returns to the pool.
      await res.body.dump()
    },
  }
}
