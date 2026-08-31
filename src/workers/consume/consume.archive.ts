/** The ONE INSERT the consumer runs against the archive.
 *
 *  It goes through the write-only `ClickHouseWriter`, which is a separate export
 *  from the dispatcher's reader so that "who can write to `signal_log`" stays
 *  answerable by grep. One call per batch, never per row. */
import type { ClickHouseWriter } from '../../client/clickhouse.js'
import type { ArchiveWriter, SignalLogInsert } from './consume.schema.js'

/** The one table this process writes. A literal, deliberately — it is the only
 *  identifier on this path that cannot be a bound parameter. */
const TABLE = 'signal_log'

export function createArchiveWriter(clickhouse: ClickHouseWriter): ArchiveWriter {
  return {
    async write(rows: readonly SignalLogInsert[]): Promise<void> {
      await clickhouse.insert(TABLE, rows)
    },
  }
}
