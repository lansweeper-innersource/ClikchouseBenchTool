import { Config } from "../config.ts";
import { formatBytes, runQuery } from "../utils.ts";

export interface QueryRunStatistics {
  elapsed: number;
  rowsRead: number;
  bytesRead: number;
  bytesReadStr: string;
  rows: number;
  rowsBeforeLimitAtLeast: number;
}

export const runQueryBenchmark = async (
  query: string,
  config: Config["database"]
): Promise<QueryRunStatistics> => {
  const resultString = await runQuery(
    config,
    query,
    "default_format=JSONCompact"
  );

  const parsedResultStatistics = resultString.statistics;

  return {
    elapsed: parsedResultStatistics.elapsed as number,
    bytesRead: parsedResultStatistics.bytes_read as number,
    bytesReadStr: formatBytes(parsedResultStatistics.bytes_read),
    rowsRead: parsedResultStatistics.rows_read,
    rows: resultString.rows,
    rowsBeforeLimitAtLeast: resultString.rows_before_limit_at_least || 0,
  };
};
