import { Config, QueryModuleQuery } from "../config.ts";
import { runQuery } from "../utils.ts";
import { sleep } from "https://deno.land/x/sleep@v1.2.1/sleep.ts";

export interface QueryRunStatistics {
  rowsRead: number;
  bytesRead: number;
  bytesResultsInRam: number;
  rows: number;
  rowsBeforeLimitAtLeast: number;
  memoryUsage: number;
  CPUTime: number;
}

export const runQueryBenchmark = async (
  query: string,
  params: QueryModuleQuery["params"],
  config: Config,
): Promise<
  { parsedQuery: string; runStatisticsResults: QueryRunStatistics }
> => {
  const queryId = crypto.randomUUID();
  const queryResults = await runQuery(
    {
      query,
      config: config.database,
      extraQueryParams: `default_format=JSONCompact&query_id=${queryId}`,
      queryParams: params,
    },
  );

  const queryLog = `SELECT query_duration_ms,
                             query,
                             read_rows,
                             read_bytes,
                             result_rows,
                             result_bytes,
                             memory_usage,
                             ProfileEvents
                      FROM clusterAllReplicas('lec', system.query_log)
                      WHERE query_id IN '${queryId}'
                        AND type = 'QueryFinish'`;

  // The query log appears with delay because of the asynchronous nature of the system.query_log table.
  while (true) {
    const queryLogResults = await runQuery(
      {
        config: config.database,
        query: queryLog,
        extraQueryParams: `default_format=JSON`,
      },
    );
    if (queryLogResults.data.length) {
      const {
        query,
        read_bytes,
        result_bytes,
        read_rows,
        result_rows,
        memory_usage,
        ProfileEvents: { OSCPUVirtualTimeMicroseconds },
      } = queryLogResults.data[0];

      return {
        parsedQuery: query,
        runStatisticsResults: {
          bytesRead: read_bytes,
          bytesResultsInRam: result_bytes,
          rowsRead: read_rows,
          rows: result_rows,
          rowsBeforeLimitAtLeast: queryResults.rows_before_limit_at_least ?? 0,
          memoryUsage: memory_usage,
          CPUTime: OSCPUVirtualTimeMicroseconds,
        },
      };
    }
    await sleep(1);
  }
};
