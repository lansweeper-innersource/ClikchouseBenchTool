import { sleep } from "https://deno.land/x/sleep/mod.ts";

import { Config } from "../config.ts";
import { runQuery } from "../utils.ts";

export interface QueryRunStatistics {
  elapsed: number;
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
  config: Config["database"]
): Promise<QueryRunStatistics> => {
  const query_id = crypto.randomUUID();
  const resultString = await runQuery(
    config,
    query,
    `default_format=JSONCompact&query_id=${query_id}`
  );

  let waitForResults = true;
  const queryLog = `SELECT 
  query_duration_ms,
  read_rows,
  read_bytes,
  result_rows,
  result_bytes,
  memory_usage,
  ProfileEvents
  FROM clusterAllReplicas('lec', system.query_log) WHERE query_id = '${query_id}' AND type = 'QueryFinish'`;
  let queryLogResults: {
    query_duration_ms: string;
    read_rows: string;
    read_bytes: string;
    result_rows: string;
    result_bytes: string;
    memory_usage: string;
    ProfileEvents: {
      FileOpen: string;
      SelectedParts: string;
      OSCPUVirtualTimeMicroseconds: string;
    };
  } = {
    query_duration_ms: "",
    read_rows: "",
    read_bytes: "",
    result_rows: "",
    result_bytes: "",
    memory_usage: "",
    ProfileEvents: {
      FileOpen: "",
      SelectedParts: "",
      OSCPUVirtualTimeMicroseconds: "",
    },
  };
  while (waitForResults) {
    const queryResults = await runQuery(
      config,
      queryLog,
      `default_format=JSONCompact`
    );
    const queryResultsData = queryResults.data[0];

    if (queryResults.data.length > 0) {
      waitForResults = false;
      queryLogResults = {
        query_duration_ms: queryResultsData[0],
        read_rows: queryResultsData[1],
        read_bytes: queryResultsData[2],
        result_rows: queryResultsData[3],
        result_bytes: queryResultsData[4],
        memory_usage: queryResultsData[5],
        ProfileEvents: {
          FileOpen: queryResultsData[6].FileOpen,
          SelectedParts: queryResultsData[6].SelectedParts,
          OSCPUVirtualTimeMicroseconds:
            queryResultsData[6].OSCPUVirtualTimeMicroseconds,
        },
      };
    }
    await sleep(2);
  }
  return {
    elapsed: +queryLogResults.query_duration_ms,
    bytesRead: +queryLogResults.read_bytes,
    bytesResultsInRam: +queryLogResults.result_bytes,
    rowsRead: +queryLogResults.read_rows,
    rows: +queryLogResults.result_rows,
    rowsBeforeLimitAtLeast: resultString.rows_before_limit_at_least || 0,
    memoryUsage: +queryLogResults.memory_usage,
    CPUTime: +queryLogResults.ProfileEvents.OSCPUVirtualTimeMicroseconds,
  };
};
