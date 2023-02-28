import json2md from "https://esm.sh/json2md@2.0.0";
import { QueryModule } from "./main.ts";

export const printResults = async (queryResults: QueryModule[]) => {
  const md: json2md.DataObject[] = [];
  const headers = [
    "Query",
    "Elapsed",
    "Bytes read",
    "Rows read",
    "QPS",
    "MB To results",
    "Rows to results",
    "RPS",
  ];
  let rows: any[] = [];
  queryResults.forEach((result) => {
    const queryRow = result.queries
      .map((q) => [
        q.name,
        `${Math.round(q.runStatisticsResults?.elapsed! * 100) / 100}s`,
        q.runStatisticsResults?.bytesReadStr,
        q.runStatisticsResults?.rowsRead,
        q.benchResult?.queriesPerSecond,
        q.benchResult?.megabytesPlacedToResult,
        q.benchResult?.rowsPlacedToResults,
        q.benchResult?.serverRowReadsPerSecond,
      ])
      .flat();
    rows.push(queryRow);
  });
  md.push({ table: { headers, rows } });
  const encoder = new TextEncoder();
  const data = encoder.encode(json2md(md));
  await Deno.writeFile("./results.md", data);
};
