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
  queryResults.forEach((result) => {
    md.push({ h2: result.name });
    const queryRow: any[] = result.queries.map((q) => [
      q.name,
      `${Math.round(q.runStatisticsResults?.elapsed! * 100) / 100}s`,
      q.runStatisticsResults?.bytesReadStr,
      q.runStatisticsResults?.rowsRead.toString(),
      q.benchResult?.queriesPerSecond.toString(),
      q.benchResult?.megabytesPlacedToResult.toString(),
      q.benchResult?.rowsPlacedToResults.toString(),
      q.benchResult?.serverRowReadsPerSecond.toString(),
    ]);

    md.push({ table: { headers, rows: queryRow } });
  });
  const encoder = new TextEncoder();
  const data = encoder.encode(json2md(md));
  await Deno.writeFile("./results.md", data);
};
