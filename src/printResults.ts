import { resolve } from "https://deno.land/std@0.178.0/path/mod.ts";
import json2md from "https://esm.sh/json2md@2.0.0";

import { QueryModule } from "./config.ts";
import { bytesToHuman, microsecondsToHuman } from "./utils.ts";

const arraySplit = (arr?: [number, number]) =>
  arr ? `${arr[0]} / ${arr[1]} ${Math.round((arr[1] * 100) / arr[0])}%` : "N/a";

export const printResults = async (queryResults: QueryModule[]) => {
  queryResults
    .filter((r) => r.executed)
    .forEach(async (result) => {
      const md: json2md.DataObject[] = [];

      md.push({ h2: "Metrics legend" });
      md.push({
        ul: [
          "Query: Query name",
          "Bytes read: Amount of bytes that ClickHouse has loaded to execute the query",
          "Rows read: Number of rows read to execute the query.",
          "QPS: Queries executed per second",
          "Result RPS:  How many megabytes the server reads per second",
          "Result MiB/s: How many rows placed by the server to the result of a query per second",
          "RPS: How many rows the server reads per second",
        ],
      });
      md.push({ h2: result.name });
      md.push({ h4: "Query benchmark" });
      const queryBenchmarkRows: any[] = result.queries
        .filter((q) => q.executed)
        .map((q) => [
          q.name,
          bytesToHuman(q.runStatisticsResults?.bytesRead || 0),
          q.runStatisticsResults?.rowsRead?.toString(),
          q.runStatisticsResults?.rows?.toString(),
          q.runStatisticsResults?.rowsBeforeLimitAtLeast?.toString(),
          bytesToHuman(q.runStatisticsResults?.bytesResultsInRam || 0),
          bytesToHuman(q.runStatisticsResults?.memoryUsage || 0),
          microsecondsToHuman(q.runStatisticsResults?.CPUTime!),
        ]);
      md.push({
        table: {
          headers: [
            "Query / Results",
            "Bytes read",
            "Rows read",
            "Rows",
            "Rows before Limit",
            "Results In Ram",
            "Ram used",
            "CPU Time",
          ],
          rows: queryBenchmarkRows,
        },
      });

      const diskBenchmarkRows: any[] = result.queries
        .filter((q) => q.executed)
        .map((q) => [
          q.name,
          q.benchResult?.qps.toString(),
          q.benchResult?.resultRps.toString(),
          q.benchResult?.resultMiBs.toString(),
          q.benchResult?.rps.toString(),
        ]);
      md.push({
        table: {
          headers: [
            "Query / Results",
            "QPS",
            "Result RPS",
            "Result MiB/s",
            "RPS",
          ],
          rows: diskBenchmarkRows,
        },
      });

      md.push({ h4: "Index benchmark" });
      const indexBenchmark: any[] = result.queries
        .filter((q) => q.executed)
        .map((q) => [
          q.name,
          ...(q.indexResults?.MinMax
            ? [
              arraySplit(q.indexResults.MinMax.granules),
              arraySplit(q.indexResults.MinMax.parts),
            ]
            : []),
          ...(q.indexResults?.Partition
            ? [
              (q.indexResults?.Partition.keys || []).join(", "),
              arraySplit(q.indexResults?.Partition.parts),
              arraySplit(q.indexResults?.Partition.granules),
            ]
            : []),
          ,
          ...(q.indexResults?.PrimaryKey
            ? [
              (q.indexResults?.PrimaryKey.keys || []).join(", "),
              arraySplit(q.indexResults?.PrimaryKey.parts),
              arraySplit(q.indexResults?.PrimaryKey.granules),
            ]
            : []),
        ]);
      md.push({
        table: {
          headers: [
            "Query / Results",
            "MinMax granules",
            "MinMax parts",
            "Partition keys",
            "Partition parts",
            "Partition granules",
            "PK keys",
            "PK parts",
            "PK granules",
          ],
          rows: indexBenchmark,
        },
      });
      const encoder = new TextEncoder();
      const data = encoder.encode(json2md(md));
      await Deno.writeFile(resolve(`./${result.name}_results.md`), data);
    });
};
