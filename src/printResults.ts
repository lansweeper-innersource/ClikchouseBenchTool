import { resolve } from "https://deno.land/std@0.178.0/path/mod.ts";
import json2md from "https://esm.sh/json2md@2.0.0";

import { QueryModule } from "./config.ts";

const arraySplit = (arr?: [number, number]) =>
  arr ? `${arr[0]} / ${arr[1]} ${Math.round((arr[1] * 100) / arr[0])}%` : "N/a";

export const printResults = async (queryResults: QueryModule[]) => {
  const md: json2md.DataObject[] = [];

  md.push({
    ul: [
      "Query: Query name",
      "Elapsed: Time it took for the query to be executed.",
      "Bytes read: Amount of bytes that ClickHouse has loaded to execute the query",
      "Rows read: Number of rows read to execute the query.",
      "QPS: Queries executed per second",
      "Result RPS:  How many megabytes the server reads per second",
      "Result MiB/s: How many rows placed by the server to the result of a query per second",
      "RPS: How many rows the server reads per second",
    ],
  });

  queryResults
    .filter((r) => r.executed)
    .forEach((result) => {
      md.push({ h2: result.name });
      md.push({ h4: "Query benchmark" });
      const queryBenchmarkRows: any[] = result.queries
        .filter((q) => q.executed)
        .map((q) => [
          q.name,
          `${Math.round(q.runStatisticsResults?.elapsed! * 10000) / 10000}s`,
          q.runStatisticsResults?.bytesReadStr,
          q.runStatisticsResults?.rowsRead?.toString(),
          q.runStatisticsResults?.rows?.toString(),
          q.runStatisticsResults?.rowsBeforeLimitAtLeast?.toString(),
        ]);
      md.push({
        table: {
          headers: ["Query / Results", "Elapsed", "Bytes read", "Rows read"],
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
    });
  const encoder = new TextEncoder();
  const data = encoder.encode(json2md(md));
  await Deno.writeFile(resolve("./results.md"), data);
};
