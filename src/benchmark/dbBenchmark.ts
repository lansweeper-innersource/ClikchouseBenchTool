import { readLines } from "https://deno.land/std@0.177.0/io/read_lines.ts";
import { resolve } from "https://deno.land/std@0.178.0/path/mod.ts";

import { Config, getConfig } from "../config.ts";

export interface QueryBenchResult {
  executions: number;
  qps: number;
  rps: number;
  MiBs: number;
  resultRps: number;
  resultMiBs: number;
}

const parseBenchmarkResult = async (
  reader: Deno.Reader
): Promise<{ results: QueryBenchResult; output: string }> => {
  const config = await getConfig();
  let output = "";
  const results: QueryBenchResult = {
    resultMiBs: 0,
    executions: 0,
    qps: 0,
    resultRps: 0,
    MiBs: 0,
    rps: 0,
  };
  for await (const line of readLines(reader)) {
    output += `${line}\n`;
    if (line.startsWith(config.database.host)) {
      const stringResults = line.split(", ");
      stringResults.forEach((stringRow) => {
        if (stringRow.startsWith("queries")) {
          results.executions = +stringRow.split(" ")[1].replace(".", "");
        } else if (stringRow.startsWith("QPS:")) {
          results.qps = +stringRow.split(" ")[1];
        } else if (stringRow.startsWith("RPS:")) {
          results.rps = +stringRow.split(" ")[1];
        } else if (stringRow.startsWith("MiB/s:")) {
          results.MiBs = +stringRow.split(" ")[1];
        } else if (stringRow.startsWith("result RPS:")) {
          results.resultRps = +stringRow.split(" ")[2];
        } else if (stringRow.startsWith("result MiB/s:")) {
          results.resultMiBs = +stringRow.split(" ")[2].replace(".", "");
        }
      });
    }
  }
  return { results, output };
};

export const runDbBenchmark = async (query: string, config: Config) => {
  const benchCmd = [
    resolve(`./clickhouse_${Deno.build.os}`),
    "benchmark",
    `--host=${config.database.host}`,
    ...(config.database.tcpPort ? [`--port=${config.database.tcpPort}`] : []),
    `--user=${config.database.user}`,
    `--password=${config.database.password}`,
    `--iterations=${config.benchmark?.iterations || 1}`,
    `--database=assets`,
    `--query=${query}`,
  ];
  const p = Deno.run({ cmd: benchCmd, stdout: "piped", stderr: "piped" });

  const { code } = await p.status();

  const parsedResult = await parseBenchmarkResult(p.stderr);
  const benchResults = parsedResult.results;
  let benchOutput = parsedResult.output;

  if (code !== 0) {
    throw `ERROR: ${benchOutput}`;
  }

  return benchResults;
};
