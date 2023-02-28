import * as log from "https://deno.land/std/log/mod.ts";

import { getConfig } from "./config.ts";
import { parseBenchmarkResult, QueryBenchResult } from "./benchmarkParser.ts";
import {
  getQueryExplain,
  getQueryRunStatistics,
  QueryRunStatistics,
} from "./chHttpClient.ts";
import { parserExplain, QueryExplain } from "./explainParser.ts";
import { printResults } from "./printResults.ts";

export interface QueryModuleQuery {
  name: string;
  query: string;
  benchResult?: QueryBenchResult;
  runStatisticsResults?: QueryRunStatistics;
  indexResults?: QueryExplain[];
}
export interface QueryModule {
  name: string;
  queries: QueryModuleQuery[];
}

// Check if clickhouse executable exists and download it
try {
  await Deno.stat("./clickhouse");
} catch (err) {
  log.error("Clickhouse executable not found");
  log.info(
    `Run: "curl https://clickhouse.com/ | sh" to download the official clickhouse binary`
  );
  Deno.exit(-1);
}

const config = await getConfig();

const replaceQueryParameters = (
  query: string,
  params: Record<string, string>
) => {
  return Object.entries(params).reduce((acc, [key, value]) => {
    return acc.replaceAll(`{ ${key} :String }`, `'${value}'`);
  }, query);
};

const queryModules: QueryModule[] = [];
const queryFilesIterable = Deno.readDir(config.queryDirectory);
for await (const file of queryFilesIterable) {
  if (file.isDirectory) {
    const queryModuleIterable = Deno.readDir(
      `${config.queryDirectory}/${file.name}`
    );
    for await (const moduleQuery of queryModuleIterable) {
      const module: QueryModule = {
        name: moduleQuery.name,
        queries: [],
      };
      if (moduleQuery.isFile && moduleQuery.name.split(".").pop() === "sql") {
        const sqlContent = await Deno.readFile(
          `${config.queryDirectory}/${file.name}/${moduleQuery.name}`
        );

        module.queries.push({
          name: moduleQuery.name,
          query: replaceQueryParameters(
            new TextDecoder().decode(sqlContent),
            config.params
          ),
        });
      }
      queryModules.push(module);
    }
  }
}

for (const module of queryModules) {
  log.info(`Running module ${module.name}`);
  for (const moduleQuery of module.queries) {
    const benchCmd = [
      "./clickhouse",
      "benchmark",
      `--host=${config.database.host}`,
      // ...(config.database.port ? [`--port=${config.database.port}`] : []),
      `--user=${config.database.user}`,
      `--password=${config.database.password}`,
      `--iterations=10`,
      `--database=assets`,
      `--query=${moduleQuery.query}`,
    ];
    const p = Deno.run({ cmd: benchCmd, stdout: "piped", stderr: "piped" });

    const { code } = await p.status();
    let benchOutput = "";
    try {
      // For any reason the clickhouse cli outputs the results on the stderr stream
      const parsedResult = await parseBenchmarkResult(p.stderr);
      const benchResults = parsedResult.results;
      benchOutput = parsedResult.output;
      const explainResults = await getQueryExplain(
        config.database,
        moduleQuery.query
      );
      const runStatisticsResults = await getQueryRunStatistics(
        config.database,
        moduleQuery.query
      );
      const indexResults = parserExplain(explainResults);
      moduleQuery.benchResult = benchResults;
      moduleQuery.indexResults = indexResults;
      moduleQuery.runStatisticsResults = runStatisticsResults;
    } catch (err) {
      log.error(`Error parsing benchmark results: ${err}`);
      Deno.exit(-1);
    }
    if (code === 0) {
      log.info(`${moduleQuery.name} ✔️`);
    } else {
      log.error(`ERROR: ${benchOutput}`);
      Deno.exit(-1);
    }
  }
}

await printResults(queryModules);

log.info("Results generated ✅");
