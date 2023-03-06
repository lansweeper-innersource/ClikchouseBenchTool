import { log } from "./deps.ts";

import { getConfig, loadQueryModules } from "./config.ts";
import { getOptions, initProgram } from "./options.ts";
import { printResults } from "./printResults.ts";
import { runDbBenchmark } from "./benchmark/dbBenchmark.ts";
import { runQueryBenchmark } from "./benchmark/runBenchmark.ts";
import { getQueryExplain } from "./benchmark/explainBenchmark.ts";
import { downloadClickhouse } from "./utils.ts";

// Program settings
initProgram();
const options = getOptions();

// Check if clickhouse executable exists
try {
  await Deno.stat(`./clickhouse_${Deno.build.os}`);
} catch (err) {
  log.info(`Clickhouse executable ./clickhouse_${Deno.build.os} not found`);
  log.info(
    `Run: "curl https://clickhouse.com/ | sh" to download the official clickhouse binary`
  );
  try {
    await downloadClickhouse();
    log.info("Clickhouse executable downloaded!");
  } catch (err) {
    log.error("Error downloading clickhouse executable");
    Deno.exit(-1);
  }
}

const config = await getConfig();
const queryModules = await loadQueryModules();

for (const module of queryModules) {
  if (!options.module || options.module === module.name) {
    log.info(`Running module ${module.name}`);
    module.executed = true;
    for (const moduleQuery of module.queries) {
      try {
        moduleQuery.benchResult = await runDbBenchmark(
          moduleQuery.query,
          config
        );
        moduleQuery.indexResults = await getQueryExplain(
          moduleQuery.query,
          config.database
        );
        moduleQuery.runStatisticsResults = await runQueryBenchmark(
          moduleQuery.query,
          config.database
        );
        moduleQuery.executed = true;
        log.info(`${moduleQuery.name} ✔️`);
      } catch (err) {
        log.error(
          `Error running benchmark for query ${moduleQuery.query}: ${err}`
        );
        Deno.exit(-1);
      }
    }
  }
}

await printResults(queryModules);

log.info("Results generated ✅");
