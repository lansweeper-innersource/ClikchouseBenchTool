import { parse } from "https://deno.land/std/encoding/toml.ts";

import { QueryBenchResult } from "./benchmark/dbBenchmark.ts";
import { QueryExplain } from "./benchmark/explainBenchmark.ts";
import { QueryRunStatistics } from "./benchmark/runBenchmark.ts";

export interface Config {
  queryDirectory: string;
  database: {
    host: string;
    user: string;
    password: string;
    port: string;
    database: string;
  };
  params: Record<string, string>;
}

export interface QueryModuleQuery {
  executed?: boolean;
  name: string;
  query: string;
  benchResult?: QueryBenchResult;
  runStatisticsResults?: QueryRunStatistics;
  indexResults?: QueryExplain[];
}
export interface QueryModule {
  executed?: boolean;
  name: string;
  queries: QueryModuleQuery[];
}

let config: Config;
export const getConfig = async (): Promise<Config> => {
  if (!config) {
    const configFile = await Deno.readTextFile("./config.toml");
    config = parse(configFile) as unknown as Config;
  }
  return config;
};

export const loadQueryModules = async () => {
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
      const module: QueryModule = {
        name: file.name,
        queries: [],
      };
      for await (const moduleQuery of queryModuleIterable) {
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
      }
      queryModules.push(module);
    }
  }

  return queryModules;
};
