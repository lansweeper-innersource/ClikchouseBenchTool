import * as log from "https://deno.land/std/log/mod.ts";
import { parse, stringify } from "https://deno.land/std/encoding/toml.ts";
import {
  Confirm,
  Input,
  Number,
  Secret,
} from "https://deno.land/x/cliffy@v0.25.7/prompt/mod.ts";

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
  indexResults?: QueryExplain;
}
export interface QueryModule {
  executed?: boolean;
  name: string;
  queries: QueryModuleQuery[];
}

let config: Config;
export const getConfig = async (): Promise<Config> => {
  if (!config) {
    try {
      const configFile = await Deno.readTextFile("./config.toml");
      config = parse(configFile) as unknown as Config;
    } catch (err) {
      const host = await Input.prompt("Enter clickhouse host");
      const port = await Number.prompt("Enter clickhouse port");
      const user = await Input.prompt(`Enter clickhouse user`);
      const password = await Secret.prompt(
        `Enter clickhouse password for user ${user}`
      );
      const database = await Input.prompt("Enter clickhouse databse");
      console.log({ host, port, user, database });
      if (!(await Confirm.prompt("Is everything correct?"))) {
        throw new Error("Bye");
      }
      const promptConfig: Config = {
        database: {
          database,
          host,
          password,
          port: (port || 8123).toString(),
          user,
        },
        params: { siteId: "123" },
        queryDirectory: "queries",
      };
      console.log(promptConfig);
      const tomlConfig = stringify(
        promptConfig as unknown as Record<string, unknown>
      );
      const encoder = new TextEncoder();
      const data = encoder.encode(tomlConfig);
      await Deno.writeFile("./config.toml", data);

      const configFile = await Deno.readTextFile("./config.toml");
      config = parse(configFile) as unknown as Config;
    }
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
