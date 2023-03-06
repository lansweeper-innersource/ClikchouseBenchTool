import { path, toml, cliffy } from "./deps.ts";

import { QueryBenchResult } from "./benchmark/dbBenchmark.ts";
import { QueryExplain } from "./benchmark/explainBenchmark.ts";
import { QueryRunStatistics } from "./benchmark/runBenchmark.ts";

export interface Config {
  queryDirectory: string;
  database: {
    host: string;
    user: string;
    password: string;
    httpPort: number;
    tcpPort: number;
    database: string;
  };
  benchmark: {
    iterations: number;
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
      const configFile = await Deno.readTextFile(path.resolve("./config.toml"));
      config = toml.parse(configFile) as unknown as Config;
    } catch (err) {
      const host = await cliffy.Input.prompt("Enter clickhouse host");
      const httpPort = await cliffy.Number.prompt({
        message: "Enter clickhouse http port",
        default: 8123,
      });
      const tcpPort = await cliffy.Number.prompt({
        message: "Enter clickhouse tcp port",
        default: 9000,
      });
      const user = await cliffy.Input.prompt(`Enter clickhouse user`);
      const password = await cliffy.Secret.prompt(
        `Enter clickhouse password for user ${user}`
      );
      const database = await cliffy.Input.prompt("Enter clickhouse databse");
      console.log({ host, httpPort, tcpPort, user, database });
      if (!(await cliffy.Confirm.prompt("Is everything correct?"))) {
        throw new Error("Bye");
      }
      const promptConfig: Config = {
        database: {
          database,
          host,
          password,
          httpPort: httpPort || 9000,
          tcpPort: tcpPort || 8123,
          user,
        },
        params: { siteId: "123" },
        queryDirectory: "queries",
        benchmark: {
          iterations: 1,
        },
      };
      const tomlConfig = toml.stringify(
        promptConfig as unknown as Record<string, unknown>
      );
      const encoder = new TextEncoder();
      const data = encoder.encode(tomlConfig);

      await Deno.writeFile(path.resolve("./config.toml"), data);
      await Deno.mkdir(path.resolve("./queries"));
      await Deno.mkdir(path.resolve("./queries/001_demo_query"));

      await Deno.writeFile(
        path.resolve("./queries/001_demo_query/demo_query.sql"),
        encoder.encode("SELECT 1 = 1")
      );

      const configFile = await Deno.readTextFile(path.resolve("./config.toml"));
      config = toml.parse(configFile) as unknown as Config;
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
  const queryFilesIterable = Deno.readDir(path.resolve(config.queryDirectory));
  for await (const file of queryFilesIterable) {
    if (file.isDirectory) {
      const queryModuleIterable = Deno.readDir(
        path.resolve(`./${config.queryDirectory}/${file.name}`)
      );
      const module: QueryModule = {
        name: file.name,
        queries: [],
      };
      for await (const moduleQuery of queryModuleIterable) {
        if (moduleQuery.isFile && moduleQuery.name.split(".").pop() === "sql") {
          const sqlContent = await Deno.readFile(
            path.resolve(
              `./${config.queryDirectory}/${file.name}/${moduleQuery.name}`
            )
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
