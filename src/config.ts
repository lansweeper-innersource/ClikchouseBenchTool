import { parse } from "https://deno.land/std/encoding/toml.ts";

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

let config: Config;
export const getConfig = async (): Promise<Config> => {
  if (!config) {
    const configFile = await Deno.readTextFile("./config.toml");
    config = parse(configFile) as unknown as Config;
  }
  return config;
};
