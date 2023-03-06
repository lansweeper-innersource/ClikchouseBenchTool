import { resolve } from "https://deno.land/std@0.178.0/path/mod.ts";

import { Config } from "./config.ts";

export const formatBytes = (bytes: number, decimals = 2) => {
  if (!+bytes) return "0 Bytes";

  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"];

  const i = Math.floor(Math.log(bytes) / Math.log(k));

  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${sizes[i]}`;
};

export const runQuery = async (
  config: Config["database"],
  query: string,
  params: string | undefined = undefined
) => {
  const url = `http://${config.host}:${config.httpPort}${
    params ? `?${params}` : ""
  }`;
  const resp = await fetch(encodeURI(url), {
    method: "POST",
    headers: {
      "X-ClickHouse-Database": config.database,
      "X-ClickHouse-User": config.user,
      "X-ClickHouse-Key": config.password,
    },
    body: query,
  });

  let resultString = "";
  for await (const chunk of resp.body!) {
    const result = new TextDecoder().decode(chunk);
    resultString += result;
  }
  const parsedResult = JSON.parse(resultString);
  return parsedResult;
};

export const downloadClickhouse = async () => {
  const p = Deno.run({
    cmd: ["curl", "https://clickhouse.com/"],
    stdout: "piped",
  });
  await p.status();
  const rawOutput = await p.output();

  await Deno.writeFile("./download.sh", rawOutput);
  const installChCommand = Deno.run({
    cmd: ["sh", resolve("./download.sh")],
  });
  await installChCommand.status();
  await Deno.remove(resolve("./download.sh"));

  const rename = Deno.run({
    cmd: [
      "mv",
      resolve("./clickhouse"),
      resolve(`./clickhouse_${Deno.build.os}`),
    ],
  });

  const { success } = await rename.status();
  if (!success) {
    throw new Error(`Error renaming ${resolve("./clickhouse")}`);
  }
};
