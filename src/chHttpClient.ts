import { Config } from "./config.ts";

export interface QueryRunStatistics {
  elapsed: number;
  rowsRead: number;
  bytesRead: number;
  bytesReadStr: string;
}

const formatBytes = (bytes: number, decimals = 2) => {
  if (!+bytes) return "0 Bytes";

  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"];

  const i = Math.floor(Math.log(bytes) / Math.log(k));

  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${sizes[i]}`;
};

const runQuery = async (
  config: Config["database"],
  query: string,
  params: string | undefined = undefined
) => {
  const url = `http://${config.host}:${config.port}${
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

export const getQueryExplain = (
  config: Config["database"],
  query: string
): Promise<unknown> => {
  return runQuery(
    config,
    `EXPLAIN indexes = 1, json = 1, description = 1 ${query} FORMAT TSVRaw`
  );
};

export const getQueryRunStatistics = async (
  config: Config["database"],
  query: string
): Promise<QueryRunStatistics> => {
  const resultString = await runQuery(
    config,
    query,
    "default_format=JSONCompact"
  );

  const parsedResultStatistics = resultString.statistics;
  return {
    elapsed: parsedResultStatistics.elapsed as number,
    bytesRead: parsedResultStatistics.bytes_read as number,
    bytesReadStr: formatBytes(parsedResultStatistics.bytes_read),
    rowsRead: parsedResultStatistics.rows_read,
  };
};
