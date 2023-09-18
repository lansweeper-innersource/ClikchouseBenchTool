import { JSONPath } from "https://esm.sh/jsonpath-plus@7.2.0";

import { Config } from "../config.ts";
import { runQuery } from "../utils.ts";

export interface QueryExplainRaw {
  Type: string;
  Keys: string[];
  "Initial Parts": number;
  "Selected Parts": number;
  "Initial Granules": number;
  "Selected Granules": number;
}

export interface QueryExplainArray {
  type: string;
  keys: string[];
  parts: [number, number];
  granules: [number, number];
}

interface QueryExplainResult {
  type: "MinMax" | "Partition" | "PrimaryKey";
  parts: [number, number];
  keys: [string, string];
  granules: [number, number];
}

export type QueryExplain = Record<
  "MinMax" | "Partition" | "PrimaryKey",
  QueryExplainResult
>;

export const getQueryExplain = async (
  query: string,
  config: Config["database"],
): Promise<QueryExplain> => {
  const explain = await runQuery(
    {
      config,
      query:
        `EXPLAIN indexes = 1, json = 1, description = 1 ${query} FORMAT TSVRaw`,
    },
  );
  const indexes: QueryExplainRaw[] = JSONPath({
    path: "$..Indexes",
    json: explain,
    flatten: true,
  });

  const maps = indexes.map((rawExplain) => ({
    type: rawExplain.Type,
    keys: rawExplain.Keys,
    parts: [rawExplain["Initial Parts"], rawExplain["Selected Parts"]],
    granules: [rawExplain["Initial Granules"], rawExplain["Selected Granules"]],
  }));
  return maps.reduce(
    (acc, curr) => ({ ...acc, [curr.type]: curr }),
    {} as QueryExplain,
  );
};
