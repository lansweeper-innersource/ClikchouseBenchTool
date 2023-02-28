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

export interface QueryExplain {
  type: string;
  keys: string[];
  parts: [number, number];
  granules: [number, number];
}

export const parserExplain = (explain: unknown): QueryExplain[] => {
  const indexes: QueryExplainRaw[] = JSONPath({
    path: "$..Indexes",
    json: explain,
    flatten: true,
  });
  return indexes.map((rawExplain) => ({
    type: rawExplain.Type,
    keys: rawExplain.Keys,
    parts: [rawExplain["Initial Parts"], rawExplain["Selected Parts"]],
    granules: [rawExplain["Initial Granules"], rawExplain["Selected Granules"]],
  }));
};

export const getQueryExplain = async (
  query: string,
  config: Config["database"]
): Promise<QueryExplain[]> => {
  const explain = await runQuery(
    config,
    `EXPLAIN indexes = 1, json = 1, description = 1 ${query} FORMAT TSVRaw`
  );
  const indexes: QueryExplainRaw[] = JSONPath({
    path: "$..Indexes",
    json: explain,
    flatten: true,
  });
  return indexes.map((rawExplain) => ({
    type: rawExplain.Type,
    keys: rawExplain.Keys,
    parts: [rawExplain["Initial Parts"], rawExplain["Selected Parts"]],
    granules: [rawExplain["Initial Granules"], rawExplain["Selected Granules"]],
  }));
};
