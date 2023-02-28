import { JSONPath } from "https://esm.sh/jsonpath-plus@7.2.0";

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
