import { commander } from "./deps.ts";

export interface ProgramParams {
  module?: string;
  iterations?: number;
  config?: string;
}

export const initProgram = () => {
  commander.program
    .name("Clickhouse benchmarking tool")
    .description("CLI tool to test, analyze and compare clickhouse queries")
    .version("0.1.0");

  commander.program
    .option("-m, --module <char>")
    .option("-i, --iterations <numbers>")
    .option("-c, --config <string>");

  commander.program.parse();
};

export const getOptions = (): ProgramParams => commander.program.opts();
