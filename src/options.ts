import { commander } from "./deps.ts";

export interface ProgramParams {
  module?: string;
  iterations?: number;
}

export const initProgram = () => {
  commander.program
    .name("Clickhouse benchmarking tool")
    .description("CLI tool to test, analyse and compare clickhouse queries")
    .version("0.1.0");

  commander.program
    .option("-m, --module <char>")
    .option("-i, --iterations <numbers>");

  commander.program.parse();
};

export const getOptions = (): ProgramParams => commander.program.opts();
