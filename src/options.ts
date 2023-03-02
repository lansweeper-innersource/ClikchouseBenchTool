import { program } from "https://esm.sh/commander@10.0.0";

export interface ProgramParams {
  module?: string;
  iterations?: number;
}

export const initProgram = () => {
  program
    .name("Clickhouse benchmarking tool")
    .description("CLI tool to test, analyse and compare clickhouse queries")
    .version("0.1.0");

  program.option("-m, --module <char>").option("-i, --iterations <numbers>");

  program.parse();
};

export const getOptions = (): ProgramParams => program.opts();
