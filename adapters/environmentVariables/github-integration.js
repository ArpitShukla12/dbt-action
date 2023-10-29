import dotenv from "dotenv";
import core from "@actions/core";

dotenv.config();

const { GITHUB_TOKEN } =
  core.getInput("GITHUB_TOKEN") || process.env.GITHUB_TOKEN;

export { GITHUB_TOKEN };
