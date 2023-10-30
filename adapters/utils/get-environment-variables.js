import dotenv from "dotenv";
import core from "@actions/core";
dotenv.config();

//Common env variables
export const ATLAN_INSTANCE_URL = new URL(
  process.env.ATLAN_INSTANCE_URL || core.getInput("ATLAN_INSTANCE_URL")
).origin;

export const ATLAN_API_TOKEN =
  process.env.ATLAN_API_TOKEN || core.getInput("ATLAN_API_TOKEN");

export const IS_DEV = process.env.IS_DEV;

export const IGNORE_MODEL_ALIAS_MATCHING =
  (process.env.IGNORE_MODEL_ALIAS_MATCHING ||
    core.getInput("IGNORE_MODEL_ALIAS_MATCHING")) == "true";

//GITLAB SPECIFIC ENV VARIABLES
export function getCIMergeRequestIID(gitlab, CI_PROJECT_ID, CI_COMMIT_SHA) {
  if (!process.env.CI_MERGE_REQUEST_IID) {
    const mergeRequestCommit = gitlab.Commits.allMergeRequests(
      CI_PROJECT_ID,
      CI_COMMIT_SHA
    );
    console.log("CI_PROJECT_ID", CI_PROJECT_ID);
    console.log("CI_COMMIT_SHA", CI_COMMIT_SHA);
    console.log("Merge Request Commit :", mergeRequestCommit);
    const firstMergeRequest = mergeRequestCommit[0];
    if (firstMergeRequest) {
      console.log(firstMergeRequest.iid); // Log the IID for reference
      return firstMergeRequest.iid;
    }
  }
  console.log("Wut");
  return process.env.CI_MERGE_REQUEST_IID;
}

export const {
  CI_PROJECT_PATH,
  CI_PROJECT_ID,
  CI_JOB_URL,
  GITLAB_TOKEN,
  CI_COMMIT_MESSAGE,
  GITLAB_USER_LOGIN,
  CI_PROJECT_NAME,
  CI_COMMIT_SHA,
} = process.env;

export function getGitLabEnvironments() {
  const { DBT_ENVIRONMENT_BRANCH_MAP } = process.env;

  if (DBT_ENVIRONMENT_BRANCH_MAP) {
    const environmentLines = DBT_ENVIRONMENT_BRANCH_MAP.split("\n");
    const environmentMap = {};

    environmentLines.forEach((line) => {
      const [environment, branch] = line.split(":").map((item) => item.trim());
      if (environment && branch) {
        environmentMap[environment] = branch;
      }
    });

    return environmentMap;
  } else {
    return {};
  }
}

//GITHUB SPECIFIC ENV VARIABLES
export const GITHUB_TOKEN =
  core.getInput("GITHUB_TOKEN") || process.env.GITHUB_TOKEN;

export const getEnvironments = () => {
  return (
    core
      .getInput("DBT_ENVIRONMENT_BRANCH_MAP")
      ?.trim()
      ?.split("\n")
      ?.map((i) => i.split(":").map((i) => i.trim())) ?? []
  );
};
