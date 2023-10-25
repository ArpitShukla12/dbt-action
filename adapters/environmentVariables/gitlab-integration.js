import dotenv from "dotenv";

dotenv.config();

const {
  IS_DEV,
  ATLAN_INSTANCE_URL,
  CI_PROJECT_PATH,
  CI_MERGE_REQUEST_IID,
  CI_PROJECT_ID,
  CI_JOB_URL,
  ATLAN_API_TOKEN,
  IGNORE_MODEL_ALIAS_MATCHING,
  GITLAB_TOKEN,
} = process.env;
