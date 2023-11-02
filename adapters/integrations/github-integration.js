// githubIntegration.js
import IntegrationInterface from "./contract/contract.js";
import github from "@actions/github";
import stringify from "json-stringify-safe";
import {
  getCertificationImage,
  getConnectorImage,
  getEnvironments,
  auth,
  truncate,
} from "../utils/index.js";
import {
  getAsset,
  getDownstreamAssets,
  sendSegmentEvent,
  createResource,
  getClassifications,
} from "../api/index.js";
import {
  getSetResourceOnAssetComment,
  getErrorResponseStatus401,
  getErrorResponseStatusUndefined,
  getAssetInfo,
  getDownstreamTable,
  getViewAssetButton,
  getMDCommentForModel,
  getMDCommentForMaterialisedView,
  getTableMD,
} from "../templates/github-integration.js";
import { getNewModelAddedComment, getBaseComment } from "../templates/atlan.js";
import {
  IS_DEV,
  ATLAN_INSTANCE_URL,
  IGNORE_MODEL_ALIAS_MATCHING,
} from "../utils/get-environment-variables.js";
import logger from "../logger/logger.js";
export default class GitHubIntegration extends IntegrationInterface {
  constructor(token) {
    super(token);
  }

  async run() {
    logger.logInfo("Starting the GitHub integration...");

    const timeStart = Date.now();
    const { context } = github;
    console.log(context);
    const octokit = github.getOctokit(this.token);
    const { pull_request } = context?.payload;
    const { state, merged } = pull_request;
    console.log(pull_request?.head);

    logger.logDebug(`Current state: ${state}, merged: ${merged}`);

    if (!(await this.authIntegration({ octokit, context }))) {
      logger.logError("Authentication failed. Wrong API Token.");
      throw { message: "Wrong API Token" };
    }

    let total_assets = 0;

    if (state === "open") {
      total_assets = await this.printDownstreamAssets({ octokit, context });
    } else if (state === "closed" && merged) {
      total_assets = await this.setResourceOnAsset({ octokit, context });
    }

    if (total_assets !== 0) {
      this.sendSegmentEventOfIntegration({
        action: "dbt_ci_action_run",
        properties: {
          asset_count: total_assets,
          total_time: Date.now() - timeStart,
        },
      });
    }

    logger.logInfo("Successfully Completed DBT_CI_ACTION");
  }

  async printDownstreamAssets({ octokit, context }) {
    logger.logInfo("Printing downstream assets...");

    const changedFiles = await this.getChangedFiles({ octokit, context });
    let comments = ``;
    let totalChangedFiles = 0;

    for (const { fileName, filePath, status } of changedFiles) {
      const aliasName = await this.getAssetName({
        octokit,
        context,
        fileName,
        filePath,
      });
      const assetName = IGNORE_MODEL_ALIAS_MATCHING ? fileName : aliasName;

      const environments = getEnvironments();
      let environment = null;
      for (const [baseBranchName, environmentName] of environments) {
        if (baseBranchName === context.payload.pull_request.base.ref) {
          environment = environmentName;
          break;
        }
      }

      logger.logDebug(`Processing asset: ${assetName}`);

      const asset = await getAsset({
        name: assetName,
        sendSegmentEventOfIntegration: this.sendSegmentEventOfIntegration,
        environment: environment,
        integration: "github",
      });

      if (totalChangedFiles !== 0) comments += "\n\n---\n\n";

      if (status === "added") {
        comments += getNewModelAddedComment(fileName);
        totalChangedFiles++;
        continue;
      }

      if (asset.error) {
        logger.logError(`Asset error: ${asset.error}`);
        comments += asset.error;
        totalChangedFiles++;
        continue;
      }

      const materialisedAsset = asset?.attributes?.dbtModelSqlAssets?.[0];
      const timeStart = Date.now();

      const totalModifiedFiles = changedFiles.filter(
        (i) => i.status === "modified"
      ).length;

      const downstreamAssets = await getDownstreamAssets(
        asset,
        materialisedAsset.guid,
        totalModifiedFiles,
        this.sendSegmentEventOfIntegration,
        "github"
      );

      if (downstreamAssets.error) {
        logger.logError(`Downstream assets error: ${downstreamAssets.error}`);
        comments += downstreamAssets.error;
        totalChangedFiles++;
        continue;
      }

      this.sendSegmentEventOfIntegration({
        action: "dbt_ci_action_downstream_unfurl",
        properties: {
          asset_guid: asset.guid,
          asset_type: asset.typeName,
          downstream_count: downstreamAssets.entities.length,
          total_fetch_time: Date.now() - timeStart,
        },
      });

      const classifications = await getClassifications({
        sendSegmentEventOfIntegration: this.sendSegmentEventOfIntegration,
      });

      const comment = await this.renderDownstreamAssetsComment({
        octokit,
        context,
        asset,
        materialisedAsset,
        downstreamAssets,
        classifications,
      });

      comments += comment;

      totalChangedFiles++;
    }

    comments = getBaseComment(totalChangedFiles, comments);

    const existingComment = await this.checkCommentExists({ octokit, context });

    logger.logDebug(`Existing Comment: ${existingComment?.id}`);

    if (totalChangedFiles > 0)
      await this.createIssueComment({
        octokit,
        context,
        content: comments,
        comment_id: existingComment?.id,
      });

    if (totalChangedFiles === 0 && existingComment)
      await this.deleteComment({
        octokit,
        context,
        comment_id: existingComment?.id,
      });

    logger.logInfo("Successfully printed Downstream Assets");

    return totalChangedFiles;
  }

  async setResourceOnAsset({ octokit, context }) {
    logger.logInfo("Setting resources on assets...");

    const changedFiles = await this.getChangedFiles({ octokit, context });
    const { pull_request } = context.payload;
    var totalChangedFiles = 0;
    let tableMd = ``;
    let setResourceFailed = false;

    if (changedFiles.length === 0) return;

    for (const { fileName, filePath } of changedFiles) {
      const aliasName = await this.getAssetName({
        octokit,
        context,
        fileName,
        filePath,
      });

      const assetName = IGNORE_MODEL_ALIAS_MATCHING ? fileName : aliasName;

      const environments = getEnvironments();
      let environment = null;
      for (const [baseBranchName, environmentName] of environments) {
        if (baseBranchName === context.payload.pull_request.base.ref) {
          environment = environmentName;
          break;
        }
      }

      logger.logDebug(`Processing asset: ${assetName}`);

      const asset = await getAsset({
        name: assetName,
        sendSegmentEventOfIntegration: this.sendSegmentEventOfIntegration,
        environment: environment,
        integration: "github",
      });

      if (asset.error) {
        logger.logError(`Asset error: ${asset.error}`);
        continue;
      }

      const materialisedAsset = asset?.attributes?.dbtModelSqlAssets?.[0];
      const timeStart = Date.now();

      const totalModifiedFiles = changedFiles.filter(
        (i) => i.status === "modified"
      ).length;

      const downstreamAssets = await getDownstreamAssets(
        asset,
        materialisedAsset.guid,
        totalModifiedFiles,
        this.sendSegmentEventOfIntegration,
        "github"
      );

      if (downstreamAssets.error) {
        logger.logError(`Downstream assets error: ${downstreamAssets.error}`);
        continue;
      }

      this.sendSegmentEventOfIntegration({
        action: "dbt_ci_action_downstream_unfurl",
        properties: {
          asset_guid: asset.guid,
          asset_type: asset.typeName,
          downstream_count: downstreamAssets.entities.length,
          total_fetch_time: Date.now() - timeStart,
        },
      });

      const model = asset;
      const materialisedView = asset?.attributes?.dbtModelSqlAssets?.[0];

      let PR_TITLE = pull_request.title;

      if (downstreamAssets.entityCount != 0) {
        if (model) {
          const { guid: modelGuid } = model;
          const resp = await createResource(
            modelGuid,
            PR_TITLE,
            pull_request.html_url,
            this.sendSegmentEventOfIntegration
          );
          const md = getMDCommentForModel(ATLAN_INSTANCE_URL, model);
          tableMd += getTableMD(md, resp);
          if (!resp) setResourceFailed = true;
        }

        if (materialisedView) {
          const { guid: tableAssetGuid } = materialisedView;
          const resp = await createResource(
            tableAssetGuid,
            PR_TITLE,
            pull_request.html_url,
            this.sendSegmentEventOfIntegration
          );
          const md = getMDCommentForMaterialisedView(
            ATLAN_INSTANCE_URL,
            materialisedView
          );
          tableMd += getTableMD(md, resp);
          if (!resp) setResourceFailed = true;
        }
      }
      totalChangedFiles++;
    }

    const comment = await this.createIssueComment({
      octokit,
      context,
      content: getSetResourceOnAssetComment(tableMd, setResourceFailed),
      comment_id: null,
      forceNewComment: true,
    });

    logger.logInfo("Successfully set the resource on the asset");

    return totalChangedFiles;
  }

  async authIntegration({ octokit, context }) {
    logger.logInfo("Authenticating with atlan....");

    const response = await auth();

    const existingComment = await this.checkCommentExists({ octokit, context });

    logger.logDebug(`Existing Comment: ${existingComment?.id}`);

    if (response?.status === 401) {
      logger.logError("Authentication failed: Status 401");
      await this.createIssueComment({
        octokit,
        context,
        content: getErrorResponseStatus401(ATLAN_INSTANCE_URL, context),
        comment_id: existingComment?.id,
      });
      return false;
    }

    if (response === undefined) {
      logger.logError("Authentication failed: Undefined response");
      await this.createIssueComment({
        octokit,
        context,
        content: getErrorResponseStatusUndefined(ATLAN_INSTANCE_URL, context),
        comment_id: existingComment?.id,
      });
      return false;
    }
    logger.logInfo("Successfully Authenticated with Atlan");
    return true;
  }

  async sendSegmentEventOfIntegration({ action, properties }) {
    const domain = new URL(ATLAN_INSTANCE_URL).hostname;
    const { context } = github; //confirm this
    const raw = stringify({
      category: "integration",
      object: "github",
      action,
      userId: "atlan-annonymous-github",
      properties: {
        ...properties,
        github_action_id: `https://github.com/${context?.payload?.repository?.full_name}/actions/runs/${context?.runId}`,
        domain,
      },
    });

    return sendSegmentEvent(action, raw);
  }

  async getChangedFiles({ octokit, context }) {
    logger.logInfo("Fetching changed files...");

    const { repository, pull_request } = context.payload,
      owner = repository.owner.login,
      repo = repository.name,
      pull_number = pull_request.number;

    const res = await octokit.request(
      `GET /repos/${owner}/${repo}/pulls/${pull_number}/files`,
      {
        owner,
        repo,
        pull_number,
      }
    );

    var changedFiles = res.data
      .map(({ filename, status }) => {
        try {
          const [modelName] = filename
            .match(/.*models\/(.*)\.sql/)[1]
            .split("/")
            .reverse()[0]
            .split(".");

          if (modelName) {
            return {
              fileName: modelName,
              filePath: filename,
              status,
            };
          }
        } catch (e) {}
      })
      .filter((i) => i !== undefined);

    changedFiles = changedFiles.filter((item, index) => {
      return (
        changedFiles.findIndex((obj) => obj.fileName === item.fileName) ===
        index
      );
    });

    logger.logInfo("Successfully fetched changed files");

    return changedFiles;
  }

  async getAssetName({ octokit, context, fileName, filePath }) {
    logger.logInfo("Getting asset name...");

    var regExp =
      /{{\s*config\s*\(\s*(?:[^,]*,)*\s*alias\s*=\s*['"]([^'"]+)['"](?:\s*,[^,]*)*\s*\)\s*}}/im;
    var fileContents = await this.getFileContents({
      octokit,
      context,
      filePath,
    });

    if (fileContents) {
      var matches = regExp.exec(fileContents);
      if (matches) {
        logger.logDebug(`Matched alias name: ${matches[1].trim()}`);
        return matches[1].trim();
      }
    }

    logger.logDebug(`Using filename as asset name: ${fileName}`);

    return fileName;
  }

  async getFileContents({ octokit, context, filePath }) {
    logger.logInfo("Fetching file contents...");

    const { repository, pull_request } = context.payload,
      owner = repository.owner.login,
      repo = repository.name,
      head_sha = pull_request.head.sha;

    const res = await octokit
      .request(
        `GET /repos/${owner}/${repo}/contents/${filePath}?ref=${head_sha}`,
        {
          owner,
          repo,
          path: filePath,
        }
      )
      .catch((e) => {
        logger.logError(`Error fetching file contents: ${e}`);
        return null;
      });

    if (!res) return null;

    const buff = Buffer.from(res.data.content, "base64");

    logger.logInfo("Successfully fetched file contents");

    return buff.toString("utf8");
  }

  async checkCommentExists({ octokit, context }) {
    logger.logInfo("Checking for existing comments...");

    if (IS_DEV) return null;

    const { pull_request } = context.payload;

    const comments = await octokit.rest.issues.listComments({
      ...context.repo,
      issue_number: pull_request.number,
    });

    return comments.data.find(
      (comment) =>
        comment.user.login === "github-actions[bot]" &&
        comment.body.includes(
          "<!-- ActionCommentIdentifier: atlan-dbt-action -->"
        )
    );
  }

  async createIssueComment({
    octokit,
    context,
    content,
    comment_id = null,
    forceNewComment = false,
  }) {
    logger.logInfo("Creating an issue comment...");
    const { pull_request } = context?.payload || {};

    content = `<!-- ActionCommentIdentifier: atlan-dbt-action -->
${content}`;

    const commentObj = {
      ...context.repo,
      issue_number: pull_request.number,
      body: content,
    };

    if (IS_DEV) return content;

    if (comment_id && !forceNewComment)
      return octokit.rest.issues.updateComment({ ...commentObj, comment_id });
    return octokit.rest.issues.createComment(commentObj);
  }

  async deleteComment({ octokit, context, comment_id }) {
    logger.logInfo("Deleting the comment...");

    const { pull_request } = context.payload;

    return octokit.rest.issues.deleteComment({
      ...context.repo,
      issue_number: pull_request.number,
      comment_id,
    });
  }

  async renderDownstreamAssetsComment({
    octokit,
    context,
    asset,
    materialisedAsset,
    downstreamAssets,
    classifications,
  }) {
    logger.logInfo("Rendering Downstream Assets...");

    let impactedData = downstreamAssets.entities.map(
      ({
        displayText,
        guid,
        typeName,
        attributes,
        meanings,
        classificationNames,
      }) => {
        // Modifying the typeName and getting the readableTypeName
        let readableTypeName = typeName
          .toLowerCase()
          .replace(attributes.connectorName, "")
          .toUpperCase();

        // Filtering classifications based on classificationNames
        let classificationsObj = classifications.filter(({ name }) =>
          classificationNames.includes(name)
        );

        // Modifying the readableTypeName
        readableTypeName =
          readableTypeName.charAt(0).toUpperCase() +
          readableTypeName.slice(1).toLowerCase();

        return [
          guid,
          truncate(displayText),
          truncate(attributes.connectorName),
          truncate(readableTypeName),
          truncate(
            attributes?.userDescription || attributes?.description || ""
          ),
          attributes?.certificateStatus || "",
          truncate(
            [...attributes?.ownerUsers, ...attributes?.ownerGroups] || []
          ),
          truncate(
            meanings.map(
              ({ displayText, termGuid }) =>
                `[${displayText}](${ATLAN_INSTANCE_URL}/assets/${termGuid}/overview?utm_source=dbt_github_action)`
            )
          ),
          truncate(
            classificationsObj?.map(
              ({ name, displayName }) => `\`${displayName}\``
            )
          ),
          attributes?.sourceURL || "",
        ];
      }
    );

    logger.logDebug(`Impacted data is as follows: ${impactedData}`);

    // Sorting the impactedData first by typeName and then by connectorName
    impactedData = impactedData.sort((a, b) => a[3].localeCompare(b[3]));
    impactedData = impactedData.sort((a, b) => a[2].localeCompare(b[2]));

    // Creating rows for the downstream table
    let rows = impactedData.map(
      ([
        guid,
        displayText,
        connectorName,
        typeName,
        description,
        certificateStatus,
        owners,
        meanings,
        classifications,
        sourceUrl,
      ]) => {
        // Getting connector and certification images
        const connectorImage = getConnectorImage(connectorName);
        const certificationImage = certificateStatus
          ? getCertificationImage(certificateStatus)
          : "";

        return [
          `${connectorImage} [${displayText}](${ATLAN_INSTANCE_URL}/assets/${guid}/overview?utm_source=dbt_github_action) ${certificationImage}`,
          `\`${typeName}\``,
          description,
          owners,
          meanings,
          classifications,
          sourceUrl ? `[Open in ${connectorName}](${sourceUrl})` : " ",
        ];
      }
    );

    const environmentName =
      materialisedAsset?.attributes?.assetDbtEnvironmentName;
    const projectName = materialisedAsset?.attributes?.assetDbtProjectName;
    // Generating asset information

    const assetInfo = getAssetInfo(
      ATLAN_INSTANCE_URL,
      asset,
      materialisedAsset,
      environmentName,
      projectName
    );

    // Generating the downstream table
    const downstreamTable = getDownstreamTable(
      ATLAN_INSTANCE_URL,
      downstreamAssets,
      rows,
      materialisedAsset
    );

    // Generating the "View asset in Atlan" button
    const viewAssetButton = getViewAssetButton(ATLAN_INSTANCE_URL, asset);

    // Generating the final comment based on the presence of downstream assets
    if (downstreamAssets.entities.length > 0) {
      return `${assetInfo}

${downstreamTable}

${viewAssetButton}`;
    } else {
      return `${assetInfo}

No downstream assets found.

${viewAssetButton}`;
    }
  }
}
