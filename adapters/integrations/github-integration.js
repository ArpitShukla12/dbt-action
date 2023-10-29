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
} from "../templates/github-integration.js";
import { getNewModelAddedComment, getBaseComment } from "../templates/atlan.js";
import {
  IS_DEV,
  ATLAN_INSTANCE_URL,
  IGNORE_MODEL_ALIAS_MATCHING,
} from "../utils/get-environment-variables.js";

export default class GitHubIntegration extends IntegrationInterface {
  constructor(token) {
    super(token);
  }

  async run() {
    const timeStart = Date.now();
    const { context } = github;
    const octokit = github.getOctokit(this.token);
    const { pull_request } = context?.payload;
    const { state, merged } = pull_request;

    if (!(await this.authIntegration({ octokit, context }))) {
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
  }

  async printDownstreamAssets({ octokit, context }) {
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
        comments += asset.error;
        totalChangedFiles++;
        continue;
      }

      const materialisedAsset = asset.attributes.dbtModelSqlAssets[0];
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
        comment_id: existingComment.id,
      });

    return totalChangedFiles;
  }

  async setResourceOnAsset({ octokit, context }) {
    const changedFiles = await this.getChangedFiles({ octokit, context });
    const { pull_request } = context.payload;
    var totalChangedFiles = 0;

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

      const asset = await getAsset({
        name: assetName,
        sendSegmentEventOfIntegration: this.sendSegmentEventOfIntegration,
        environment: environment,
        integration: "github",
      });

      if (asset.error) {
        continue;
      }

      const materialisedAsset = asset.attributes.dbtModelSqlAssets[0];
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
        console.log(downstreamAssets.error);
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

      const { guid: modelGuid } = asset;
      const { guid: tableAssetGuid } =
        asset?.attributes?.dbtModelSqlAssets?.[0];

      let PR_TITLE = pull_request.title;

      if (downstreamAssets.entityCount != 0) {
        if (modelGuid)
          await createResource(
            modelGuid,
            PR_TITLE,
            pull_request.html_url,
            this.sendSegmentEventOfIntegration
          );

        if (tableAssetGuid)
          await createResource(
            tableAssetGuid,
            PR_TITLE,
            pull_request.html_url,
            this.sendSegmentEventOfIntegration
          );
      }
      totalChangedFiles++;
    }

    const comment = await this.createIssueComment({
      octokit,
      context,
      content: getSetResourceOnAssetComment(),
      comment_id: null,
      forceNewComment: true,
    });

    return totalChangedFiles;
  }

  async authIntegration({ octokit, context }) {
    const response = await auth();

    const existingComment = await this.checkCommentExists({ octokit, context });

    if (response?.status === 401) {
      await this.createIssueComment({
        octokit,
        context,
        content: getErrorResponseStatus401(ATLAN_INSTANCE_URL, context),
        comment_id: existingComment?.id,
      });
      return false;
    }

    if (response === undefined) {
      await this.createIssueComment({
        octokit,
        context,
        content: getErrorResponseStatusUndefined(ATLAN_INSTANCE_URL, context),
        comment_id: existingComment?.id,
      });
      return false;
    }

    return true;
  }

  async sendSegmentEventOfIntegration({ action, properties }) {
    const domain = new URL(ATLAN_INSTANCE_URL).hostname;

    const raw = stringify({
      category: "integration",
      object: "github",
      action,
      userId: "atlan-annonymous-github",
      properties: {
        ...properties,
        //get context for this
        // github_action_id: `https://github.com/${context.payload.repository.full_name}/actions/runs/${context.runId}`,
        domain,
      },
    });

    return sendSegmentEvent(action, raw);
  }

  async getChangedFiles({ octokit, context }) {
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

    return changedFiles;
  }

  async getAssetName({ octokit, context, fileName, filePath }) {
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
        return matches[1].trim();
      }
    }

    return fileName;
  }

  async getFileContents({ octokit, context, filePath }) {
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
        console.log("Error fetching file contents: ", e);
        return null;
      });

    if (!res) return null;

    const buff = Buffer.from(res.data.content, "base64");

    return buff.toString("utf8");
  }

  async checkCommentExists({ octokit, context }) {
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
