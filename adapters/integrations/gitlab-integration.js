// gitlabIntegration.js
import dotenv from "dotenv";

import IntegrationInterface from "./contract/contract.js";
import { Gitlab } from "@gitbeaker/rest";
import {
  createResource,
  getAsset,
  getDownstreamAssets,
  sendSegmentEvent,
  getClassifications,
} from "../api/index.js";
import {
  getImageURL,
  auth,
  getConnectorImage,
  getCertificationImage,
  getGitLabEnvironments,
  truncate,
} from "../utils/index.js";
import stringify from "json-stringify-safe";
import {
  getSetResourceOnAssetComment,
  getErrorResponseStatus401,
  getErrorResponseStatusUndefined,
  getAssetInfo,
  getDownstreamTable,
  getViewAssetButton,
} from "../templates/gitlab-integration.js";
import { getNewModelAddedComment, getBaseComment } from "../templates/atlan.js";
import {
  IS_DEV,
  ATLAN_INSTANCE_URL,
  CI_PROJECT_PATH,
  CI_MERGE_REQUEST_IID,
  CI_PROJECT_ID,
  CI_JOB_URL,
  IS_IGNORE_MODEL_ALIAS_MATCHING,
  CI_COMMIT_MESSAGE,
  GITLAB_USER_LOGIN,
  CI_PROJECT_NAME,
} from "../environmentVariables/gitlab-integration.js";
dotenv.config();
// const ATLAN_INSTANCE_URL = process.env.ATLAN_INSTANCE_URL;
// const { IS_DEV, IS_IGNORE_MODEL_ALIAS_MATCHING } = process.env;

export default class GitLabIntegration extends IntegrationInterface {
  constructor(token) {
    super(token);
  }

  async run() {
    const timeStart = Date.now();

    const gitlab = new Gitlab({
      host: "https://gitlab.com",
      token: this.token,
    });

    // const { CI_PROJECT_PATH, CI_MERGE_REQUEST_IID } = process.env;

    if (!(await this.authIntegration({ gitlab })))
      throw { message: "Wrong API Token" };

    const { state, web_url, target_branch, diff_refs } =
      await gitlab.MergeRequests.show(CI_PROJECT_PATH, CI_MERGE_REQUEST_IID);

    let total_assets = 0;
    if (state === "opened") {
      total_assets = await this.printDownstreamAssets({
        gitlab,
        target_branch,
        diff_refs,
      });
    } else if (state === "merged") {
      total_assets = await this.setResourceOnAsset({
        gitlab,
        web_url,
        target_branch,
        diff_refs,
      });
    }

    if (total_assets !== 0)
      this.sendSegmentEventOfIntegration({
        action: "dbt_ci_action_run",
        properties: {
          asset_count: total_assets,
          total_time: Date.now() - timeStart,
        },
      });
  }

  async printDownstreamAssets({ gitlab, target_branch, diff_refs }) {
    const changedFiles = await this.getChangedFiles({ gitlab, diff_refs });

    let comments = ``;
    let totalChangedFiles = 0;

    for (const { fileName, filePath, headSHA, status } of changedFiles) {
      const aliasName = await this.getAssetName({
        gitlab,
        fileName,
        filePath,
        headSHA,
      });
      const assetName = IS_IGNORE_MODEL_ALIAS_MATCHING ? fileName : aliasName;

      // const environments = getGitLabEnvironments();
      let environment = null;
      // for (const baseBranchName of Object.keys(environments)) {
      //   const environmentName = environments[baseBranchName];
      //   if (baseBranchName === target_branch) {
      //     environment = environmentName;
      //     break;
      //   }
      // }

      const asset = await getAsset({
        name: assetName,
        sendSegmentEventOfIntegration: this.sendSegmentEventOfIntegration,
        environment: environment,
        integration: "gitlab",
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

      const { guid } = asset;

      const downstreamAssets = await getDownstreamAssets(
        asset,
        materialisedAsset.guid,
        totalModifiedFiles,
        this.sendSegmentEventOfIntegration,
        "gitlab"
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
        asset,
        downstreamAssets,
        classifications,
        materialisedAsset,
      });

      comments += comment;

      totalChangedFiles++;
    }

    comments = getBaseComment(totalChangedFiles, comments);

    const existingComment = await this.checkCommentExists({ gitlab });

    if (totalChangedFiles > 0)
      await this.createIssueComment({
        gitlab,
        content: comments,
        comment_id: existingComment?.id,
      });

    if (totalChangedFiles === 0 && existingComment)
      await this.deleteComment({ gitlab, comment_id: existingComment.id });

    return totalChangedFiles;
  }

  async setResourceOnAsset({ gitlab, web_url, target_branch, diff_refs }) {
    const changedFiles = await this.getChangedFiles({ gitlab, diff_refs });

    var totalChangedFiles = 0;
    if (changedFiles.length === 0) return;

    for (const { fileName, filePath, headSHA } of changedFiles) {
      const aliasName = await this.getAssetName({
        gitlab,
        fileName,
        filePath,
        headSHA,
      });

      const assetName = IS_IGNORE_MODEL_ALIAS_MATCHING ? fileName : aliasName;

      // const environments = getGitLabEnvironments();
      let environment = null;
      // for (const [baseBranchName, environmentName] of environments) {
      //   if (baseBranchName === target_branch) {
      //     environment = environmentName;
      //     break;
      //   }
      // }

      const asset = await getAsset({
        name: assetName,
        sendSegmentEventOfIntegration: this.sendSegmentEventOfIntegration,
        environment: environment,
        integration: "gitlab",
      });

      if (asset.error) continue;

      const materialisedAsset = asset.attributes.dbtModelSqlAssets[0];
      const timeStart = Date.now();

      const totalModifiedFiles = changedFiles.filter(
        (i) => i.status === "modified"
      ).length;

      const { guid } = asset;

      const downstreamAssets = await getDownstreamAssets(
        asset,
        materialisedAsset.guid,
        totalModifiedFiles,
        this.sendSegmentEventOfIntegration,
        "gitlab"
      );

      if (downstreamAssets.error) {
        console.log(downstreamAssets.error);
        continue;
      }

      const { guid: modelGuid } = asset;
      const { guid: tableAssetGuid } = asset.attributes.dbtModelSqlAssets[0];

      // var { CI_COMMIT_MESSAGE } = process.env;
      var lines = CI_COMMIT_MESSAGE.split("\n");
      var CI_MERGE_REQUEST_TITLE = lines[2];

      if (downstreamAssets.entityCount != 0) {
        await createResource(
          modelGuid,
          CI_MERGE_REQUEST_TITLE,
          web_url,
          this.sendSegmentEventOfIntegration
        );
        await createResource(
          tableAssetGuid,
          CI_MERGE_REQUEST_TITLE,
          web_url,
          this.sendSegmentEventOfIntegration
        );
      }

      totalChangedFiles++;
    }

    const comment = await this.createIssueComment({
      gitlab,
      content: getSetResourceOnAssetComment(),
      comment_id: null,
      forceNewComment: true,
    });

    return totalChangedFiles;
  }

  async authIntegration({ gitlab }) {
    const response = await auth();
    // var { CI_PROJECT_NAME, GITLAB_USER_LOGIN } = process.env;
    const existingComment = await this.checkCommentExists({ gitlab });
    if (response?.status === 401) {
      await this.createIssueComment({
        gitlab,
        content: getErrorResponseStatus401(
          ATLAN_INSTANCE_URL,
          CI_PROJECT_NAME,
          GITLAB_USER_LOGIN
        ),
        comment_id: existingComment?.id,
      });
      return false;
    }

    if (response === undefined) {
      await this.createIssueComment({
        gitlab,
        content: getErrorResponseStatusUndefined(
          ATLAN_INSTANCE_URL,
          CI_PROJECT_NAME,
          GITLAB_USER_LOGIN
        ),
        comment_id: existingComment?.id,
      });
      return false;
    }

    return true;
  }

  async createIssueComment({
    gitlab,
    content,
    comment_id = null,
    forceNewComment = false,
  }) {
    // const { CI_PROJECT_PATH, CI_MERGE_REQUEST_IID, CI_PROJECT_ID } =
    //   process.env;

    content = `<!-- ActionCommentIdentifier: atlan-dbt-action -->
${content}`;

    if (IS_DEV) return content;

    if (comment_id && !forceNewComment) {
      return await gitlab.MergeRequestNotes.edit(
        CI_PROJECT_ID,
        CI_MERGE_REQUEST_IID,
        comment_id,
        {
          body: content,
        }
      );
    }
    return await gitlab.MergeRequestNotes.create(
      CI_PROJECT_PATH,
      CI_MERGE_REQUEST_IID,
      content
    );
  }

  async sendSegmentEventOfIntegration({ action, properties }) {
    const domain = new URL(ATLAN_INSTANCE_URL).hostname;
    // const { CI_PROJECT_PATH, CI_JOB_URL } = process.env;

    const raw = stringify({
      category: "integration",
      object: "gitlab",
      action,
      userId: "atlan-annonymous-github",
      properties: {
        ...properties,
        gitlab_job_id: CI_JOB_URL,
        domain,
      },
    });

    return sendSegmentEvent(action, raw);
  }

  async getChangedFiles({ gitlab, diff_refs }) {
    // const { CI_PROJECT_PATH, CI_MERGE_REQUEST_IID } = process.env;

    var changes = await gitlab.MergeRequests.allDiffs(
      CI_PROJECT_PATH,
      CI_MERGE_REQUEST_IID
    );

    var changedFiles = changes
      .map(({ new_path, old_path, new_file }) => {
        try {
          const [modelName] = new_path
            .match(/.*models\/(.*)\.sql/)[1]
            .split("/")
            .reverse()[0]
            .split(".");

          if (modelName) {
            if (new_file) {
              return {
                fileName: modelName,
                filePath: new_path,
                headSHA: diff_refs.head_sha,
                status: "added",
              };
            } else if (new_path !== old_path) {
              // File is renamed or moved
              return {
                fileName: modelName,
                filePath: new_path,
                headSHA: diff_refs.head_sha,
                status: "renamed_or_moved",
              };
            } else {
              // File is modified
              return {
                fileName: modelName,
                filePath: new_path,
                headSHA: diff_refs.head_sha,
                status: "modified",
              };
            }
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

  async getAssetName({ gitlab, fileName, filePath, headSHA }) {
    var regExp =
      /{{\s*config\s*\(\s*(?:[^,]*,)*\s*alias\s*=\s*['"]([^'"]+)['"](?:\s*,[^,]*)*\s*\)\s*}}/im; //Changed
    var fileContents = await this.getFileContents({
      gitlab,
      filePath,
      headSHA,
    });

    var matches = regExp.exec(fileContents);

    if (matches) {
      return matches[1];
    }

    return fileName;
  }

  async getFileContents({ gitlab, filePath, headSHA }) {
    // const { CI_PROJECT_PATH } = process.env;
    const { content } = await gitlab.RepositoryFiles.show(
      CI_PROJECT_PATH,
      filePath,
      headSHA
    );
    const buff = Buffer.from(content, "base64");

    return buff.toString("utf8");
  }

  async checkCommentExists({ gitlab }) {
    // const { CI_PROJECT_PATH, CI_MERGE_REQUEST_IID, CI_PROJECT_ID } =
    //   process.env;

    if (IS_DEV) return null;

    const comments = await gitlab.MergeRequestNotes.all(
      CI_PROJECT_PATH,
      CI_MERGE_REQUEST_IID
    );

    const identifier = `project_${CI_PROJECT_ID}_bot_`;

    return comments.find(
      (comment) =>
        comment.author.username.includes(identifier) &&
        comment.body.includes(
          "<!-- ActionCommentIdentifier: atlan-dbt-action -->"
        )
    );
  }

  async deleteComment({ gitlab, comment_id }) {
    // const { CI_PROJECT_PATH, CI_MERGE_REQUEST_IID } = process.env;

    return await gitlab.MergeRequestNotes.remove(
      CI_PROJECT_PATH,
      CI_MERGE_REQUEST_IID,
      comment_id
    );
  }

  async renderDownstreamAssetsComment({
    asset,
    downstreamAssets,
    classifications,
    materialisedAsset,
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
                `[${displayText}](${ATLAN_INSTANCE_URL}/assets/${termGuid}/overview?utm_source=dbt_gitlab_action)`
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
          `${connectorImage} [${displayText}](${ATLAN_INSTANCE_URL}/assets/${guid}/overview?utm_source=dbt_gitlab_action) ${certificationImage}`,
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
