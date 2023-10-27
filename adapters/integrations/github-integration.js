// githubIntegration.js
import dotenv from "dotenv"; // Check do we actually need it or not
import IntegrationInterface from "./contract/contract.js";
import github from "@actions/github";
import stringify from "json-stringify-safe";
import {
  getCertificationImage,
  getConnectorImage,
  isIgnoreModelAliasMatching,
  getEnvironments,
  getImageURL,
  auth,
  isDev,
  truncate,
  getInstanceUrl,
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
const IS_DEV = isDev();
const ATLAN_INSTANCE_URL = getInstanceUrl();

dotenv.config();

export default class GitHubIntegration extends IntegrationInterface {
  constructor(token) {
    super(token);
  }

  async run() {
    const timeStart = Date.now();
    const { context } = github;
    console.log("Hmm");
    console.log("Interesting over here");
    console.log("Lets seeee");
    console.log("Lets see again");
    console.log("Context:", context);
    const octokit = github.getOctokit(this.token);
    const { pull_request } = context?.payload;
    console.log(pull_request, "hii");
    const { state, merged } = pull_request;
    console.log("state", state);
    console.log("merged", merged);
    if (!(await this.authIntegration({ octokit, context }))) {
      throw { message: "Wrong API Token" };
    }

    let total_assets = 0;
    console.log("After auth Integration");
    if (state === "open") {
      total_assets = await this.printDownstreamAssets({ octokit, context });
    } else if (state === "closed" && merged) {
      console.log("Hmm");
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
    console.log("Brother");
    const changedFiles = await this.getChangedFiles({ octokit, context }); //Complete
    let comments = ``;
    let totalChangedFiles = 0;

    for (const { fileName, filePath, status } of changedFiles) {
      const aliasName = await this.getAssetName({
        //Complete
        octokit,
        context,
        fileName,
        filePath,
      });
      const assetName = isIgnoreModelAliasMatching() ? fileName : aliasName; //Complete
      console.log("acha2", aliasName);
      // const environments = getEnvironments();

      let environment = null;
      // for (const [baseBranchName, environmentName] of environments) {
      //   if (baseBranchName === context.payload.pull_request.base.ref) {
      //     environment = environmentName;
      //     break;
      //   }
      // }
      console.log("Before getAsset");
      const asset = await getAsset({
        //Done
        name: assetName,
        sendSegmentEventOfIntegration: this.sendSegmentEventOfIntegration,
        environment: environment,
        integration: "github",
      });
      console.log("After getAsset");
      if (totalChangedFiles !== 0) comments += "\n\n---\n\n";
      console.log("Status: ", status);
      if (status === "added") {
        comments += getNewModelAddedComment(fileName);
        totalChangedFiles++;
        continue;
      }
      console.log("Before filtering");
      console.log("Asset", asset);
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
      console.log("Before getDownstreamAssets");
      const downstreamAssets = await getDownstreamAssets(
        //Complete
        asset,
        materialisedAsset.guid,
        totalModifiedFiles,
        this.sendSegmentEventOfIntegration,
        "github"
      );
      console.log("After getDownstreamAssets");
      if (downstreamAssets.error) {
        comments += downstreamAssets.error;
        totalChangedFiles++;
        continue;
      }
      console.log("At line 139 after getDownstreamAssets in printDownstream");
      this.sendSegmentEventOfIntegration({
        action: "dbt_ci_action_downstream_unfurl",
        properties: {
          asset_guid: asset.guid,
          asset_type: asset.typeName,
          downstream_count: downstreamAssets.entities.length,
          total_fetch_time: Date.now() - timeStart,
        },
      });
      console.log("At line 147 after getDownstreamAssets in printDownstream");

      const classifications = await getClassifications({
        sendSegmentEventOfIntegration: this.sendSegmentEventOfIntegration,
      });

      const comment = await this.renderDownstreamAssetsComment({
        //Done
        //Complete
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

    const existingComment = await this.checkCommentExists({ octokit, context }); //Complete

    if (totalChangedFiles > 0)
      await this.createIssueComment({
        //Complete
        octokit,
        context,
        content: comments,
        comment_id: existingComment?.id,
      });

    if (totalChangedFiles === 0 && existingComment)
      await this.deleteComment({
        //Complete
        octokit,
        context,
        comment_id: existingComment.id,
      });

    return totalChangedFiles;
  }

  async setResourceOnAsset({ octokit, context }) {
    console.log("At line 205 inside SRA");
    const changedFiles = await this.getChangedFiles({ octokit, context }); //Complete
    const { pull_request } = context.payload;
    var totalChangedFiles = 0;
    console.log("At line 209 inside SRA", changedFiles);
    if (changedFiles.length === 0) return;

    for (const { fileName, filePath } of changedFiles) {
      const assetName = await this.getAssetName({
        //Complete
        octokit,
        context,
        fileName,
        filePath,
      });
      console.log("OII", assetName);
      // const environments = getEnvironments();

      let environment = null;
      // for (const [baseBranchName, environmentName] of environments) {
      //   if (baseBranchName === context.payload.pull_request.base.ref) {
      //     environment = environmentName;
      //     break;
      //   }
      // }

      const asset = await getAsset({
        //Done
        name: assetName,
        sendSegmentEventOfIntegration: this.sendSegmentEventOfIntegration,
        environment: environment,
        integration: "github",
      });

      if (asset.error) {
        console.log("Wuuuuuttt");
        continue;
      }
      console.log("At line 240 inside SRA", asset);
      const { guid: modelGuid } = asset;
      const { guid: tableAssetGuid } =
        asset?.attributes?.dbtModelSqlAssets?.[0];
      console.log("At line 244 inside SRA", modelGuid, tableAssetGuid);
      if (modelGuid)
        await createResource(
          //Complete
          modelGuid,
          "Pull Request on GitHub",
          pull_request.html_url,
          this.sendSegmentEventOfIntegration
        );
      console.log("At line 253 inside SRA");
      if (tableAssetGuid)
        await createResource(
          //Complete
          tableAssetGuid,
          "Pull Request on GitHub",
          pull_request.html_url,
          this.sendSegmentEventOfIntegration
        );
      console.log("At line 262 inside SRA");
      totalChangedFiles++;
    }

    const comment = await this.createIssueComment({
      //Complete
      octokit,
      context,
      content: getSetResourceOnAssetComment(),
      comment_id: null,
      forceNewComment: true,
    });
    console.log("Done");
    return totalChangedFiles;
  }

  async authIntegration({ octokit, context }) {
    //DONE
    //COMPLETE
    console.log("Here is Context:", context.repo);
    const response = await auth();
    console.log("Inside authIntegration befor comment exists");
    const existingComment = await this.checkCommentExists({ octokit, context });

    console.log("Existing Comment", existingComment);

    if (response?.status === 401) {
      console.log("Inside authIntegration befor createIssueComment");
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
    //Done
    //FullyComplete
    // IMPORT ATLAN_INSTANCE_URL.
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
    //Done
    //FullyComplete
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

    console.log("Changed Files: ", changedFiles);

    return changedFiles;
  }

  async getAssetName({ octokit, context, fileName, filePath }) {
    //Done
    // FullyComplete
    var regExp =
      /{{\s*config\s*\(\s*(?:[^,]*,)*\s*alias\s*=\s*['"]([^'"]+)['"](?:\s*,[^,]*)*\s*\)\s*}}/im;
    var fileContents = await this.getFileContents({
      octokit,
      context,
      filePath,
    });
    console.log("Here it is: ", fileContents);
    if (fileContents) {
      var matches = regExp.exec(fileContents);
      console.log(matches);
      if (matches) {
        return matches[1].trim();
      }
    }

    return fileName;
  }

  async getFileContents({ octokit, context, filePath }) {
    //Done
    // FullyComplete
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
    //Done
    //FullyComplete
    if (IS_DEV) return null;

    const { pull_request } = context.payload;
    console.log(pull_request);
    console.log("Here: ", context.repo);
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
    //Done
    // FullyComplete
    octokit,
    context,
    content,
    comment_id = null,
    forceNewComment = false,
  }) {
    console.log("Inside CreateIssue:", context);
    console.log("Inside CreateIssue Comment");
    const { pull_request } = context?.payload || {};
    console.log("Inside CreateIssue Comment");
    content = `<!-- ActionCommentIdentifier: atlan-dbt-action -->
${content}`;

    const commentObj = {
      ...context.repo,
      issue_number: pull_request.number,
      body: content,
    };

    console.log(content, content.length);
    console.log("Inside CreateIssue Comment Complete");

    if (IS_DEV) return content;

    if (comment_id && !forceNewComment)
      return octokit.rest.issues.updateComment({ ...commentObj, comment_id });
    return octokit.rest.issues.createComment(commentObj);
  }

  async deleteComment({ octokit, context, comment_id }) {
    //Done
    //FullyComplete
    const { pull_request } = context.payload;

    return octokit.rest.issues.deleteComment({
      ...context.repo,
      issue_number: pull_request.number,
      comment_id,
    });
  }

  async renderDownstreamAssetsComment({
    //Done
    //FullyComplete
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
    console.log("Hmmmm", asset.guid);
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
