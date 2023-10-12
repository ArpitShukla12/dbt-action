// gitlabIntegration.js
import dotenv from "dotenv";

import IntegrationInterface from "./contract/contract.js";
import { Gitlab } from "@gitbeaker/rest";
import {
  createResource,
  getAsset,
  getDownstreamAssets,
  sendSegmentEvent,
} from "../../src/api/index.js";
import { getImageURL, auth } from "../../src/utils/index.js";
import { getGitLabEnvironments } from "../../src/utils/get-environment-variables.js";
import { getConnectorImage } from "../../src/utils/index.js";
import { getCertificationImage } from "../../src/utils/index.js";
import stringify from "json-stringify-safe";

dotenv.config();
const ATLAN_INSTANCE_URL = process.env.ATLAN_INSTANCE_URL;
const { IS_DEV } = process.env;

export default class GitLabIntegration extends IntegrationInterface {
  constructor(token) {
    super(token);
  }

  async run() {
    //Done
    console.log("Run Gitlab");
    const timeStart = Date.now();

    const gitlab = new Gitlab({
      host: "https://gitlab.com",
      token: this.token,
    });

    const { CI_PROJECT_PATH, CI_MERGE_REQUEST_IID } = process.env;

    if (!(await this.authIntegration({ gitlab })))
      //Done
      throw { message: "Wrong API Token" };
    console.log("CI_PROJECT_PATH", CI_PROJECT_PATH);
    console.log("CI_MERGE_REQUEST_IID", CI_MERGE_REQUEST_IID);
    var temp5 = await gitlab.MergeRequests.show(
      CI_PROJECT_PATH,
      CI_MERGE_REQUEST_IID
    );
    console.log(temp5);
    const { state, web_url, source_branch, diff_refs } =
      await gitlab.MergeRequests.show(CI_PROJECT_PATH, CI_MERGE_REQUEST_IID);
    console.log("Diff_refs", diff_refs);
    console.log("At line 47", state, web_url, source_branch);
    let total_assets = 0;
    if (state === "opened") {
      total_assets = await this.printDownstreamAssets({
        gitlab,
        source_branch,
        diff_refs,
      });
    } else if (state === "merged") {
      total_assets = await this.setResourceOnAsset({
        gitlab,
        web_url,
        source_branch,
      });
    }

    if (total_assets !== 0)
      this.sendSegmentEventOfIntegration("dbt_ci_action_run", {
        asset_count: total_assets,
        total_time: Date.now() - timeStart,
      });
  }

  async printDownstreamAssets({ gitlab, source_branch, diff_refs }) {
    //Done
    // Implementation for printing impact on GitHub
    // Use this.token to access the token
    console.log("At line 74 inside printDownstreamAssets");
    const changedFiles = await this.getChangedFiles({ gitlab, diff_refs }); //Complete
    console.log("At line 75", changedFiles);
    let comments = ``;
    let totalChangedFiles = 0;

    for (const { fileName, filePath, headSHA } of changedFiles) {
      const assetName = await this.getAssetName({
        //Complete
        gitlab,
        fileName,
        filePath,
        headSHA,
      });
      console.log("At line 88");
      // const environments = getGitLabEnvironments();
      // console.log("At line 90", environments);
      let environment = null;
      // for (const [baseBranchName, environmentName] of environments) {
      //   if (baseBranchName === source_branch) {
      //     environment = environmentName;
      //     break;
      //   }
      // }
      console.log("At line 98", environment);
      const asset = await getAsset({
        //Complete
        name: assetName,
        sendSegmentEventOfIntegration: this.sendSegmentEventOfIntegration,
        environment: environment,
        integration: "gitlab",
      });

      if (asset.error) {
        comments += asset.error;
        totalChangedFiles++;
        continue;
      }
      console.log("At line 112", asset);
      //Cross-check this part once with Jaagrav.

      const totalModifiedFiles = changedFiles.filter(
        (i) => i.status === "modified"
      ).length;
      console.log("At line 118", totalModifiedFiles);
      const { guid } = asset; //Changed code over here
      const timeStart = Date.now();
      const downstreamAssets = await getDownstreamAssets(
        //Done
        asset,
        guid,
        totalModifiedFiles,
        this.sendSegmentEventOfIntegration,
        "gitlab"
      );
      console.log("At line 129 Completed getDownstreamAssets");
      if (totalChangedFiles !== 0) comments += "\n\n---\n\n";

      if (downstreamAssets.error) {
        comments += downstreamAssets.error;
        totalChangedFiles++;
        continue;
      }

      this.sendSegmentEventOfIntegration("dbt_ci_action_downstream_unfurl", {
        //Complete
        asset_guid: asset.guid,
        asset_type: asset.typeName,
        downstream_count: downstreamAssets.length,
        total_fetch_time: Date.now() - timeStart,
      });

      const comment = await this.renderDownstreamAssetsComment({
        //Complete
        asset,
        downstreamAssets,
      });

      comments += comment;

      totalChangedFiles++;
    }

    comments = `### ${getImageURL("atlan-logo", 15, 15)} Atlan impact analysis
Here is your downstream impact analysis for **${totalChangedFiles} ${
      totalChangedFiles > 1 ? "models" : "model"
    }** you have edited.

${comments}`;
    console.log("At line 163 in printDownstreamAssets");
    const existingComment = await this.checkCommentExists({ gitlab }); //Complete
    console.log("At line 165 after checkCommentExists", existingComment);
    if (totalChangedFiles > 0)
      await this.createIssueComment({
        //Complete
        gitlab,
        content: comments,
        comment_id: existingComment?.id,
      });

    if (totalChangedFiles === 0 && existingComment)
      await this.deleteComment({ gitlab, comment_id: existingComment.id }); //Complete
    console.log("Completed PrintDownstreamAssets");
    return totalChangedFiles;
  }

  async setResourceOnAsset({ gitlab, web_url, source_branch }) {
    //Done
    // Implementation for setting resources on GitHub
    // Use this.token to access the token
    const changedFiles = await this.getChangedFiles({ gitlab }); //Done
    var totalChangedFiles = 0;

    if (changedFiles.length === 0) return;

    for (const { fileName, filePath, headSHA } of changedFiles) {
      const assetName = await this.getAssetName({
        gitlab,
        fileName,
        filePath,
        headSHA,
      });

      const environments = getGitLabEnvironments();

      let environment = null;
      for (const [baseBranchName, environmentName] of environments) {
        if (baseBranchName === source_branch) {
          environment = environmentName;
          break;
        }
      }

      const asset = await getAsset({
        //Done
        name: assetName,
        sendSegmentEventOfIntegration: this.sendSegmentEventOfIntegration,
        environment: environment,
        integration: "gitlab",
      });

      if (!asset) continue;

      const { guid: modelGuid } = asset;
      const { guid: tableAssetGuid } = asset.attributes.sqlAsset;

      await createResource(
        //Done
        //Complete
        modelGuid,
        "Pull Request on GitLab",
        web_url,
        this.sendSegmentEventOfIntegration
      );
      await createResource(
        //Done
        tableAssetGuid,
        "Pull Request on GitLab",
        web_url,
        this.sendSegmentEventOfIntegration
      );

      totalChangedFiles++;
    }

    const comment = await this.createIssueComment({
      //Done
      //Complete
      gitlab,
      content: `🎊 Congrats on the merge!

This pull request has been added as a resource to all the assets modified. ✅
`,
      comment_id: null,
      forceNewComment: true,
    });

    return totalChangedFiles;
  }

  async authIntegration({ gitlab }) {
    //Done
    const response = await auth();

    if (response?.status === 401) {
      //Complete
      await this.createIssueComment({
        gitlab,
        content: `We couldn't connect to your Atlan Instance, please make sure to set the valid Atlan Bearer Token as \`ATLAN_API_TOKEN\` in your .gitlab-ci.yml file.

Atlan Instance URL: ${ATLAN_INSTANCE_URL}`,
      });
      return false;
    }

    if (response === undefined) {
      await this.createIssueComment({
        gitlab,
        content: `We couldn't connect to your Atlan Instance, please make sure to set the valid Atlan Instance URL as \`ATLAN_INSTANCE_URL\` in your .gitlab-ci.yml file.

Atlan Instance URL: ${ATLAN_INSTANCE_URL}

Make sure your Atlan Instance URL is set in the following format.
\`https://tenant.atlan.com\`

`,
      });
      return false;
    }
    console.log("Completed authOfIntegration");
    return true;
  }

  async createIssueComment({
    //Done
    //Complete
    gitlab,
    content,
    comment_id = null,
    forceNewComment = false,
  }) {
    console.log("At line 295 inside createIssueComment");
    const { CI_PROJECT_PATH, CI_MERGE_REQUEST_IID } = process.env;

    content = `<!-- ActionCommentIdentifier: atlan-dbt-action -->
${content}`;

    console.log("At line 301 inside createIssueComment", content);
    console.log("IS_DEV", IS_DEV)
    if (IS_DEV) return content;
    
    if (comment_id && !forceNewComment)
      return await gitlab.MergeRequestNotes.edit(
        CI_PROJECT_PATH,
        CI_MERGE_REQUEST_IID,
        comment_id,
        content
      );
    return await gitlab.MergeRequestNotes.create(
      CI_PROJECT_PATH,
      CI_MERGE_REQUEST_IID,
      content
    );
  }

  async sendSegmentEventOfIntegration({ action, properties }) {
    //Done
    //Complete
    // Implement your sendSegmentEvent logic here
    // IMPORT ATLAN_INSTANCE_URL.
    console.log("At line 324 inside sendSegmentOfIntegration");
    const domain = new URL(ATLAN_INSTANCE_URL).hostname;
    const { CI_PROJECT_PATH, CI_JOB_URL } = process.env;

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
    // IMPORT SEGMENTEVENT
    return sendSegmentEvent(action, raw);
  }

  async getChangedFiles({ gitlab, diff_refs }) {
    //Done
    //Complete
    console.log("At line 344 Inside getChangedFiles");
    const { CI_PROJECT_PATH, CI_MERGE_REQUEST_IID } = process.env;
    console.log(CI_PROJECT_PATH, CI_MERGE_REQUEST_IID);
    var changes = await gitlab.MergeRequests.allDiffs(
      //Changed the function name
      CI_PROJECT_PATH,
      CI_MERGE_REQUEST_IID
    );
    // const { changes, diff_refs } = await gitlab.MergeRequests.allDiffs(
    //   //Changed the function name
    //   CI_PROJECT_PATH,
    //   CI_MERGE_REQUEST_IID
    // );
    console.log("At line 351 Inside getChangedFiles");
    console.log("Changes", changes);
    var changedFiles = changes
      .map(({ new_path, old_path }) => {
        try {
          const [modelName] = new_path
            .match(/.*models\/(.*)\.sql/)[1]
            .split("/")
            .reverse()[0]
            .split(".");

          //Cross-check this with Jaagrav. ###
          if (modelName) {
            if (old_path === null) {
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

    console.log("At line 399 Completed getChangedFiles ", changedFiles);

    return changedFiles;
  }

  async getAssetName({ gitlab, fileName, filePath, headSHA }) {
    //Done
    //Complete
    console.log("At line 407 inside getAssetName");
    var regExp = /config\(.*alias=\'([^']+)\'.*\)/im;
    var fileContents = await this.getFileContents({
      gitlab,
      filePath,
      headSHA,
    });
    console.log("At line 414 inside getAssetName");
    var matches = regExp.exec(fileContents);
    console.log("At line 416");
    if (matches) {
      return matches[1];
    }
    console.log("At line 420 completed getAssetName");
    return fileName;
  }

  async getFileContents({ gitlab, filePath, headSHA }) {
    //Done
    //Complete
    console.log("At line 427 inside getFileContents");
    const { CI_PROJECT_PATH } = process.env;
    const { content } = await gitlab.RepositoryFiles.show(
      CI_PROJECT_PATH,
      filePath,
      headSHA
    );
    const buff = Buffer.from(content, "base64");
    console.log("At line 435 inside getFileContents");
    return buff.toString("utf8");
  }

  async checkCommentExists({ gitlab }) {
    //Done
    //Complete
    console.log("At line 442 inside checkCommentExists");
    const { CI_PROJECT_PATH, CI_MERGE_REQUEST_IID } = process.env;
    if (IS_DEV) return null;

    const comments = await gitlab.MergeRequestNotes.all(
      CI_PROJECT_PATH,
      CI_MERGE_REQUEST_IID
    );
    console.log("Existing comments inside checkCommentExists :", comments) 
    return comments.find(
      // Why here we have hardocded value? What should be over here inplace of this.
      (comment) =>
        comment.author.username === "arpit.shukla1" &&
        comment.body.includes(
          "<!-- ActionCommentIdentifier: atlan-dbt-action -->"
        )
    );
  }

  async deleteComment({ gitlab, comment_id }) {
    //Done
    //Complete
    console.log("At line 464 inside deleteComment");
    const { CI_PROJECT_PATH, CI_MERGE_REQUEST_IID } = process.env;

    return await gitlab.MergeRequestNotes.remove(
      CI_PROJECT_PATH,
      CI_MERGE_REQUEST_IID,
      comment_id
    );
  }

  async renderDownstreamAssetsComment({ asset, downstreamAssets }) {
    //Done
    console.log("At line 474 inside renderDownstreamAssetsComment");
    console.log("Assets :", asset);
    console.log("Downstream Assets: ", downstreamAssets);
    let impactedData = downstreamAssets.entities.map(
      //here changed from downstreamAssets.map to downstreamAssets.entities.map
      ({ displayText, guid, typeName, attributes, meanings }) => {
        let readableTypeName = typeName
          .toLowerCase()
          .replace(attributes.connectorName, "")
          .toUpperCase();
        readableTypeName =
          readableTypeName.charAt(0).toUpperCase() +
          readableTypeName.slice(1).toLowerCase();
        return [
          guid,
          displayText,
          attributes.connectorName,
          readableTypeName,
          attributes?.userDescription || attributes?.description || "",
          attributes?.certificateStatus || "",
          [...attributes?.ownerUsers, ...attributes?.ownerGroups] || [],
          meanings
            .map(
              ({ displayText, termGuid }) =>
                `[${displayText}](${ATLAN_INSTANCE_URL}/assets/${termGuid}?utm_source=dbt_gitlab_action)`
            )
            ?.join(", ") || " ",
          attributes?.sourceURL || "",
        ];
      }
    );

    console.log("Impacted Data", impactedData);
    console.log("At line 502 in renderDownstreamAsset");
    impactedData = impactedData.sort((a, b) => a[3].localeCompare(b[3])); // Sort by typeName
    impactedData = impactedData.sort((a, b) => a[2].localeCompare(b[2])); // Sort by connectorName

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
        sourceUrl,
      ]) => {
        const connectorImage = getConnectorImage(connectorName),
          certificationImage = certificateStatus
            ? getCertificationImage(certificateStatus)
            : "";

        return [
          `${connectorImage} [${displayText}](${ATLAN_INSTANCE_URL}/assets/${guid}?utm_source=dbt_gitlab_action) ${certificationImage}`,
          `\`${typeName}\``,
          description,
          owners.join(", ") || " ",
          meanings,
          sourceUrl ? `[Open in ${connectorName}](${sourceUrl})` : " ",
        ];
      }
    );
    console.log("Rows: ", rows);
    //changed downstreamAssets.length to downstreamAssets.entities.length

    const comment = `### ${getConnectorImage(asset.attributes.connectorName)} [${
      asset.displayText
  }](${ATLAN_INSTANCE_URL}/assets/${asset.guid}?utm_source=dbt_github_action) ${
      asset.attributes?.certificateStatus
          ? getCertificationImage(asset.attributes.certificateStatus)
          : ""
  }

<details><summary>
    
<b>${downstreamAssets.entityCount} downstream assets 👇</b></summary><br/>
Name | Type | Description | Owners | Terms | Source URL
--- | --- | --- | --- | --- | ---
${rows.map((row) => row.map(i => i.replace(/\|/g, "•").replace(/\n/g, "")).join(" | ")).join("\n")}

${getImageURL("atlan-logo", 15, 15)} [View asset in Atlan](${ATLAN_INSTANCE_URL}/assets/${asset.guid}?utm_source=dbt_github_action)</details>`;

  return comment
  }
}
