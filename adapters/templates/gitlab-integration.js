import { getImageURL, getConnectorImage, getCertificationImage } from "../utils/index.js";

export function getErrorResponseStatus401 (ATLAN_INSTANCE_URL, CI_PROJECT_NAME, GITLAB_USER_LOGIN) {//Have changed comment make sure to recheck it with team
    return `We couldn't connect to your Atlan Instance, please make sure to set the valid Atlan Bearer Token as \`ATLAN_API_TOKEN\` as this repository's action secret. 

Atlan Instance URL: ${ATLAN_INSTANCE_URL}
    
Set your repository action secrets [here](https://gitlab.com/${GITLAB_USER_LOGIN}/${CI_PROJECT_NAME}/-/settings/ci_cd). For more information on how to setup the Atlan dbt Action, please read the [setup documentation here](https://github.com/atlanhq/dbt-action/blob/main/README.md).`
}

export function getErrorResponseStatusUndefined(ATLAN_INSTANCE_URL, CI_PROJECT_NAME, GITLAB_USER_LOGIN) {
    return `We couldn't connect to your Atlan Instance, please make sure to set the valid Atlan Instance URL as \`ATLAN_INSTANCE_URL\` as this repository's action secret. 

Atlan Instance URL: ${ATLAN_INSTANCE_URL}
    
Make sure your Atlan Instance URL is set in the following format.
\`https://tenant.atlan.com\`
    
Set your repository action secrets [here](https://gitlab.com/${GITLAB_USER_LOGIN}/${CI_PROJECT_NAME}/-/settings/ci_cd). For more information on how to setup the Atlan dbt Action, please read the [setup documentation here](https://github.com/atlanhq/dbt-action/blob/main/README.md).`
}

export function getRenderDownstreamComment(asset,ATLAN_INSTANCE_URL,downstreamAssets,rows) {
    console.log("Rows",rows)
    if(rows.length == 0) {
        return `### ${getConnectorImage(
            asset.attributes.connectorName
        )} [${asset.displayText}](${ATLAN_INSTANCE_URL}/assets/${
            asset.guid
        }?utm_source=dbt_gitlab_action) ${
            asset.attributes?.certificateStatus
                ? getCertificationImage(asset.attributes.certificateStatus)
                : ""
        }
        
  ${getImageURL("atlan-logo", 15, 15)} [View asset in Atlan](${ATLAN_INSTANCE_URL}/assets/${asset.guid}?utm_source=dbt_gitlab_action)`;
    }
    return `### ${getConnectorImage(
        asset.attributes.connectorName
      )} [${asset.displayText}](${ATLAN_INSTANCE_URL}/assets/${
        asset.guid
      }?utm_source=dbt_gitlab_action) ${
        asset.attributes?.certificateStatus
          ? getCertificationImage(asset.attributes.certificateStatus)
          : ""
      }
  
  <details><summary>
      
  <b>${downstreamAssets.entityCount} downstream assets 👇</b></summary><br/>
  Name | Type | Description | Owners | Terms | Source URL
  --- | --- | --- | --- | --- | ---
  ${rows
    .map((row) =>
      row.map((i) => i.replace(/\|/g, "•").replace(/\n/g, "")).join(" | ")
    )
    .join("\n")}
  
  ${getImageURL(
    "atlan-logo",
    15,
    15
  )} [View asset in Atlan](${ATLAN_INSTANCE_URL}/assets/${
        asset.guid
      }?utm_source=dbt_gitlab_action)</details>`
}

export function getSetResourceOnAssetComment() {
    return `🎊 Congrats on the merge!
  
This pull request has been added as a resource to all the assets modified. ✅
`
}