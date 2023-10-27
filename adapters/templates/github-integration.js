import { getImageURL, getConnectorImage, getCertificationImage } from "../utils/index.js";

export function getErrorResponseStatus401 (ATLAN_INSTANCE_URL, context) {
    return `We couldn't connect to your Atlan Instance, please make sure to set the valid Atlan Bearer Token as \`ATLAN_API_TOKEN\` as this repository's action secret. 

    Atlan Instance URL: ${ATLAN_INSTANCE_URL}
    
    Set your repository action secrets [here](https://github.com/${context.payload.repository.full_name}/settings/secrets/actions). For more information on how to setup the Atlan dbt Action, please read the [setup documentation here](https://github.com/atlanhq/dbt-action/blob/main/README.md).`
}

export function getErrorResponseStatusUndefined(ATLAN_INSTANCE_URL, context) {
    return `We couldn't connect to your Atlan Instance, please make sure to set the valid Atlan Instance URL as \`ATLAN_INSTANCE_URL\` as this repository's action secret. 

    Atlan Instance URL: ${ATLAN_INSTANCE_URL}
    
    Make sure your Atlan Instance URL is set in the following format.
    \`https://tenant.atlan.com\`
    
    Set your repository action secrets [here](https://github.com/${context.payload.repository.full_name}/settings/secrets/actions). For more information on how to setup the Atlan dbt Action, please read the [setup documentation here](https://github.com/atlanhq/dbt-action/blob/main/README.md).`
}

export function getSetResourceOnAssetComment() {
    return `🎊 Congrats on the merge!

    This pull request has been added as a resource to all the assets modified. ✅`
}

export function getAssetInfo(ATLAN_INSTANCE_URL, asset, materialisedAsset, environmentName, projectName) {
    return `### ${getConnectorImage(
        asset.attributes.connectorName
      )} [${asset.displayText}](${ATLAN_INSTANCE_URL}/assets/${
        asset.guid
      }/overview?utm_source=dbt_github_action) ${
        asset.attributes?.certificateStatus
          ? getCertificationImage(asset.attributes.certificateStatus)
          : ""
      }
  Materialised asset: ${getConnectorImage(
        materialisedAsset.attributes.connectorName
      )} [${materialisedAsset.attributes.name}](${ATLAN_INSTANCE_URL}/assets/${
        materialisedAsset.guid
      }/overview?utm_source=dbt_github_action) ${
        materialisedAsset.attributes?.certificateStatus
          ? getCertificationImage(materialisedAsset.attributes.certificateStatus)
          : ""
      }${environmentName ? ` | Environment Name: \`${environmentName}\`` : ""}${
        projectName ? ` | Project Name: \`${projectName}\`` : ""
      }`
}

export function getDownstreamTable(ATLAN_INSTANCE_URL, downstreamAssets, rows, materialisedAsset) {
    return `<details><summary><b>${
        downstreamAssets.entityCount
      } downstream assets 👇</b></summary><br/>
  
  Name | Type | Description | Owners | Terms | Classifications | Source URL
  --- | --- | --- | --- | --- | --- | ---
  ${rows
    .map((row) =>
      row.map((i) => i.replace(/\|/g, "•").replace(/\n/g, "")).join(" | ")
    )
    .join("\n")}
  
  ${
    downstreamAssets.hasMore
      ? `[See more downstream assets at Atlan](${ATLAN_INSTANCE_URL}/assets/${materialisedAsset.guid}/lineage?utm_source=dbt_github_action)`
      : ""
  }
  
  </details>`
}

export function getViewAssetButton(ATLAN_INSTANCE_URL, asset) {
    return `${getImageURL(
        "atlan-logo",
        15,
        15
      )} [View asset in Atlan](${ATLAN_INSTANCE_URL}/assets/${
        asset.guid
      }/overview?utm_source=dbt_github_action)`
}