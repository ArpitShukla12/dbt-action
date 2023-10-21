const ATLAN_INSTANCE_URL = process.env.ATLAN_INSTANCE_URL;


//Authentication 

function getErrorResponseStatus401 () {
    return `We couldn't connect to your Atlan Instance, please make sure to set the valid Atlan Bearer Token as \`ATLAN_API_TOKEN\` as this repository's action secret. 

    Atlan Instance URL: ${ATLAN_INSTANCE_URL}
    
    Set your repository action secrets [here](https://github.com/${context.payload.repository.full_name}/settings/secrets/actions). For more information on how to setup the Atlan dbt Action, please read the [setup documentation here](https://github.com/atlanhq/dbt-action/blob/main/README.md).`
}

function getErrorResponseStatusUndefined() {
    return `We couldn't connect to your Atlan Instance, please make sure to set the valid Atlan Instance URL as \`ATLAN_INSTANCE_URL\` in your .gitlab-ci.yml file.

    Atlan Instance URL: ${ATLAN_INSTANCE_URL}
    
    Make sure your Atlan Instance URL is set in the following format.
    \`https://tenant.atlan.com\`
    
    `
}

function getRenderDownstreamComment() {
    return `### ${getConnectorImage(
        asset.attributes.connectorName
      )} [${asset.displayText}](${ATLAN_INSTANCE_URL}/assets/${
        asset.guid
      }?utm_source=dbt_github_action) ${
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
      }?utm_source=dbt_github_action)</details>`
}

function getSetResourceOnAssetComment() {
    return `🎊 Congrats on the merge!
  
    This pull request has been added as a resource to all the assets modified. ✅
    `
}