const ATLAN_INSTANCE_URL = process.env.ATLAN_INSTANCE_URL;


//Authentication 

function generateErrorMessageForAuthorization() {
    return `We couldn't connect to your Atlan Instance, please make sure to set the valid Atlan Bearer Token as \`ATLAN_API_TOKEN\` as this repository's action secret. 

    Atlan Instance URL: ${ATLAN_INSTANCE_URL}
    
    Set your repository action secrets [here](https://github.com/${context.payload.repository.full_name}/settings/secrets/actions). For more information on how to setup the Atlan dbt Action, please read the [setup documentation here](https://github.com/atlanhq/dbt-action/blob/main/README.md).`
}

function generateErrorMessageForAuthorizationResponseUndefined() {
    return `We couldn't connect to your Atlan Instance, please make sure to set the valid Atlan Instance URL as \`ATLAN_INSTANCE_URL\` in your .gitlab-ci.yml file.

    Atlan Instance URL: ${ATLAN_INSTANCE_URL}
    
    Make sure your Atlan Instance URL is set in the following format.
    \`https://tenant.atlan.com\`
    
    `
}

