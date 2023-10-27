import {getConnectorImage} from "../utils/index.js"

export function getErrorModelNotFound(name) {
  return `
 <br>❌ Model with name **${name}** could not be found or is deleted <br><br>
  `;
}

export function getErrorDoesNotMaterialize(
  name,
  ATLAN_INSTANCE_URL,
  response,
  integration
) {

  return `
<br>❌ Model with name [${name}](${ATLAN_INSTANCE_URL}/assets/${response.entities[0].guid}/overview?utm_source=dbt_${integration}_action) does not materialise any asset <br><br>`;
}

export function getNewModelAddedComment() {
  return `### ${getConnectorImage("dbt")} <b>${fileName}</b> 🆕
  Its a new model and not present in Atlan yet, you'll see the downstream impact for it after its present in Atlan.`
}