export function getErrorModelNotFound(name) {
  return `❌ Model with name **${name}** could not be found or is deleted <br><br>`;
}

export function getErrorDoesNotMaterialize(
  name,
  ATLAN_INSTANCE_URL,
  response,
  integration
) {
  return `❌ Model with name [${name}](${ATLAN_INSTANCE_URL}/assets/${response.entities[0].guid}/overview?utm_source=dbt_${integration}_action) does not materialise any asset <br><br>`;
}
