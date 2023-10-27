import fetch from "node-fetch";
import core from "@actions/core";
import dotenv from "dotenv";
import stringify from "json-stringify-safe";
import {
  getErrorModelNotFound,
  getErrorDoesNotMaterialize,
} from "../templates/atlan.js";

dotenv.config();

const ATLAN_INSTANCE_URL =
  core.getInput("ATLAN_INSTANCE_URL") || process.env.ATLAN_INSTANCE_URL;
const ATLAN_API_TOKEN =
  core.getInput("ATLAN_API_TOKEN") || process.env.ATLAN_API_TOKEN;

export default async function getAsset({
  //Done
  name,
  sendSegmentEventOfIntegration,
  environment,
  integration,
}) {
  var myHeaders = {
    Authorization: `Bearer ${ATLAN_API_TOKEN}`,
    "Content-Type": "application/json",
  };
  console.log("At line 24 inside getAsset function");
  var raw = stringify({
    dsl: {
      from: 0,
      size: 21,
      query: {
        bool: {
          must: [
            {
              match: {
                __state: "ACTIVE",
              },
            },
            {
              match: {
                "__typeName.keyword": "DbtModel",
              },
            },
            {
              match: {
                "name.keyword": name,
              },
            },
            ...(environment
              ? [
                  {
                    term: {
                      "assetDbtEnvironmentName.keyword": environment,
                    },
                  },
                ]
              : []),
          ],
        },
      },
    },
    attributes: [
      "name",
      "description",
      "userDescription",
      "sourceURL",
      "qualifiedName",
      "connectorName",
      "certificateStatus",
      "certificateUpdatedBy",
      "certificateUpdatedAt",
      "ownerUsers",
      "ownerGroups",
      "classificationNames",
      "meanings",
      "dbtModelSqlAssets",
    ],
    relationAttributes: [
      "name",
      "description",
      "assetDbtProjectName",
      "assetDbtEnvironmentName",
      "connectorName",
      "certificateStatus",
    ],
  });

  var requestOptions = {
    method: "POST",
    headers: myHeaders,
    body: raw,
  };
  console.log("Before SendSegmentEventOfIntegration");
  console.log("At line 92 inside getAsset");

  var response = await fetch(
    `${ATLAN_INSTANCE_URL}/api/meta/search/indexsearch#findAssetByExactName`,
    requestOptions
  )
    .then((e) => e.json())
    .catch((err) => {
      sendSegmentEventOfIntegration({
        action: "dbt_ci_action_failure",
        properties: {
          reason: "failed_to_get_asset",
          asset_name: name,
          msg: err,
        },
      });
    });
  console.log("<><><><><><><><><><><><><>");
  console.log(response);
  if (response?.entities?.length) {
    console.log("Over here", response?.entities[0]?.attributes);
  }
  console.log("Got Printed?");
  //Test both the below comments as we have replaced with functions
  if (!response?.entities?.length)
    return {
      error: getErrorModelNotFound(name),
    };

  if (!response?.entities[0]?.attributes?.dbtModelSqlAssets?.length > 0)
    return {
      error: getErrorDoesNotMaterialize(
        name,
        ATLAN_INSTANCE_URL,
        response,
        integration
      ),
    };

  return response.entities[0];
}
