// logger.js

// logger.js

function getCurrentTimestamp() {
  const now = new Date();
  return now.toISOString();
}

function logInfo(message) {
  const timestamp = getCurrentTimestamp();
  console.log(`[INFO][${timestamp}] ${message}`);
}

function logError(message) {
  const timestamp = getCurrentTimestamp();
  console.error(`[ERROR][${timestamp}] ${message}`);
}

function logDebug(message) {
  const timestamp = getCurrentTimestamp();
  console.debug(`[DEBUG][${timestamp}] ${message}`);
}

const logger = {
  logInfo,
  logError,
  logDebug,
};

export default logger;
