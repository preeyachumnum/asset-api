const sql = require("mssql");

let pool = null;

function getConfig() {
  const portValue = process.env.DB_PORT
    ? Number.parseInt(process.env.DB_PORT, 10)
    : undefined;
  const hasPort = Number.isFinite(portValue);
  const instanceName = String(process.env.DB_INSTANCE || "").trim();

  const options = {
    encrypt: process.env.DB_ENCRYPT === "true",
    trustServerCertificate: process.env.DB_TRUST_SERVER_CERT === "true",
  };

  // If DB_PORT is set, prefer direct TCP and skip instance discovery.
  if (!hasPort && instanceName) {
    options.instanceName = instanceName;
  }

  return {
    server: process.env.DB_SERVER,
    database: process.env.DB_DATABASE,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    port: hasPort ? portValue : undefined,
    options,
    pool: { max: 10, min: 0, idleTimeoutMillis: 30000 },
    requestTimeout: 60000,
  };
}

async function getPool() {
  if (pool && pool.connected) return pool;
  pool = await new sql.ConnectionPool(getConfig()).connect();
  return pool;
}

module.exports = { sql, getPool };
