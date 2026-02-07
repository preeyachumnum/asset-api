const sql = require("mssql");

let pool = null;

function getConfig() {
  const portValue = process.env.DB_PORT
    ? Number.parseInt(process.env.DB_PORT, 10)
    : undefined;
  return {
    server: process.env.DB_SERVER,
    database: process.env.DB_DATABASE,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    port: Number.isFinite(portValue) ? portValue : undefined,
    options: {
      encrypt: process.env.DB_ENCRYPT === "true",
      trustServerCertificate: process.env.DB_TRUST_SERVER_CERT === "true",
      instanceName: process.env.DB_INSTANCE || undefined,
    },
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
