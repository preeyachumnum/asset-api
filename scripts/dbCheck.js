require("dotenv").config();
const { getPool } = require("../src/db/mssql");

function toInt(v, def) {
  const n = Number.parseInt(String(v ?? "").trim(), 10);
  return Number.isFinite(n) && n > 0 ? n : def;
}

function splitList(v, def = []) {
  const s = String(v || "").trim();
  if (!s) return def;
  return s
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}

function keyOf(schema, name) {
  return `${schema}.${name}`.toLowerCase();
}

async function main() {
  const top = toInt(process.env.DBCHECK_TOP, 20);
  const watchTables = splitList(process.env.DBCHECK_TABLES, [
    "dbo.SapAsset_Staging",
    "dbo.SapAsset_Current",
    "dbo.Assets",
  ]).map((x) => x.toLowerCase());

  const pool = await getPool();

  const meta = await pool.request().query(`
    SELECT
      DB_NAME() AS database_name,
      @@SERVERNAME AS server_name,
      SYSUTCDATETIME() AS checked_at_utc
  `);

  const latest = await pool.request().query(`
    SELECT TOP (${top})
      o.type_desc,
      s.name AS schema_name,
      o.name AS object_name,
      o.modify_date
    FROM sys.objects o
    JOIN sys.schemas s ON s.schema_id = o.schema_id
    WHERE o.type IN ('U','P')
    ORDER BY o.modify_date DESC
  `);

  const rowCounts = await pool.request().query(`
    SELECT
      s.name AS schema_name,
      t.name AS table_name,
      SUM(p.rows) AS row_count
    FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    JOIN sys.partitions p ON p.object_id = t.object_id
    WHERE p.index_id IN (0,1)
    GROUP BY s.name, t.name
    ORDER BY row_count DESC
  `);

  let stagingBatches = [];
  try {
    const rs = await pool.request().query(`
      SELECT TOP (5)
        ImportBatchId,
        SourceFileName,
        COUNT(*) AS row_count,
        MIN(LoadedAt) AS first_loaded_at,
        MAX(LoadedAt) AS last_loaded_at
      FROM dbo.SapAsset_Staging
      GROUP BY ImportBatchId, SourceFileName
      ORDER BY MAX(LoadedAt) DESC
    `);
    stagingBatches = rs.recordset || [];
  } catch {
    stagingBatches = [];
  }

  const countsByKey = new Map(
    (rowCounts.recordset || []).map((r) => [
      keyOf(r.schema_name, r.table_name),
      Number(r.row_count || 0),
    ])
  );

  const watched = watchTables.map((fullName) => {
    const [schemaName, tableName] = fullName.includes(".")
      ? fullName.split(".", 2)
      : ["dbo", fullName];
    return {
      table: `${schemaName}.${tableName}`,
      rowCount: countsByKey.get(keyOf(schemaName, tableName)) ?? null,
    };
  });

  const info = (meta.recordset && meta.recordset[0]) || {};
  console.log("DB CHECK");
  console.log(
    JSON.stringify(
      {
        database: info.database_name,
        server: info.server_name,
        checkedAtUtc: info.checked_at_utc,
      },
      null,
      2
    )
  );

  console.log("\nWATCH TABLES");
  console.table(watched);

  console.log("\nLATEST OBJECTS");
  console.table(latest.recordset || []);

  if (stagingBatches.length) {
    console.log("\nSAP STAGING LATEST BATCHES");
    console.table(stagingBatches);
  }
}

main().catch((e) => {
  console.error("db:check failed:", e.message || e);
  process.exit(1);
});
