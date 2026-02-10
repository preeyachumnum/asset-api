const fs = require("fs/promises");
const path = require("path");
const { parse } = require("csv-parse/sync");

const { execProc, p } = require("../db/execProc");

function splitFiles(envValue) {
  return String(envValue || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

function toPositiveInt(v, def) {
  const n = Number.parseInt(String(v ?? "").trim(), 10);
  if (!Number.isFinite(n) || n <= 0) return def;
  return n;
}

async function readAndParseSapCsv(fullPath) {
  const text = await fs.readFile(fullPath, "utf8");

  // ไฟล์ SAP คั่นด้วย | และมีข้อความแบบ 10" 8" จึงปิด quote ไปเลย
  const rows = parse(text, {
    delimiter: "|",
    columns: true,
    trim: true,
    skip_empty_lines: true,
    bom: true,
    quote: false,
  });

  return Array.isArray(rows) ? rows : [];
}

async function purgeSapStaging({ retainDays = 3, batchSize = 50000 } = {}) {
  const safeRetainDays = toPositiveInt(retainDays, 3);
  const safeBatchSize = Math.min(toPositiveInt(batchSize, 50000), 100000);

  let totalDeleted = 0;
  let rounds = 0;

  try {
    // Delete in batches so large tables do not lock for too long.
    for (let i = 0; i < 100; i += 1) {
      rounds += 1;
      const rs = await execProc("dbo.spSapAsset_Staging_Purge", {
        RetainDays: p.int(safeRetainDays),
        BatchSize: p.int(safeBatchSize),
      });

      const row = (rs[0] && rs[0][0]) || {};
      const deletedRows = Number(
        row.DeletedRows ??
          row.deletedRows ??
          row.RowCount ??
          row.rowCount ??
          0
      );

      totalDeleted += Number.isFinite(deletedRows) ? deletedRows : 0;

      if (!Number.isFinite(deletedRows) || deletedRows < safeBatchSize) break;
    }

    return {
      ok: true,
      retainDays: safeRetainDays,
      batchSize: safeBatchSize,
      rounds,
      deletedRows: totalDeleted,
    };
  } catch (e) {
    // Keep import success even when purge SP is not deployed yet.
    return {
      ok: false,
      retainDays: safeRetainDays,
      batchSize: safeBatchSize,
      message: e.message || String(e),
    };
  }
}

// อ่านไฟล์ SAP -> แปลงเป็น JSON -> ส่งให้ SP อัปเดตลง DB
async function importSapFiles() {
  const dropDir = String(process.env.SAP_DROP_DIR || "").trim();
  if (!dropDir) throw new Error("Missing SAP_DROP_DIR");

  const files = splitFiles(process.env.SAP_FILES);
  if (!files.length) throw new Error("Missing SAP_FILES");

  const results = [];

  for (const fileName of files) {
    const fullPath = path.join(dropDir, fileName);

    try {
      const rows = await readAndParseSapCsv(fullPath);
      const json = JSON.stringify(rows);

      const rs = await execProc("dbo.spSapAsset_ImportJson", {
        SourceFileName: p.nvarchar(255, fileName),
        Json: p.ntext(json),
      });

      const batchId = (rs[0] && rs[0][0] && (rs[0][0].ImportBatchId || rs[0][0].importBatchId)) || null;

      results.push({ ok: true, file: fileName, rows: rows.length, importBatchId: batchId });
    } catch (e) {
      results.push({ ok: false, file: fileName, message: e.message || String(e) });
    }
  }

  const stagingPurge = await purgeSapStaging({
    retainDays: process.env.SAP_STAGING_RETENTION_DAYS || 3,
    batchSize: process.env.SAP_STAGING_PURGE_BATCH_SIZE || 50000,
  });

  return { results, stagingPurge };
}

module.exports = { importSapFiles };
