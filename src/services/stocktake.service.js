const path = require("path");
const { parse } = require("csv-parse/sync");
const XLSX = require("xlsx");

const { execProc, execProcRaw, p, sql } = require("../db/execProc");
const { getPool } = require("../db/mssql");

function isGuid(s) {
  return /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(
    String(s || "").trim()
  );
}

function toYear(v) {
  const n = Number.parseInt(String(v ?? "").trim(), 10);
  if (!Number.isFinite(n) || n < 2000 || n > 2600) {
    return new Date().getUTCFullYear();
  }
  return n;
}

function toText(v, max = 1000) {
  const s = String(v ?? "").trim();
  if (!s) return null;
  return s.slice(0, max);
}

function normalizeCode(v) {
  return String(v ?? "")
    .trim()
    .toUpperCase()
    .replace(/[\s-]+/g, "_");
}

const STATUS_ALIAS_MAP = {
  COUNTED: "COUNTED",
  NORMAL: "COUNTED",
  ACTIVE: "COUNTED",
  OK: "COUNTED",

  NOT_COUNTED: "NOT_COUNTED",
  NOTFOUND: "NOT_COUNTED",
  NOT_FOUND: "NOT_COUNTED",
  LOST: "NOT_COUNTED",
  MISSING: "NOT_COUNTED",

  DAMAGED: "OTHER",
  BROKEN: "OTHER",
  DEFECTIVE: "OTHER",
  OTHER: "OTHER",

  PENDING: "PENDING",
  PENDING_DEMOLISH: "PENDING",
  WAITING_DEMOLISH: "PENDING",

  REJECTED: "REJECTED",
};

function toStatusCode(v) {
  const key = normalizeCode(v);
  if (!key) return "COUNTED";
  return STATUS_ALIAS_MAP[key] || key;
}

const COUNT_METHOD_ALIAS_MAP = {
  QR: "QR",
  QRCODE: "QR",
  MANUAL: "MANUAL",
  EXCEL: "EXCEL",
  MOBILE: "MANUAL",
  BARCODE: "BARCODE",
  BAR_CODE: "BARCODE",
  BC: "BARCODE",
};

function toCountMethod(v) {
  const key = normalizeCode(v);
  if (!key) return "MANUAL";
  return COUNT_METHOD_ALIAS_MAP[key] || key;
}

async function stocktakeGetOrCreate({ plantId, stocktakeYear, userId }) {
  if (!isGuid(plantId)) throw new Error("plantId must be GUID");
  if (!isGuid(userId)) throw new Error("userId must be GUID");

  const r = await execProcRaw(
    "dbo.spStocktakeGetOrCreate",
    {
      PlantId: p.uuid(plantId),
      StocktakeYear: p.int(toYear(stocktakeYear)),
      UserId: p.uuid(userId),
    },
    {
      StocktakeId: sql.UniqueIdentifier,
    }
  );

  const outId = r.output && r.output.StocktakeId;
  if (isGuid(outId)) return outId;

  const rsId =
    r.recordsets &&
    r.recordsets[0] &&
    r.recordsets[0][0] &&
    (r.recordsets[0][0].StocktakeId || r.recordsets[0][0].stocktakeId);

  if (!isGuid(rsId)) {
    throw new Error("Unable to resolve StocktakeId");
  }

  return rsId;
}

async function stocktakeGetConfig({ plantId, stocktakeYear }) {
  if (!isGuid(plantId)) throw new Error("plantId must be GUID");
  const year = toYear(stocktakeYear);

  const pool = await getPool();
  const r = await pool
    .request()
    .input("PlantId", sql.UniqueIdentifier, plantId)
    .input("StocktakeYear", sql.Int, year)
    .query(`
      SELECT
        yc.StocktakeYearConfigId,
        yc.PlantId,
        yc.StocktakeYear,
        yc.IsOpen,
        yc.ReportGeneratedAt,
        yc.ClosedAt,
        yc.ClosedByUserId,
        s.StocktakeId
      FROM dbo.StocktakeYearConfigs yc
      LEFT JOIN dbo.Stocktakes s
        ON s.StocktakeYearConfigId = yc.StocktakeYearConfigId
      WHERE yc.PlantId = @PlantId
        AND yc.StocktakeYear = @StocktakeYear
    `);

  return (r.recordset && r.recordset[0]) || null;
}

async function stocktakeReportSummary({ plantId, stocktakeYear }) {
  return execProc("dbo.spStocktake_Report_Summary", {
    PlantId: p.uuid(plantId),
    StocktakeYear: p.int(toYear(stocktakeYear)),
  });
}

async function stocktakeReportDetail({ plantId, stocktakeYear, statusCode = null, search = null }) {
  return execProc("dbo.spStocktake_Report_Detail", {
    PlantId: p.uuid(plantId),
    StocktakeYear: p.int(toYear(stocktakeYear)),
    StatusCode: p.nvarchar(50, toText(statusCode, 50)),
    Search: p.nvarchar(200, toText(search, 200)),
  });
}

async function stocktakeReportExport3Tabs({ plantId, stocktakeYear, search = null }) {
  return execProc("dbo.spStocktake_Report_Export3Tabs", {
    PlantId: p.uuid(plantId),
    StocktakeYear: p.int(toYear(stocktakeYear)),
    Search: p.nvarchar(200, toText(search, 200)),
  });
}

async function stocktakeScan({ stocktakeId, assetId, statusCode, countedByUserId, countMethod, noteText }) {
  if (!isGuid(stocktakeId)) throw new Error("stocktakeId must be GUID");
  if (!isGuid(assetId)) throw new Error("assetId must be GUID");
  if (!isGuid(countedByUserId)) throw new Error("countedByUserId must be GUID");

  const r = await execProcRaw(
    "dbo.spStocktake_Scan",
    {
      StocktakeId: p.uuid(stocktakeId),
      AssetId: p.uuid(assetId),
      StatusCode: p.nvarchar(50, toStatusCode(statusCode)),
      CountedByUserId: p.uuid(countedByUserId),
      CountMethod: p.nvarchar(20, toCountMethod(countMethod)),
      NoteText: p.nvarchar(1000, toText(noteText, 1000)),
    },
    {
      StocktakeItemId: sql.UniqueIdentifier,
    }
  );

  const outId = r.output && r.output.StocktakeItemId;
  const rsId =
    r.recordsets &&
    r.recordsets[0] &&
    r.recordsets[0][0] &&
    (r.recordsets[0][0].StocktakeItemId || r.recordsets[0][0].stocktakeItemId);
  const stocktakeItemId = isGuid(outId) ? outId : rsId;
  if (!isGuid(stocktakeItemId)) throw new Error("Unable to resolve StocktakeItemId");
  return stocktakeItemId;
}

async function stocktakeAddImage({ stocktakeItemId, fileUrl }) {
  if (!isGuid(stocktakeItemId)) throw new Error("stocktakeItemId must be GUID");
  const safeUrl = String(fileUrl || "").trim();
  if (!safeUrl) throw new Error("fileUrl is required");

  const r = await execProcRaw(
    "dbo.spStocktake_AddImage",
    {
      StocktakeItemId: p.uuid(stocktakeItemId),
      FileUrl: p.nvarchar(1000, safeUrl),
    },
    {
      StocktakeItemImageId: sql.UniqueIdentifier,
    }
  );

  const outId = r.output && r.output.StocktakeItemImageId;
  const rsId =
    r.recordsets &&
    r.recordsets[0] &&
    r.recordsets[0][0] &&
    (r.recordsets[0][0].StocktakeItemImageId || r.recordsets[0][0].stocktakeItemImageId);

  return isGuid(outId) ? outId : rsId || null;
}

async function stocktakeImportCountJson({ stocktakeId, importedByUserId, items }) {
  if (!isGuid(stocktakeId)) throw new Error("stocktakeId must be GUID");
  if (!isGuid(importedByUserId)) throw new Error("importedByUserId must be GUID");
  if (!Array.isArray(items)) throw new Error("items must be array");

  const payload = items.map((x) => ({
    AssetNo: String(x.AssetNo || "").trim(),
    StatusCode: toStatusCode(x.StatusCode),
    NoteText: toText(x.NoteText, 1000),
    CountMethod: toCountMethod(x.CountMethod || "EXCEL"),
  }));

  const rs = await execProc("dbo.spStocktake_ImportCountJson", {
    StocktakeId: p.uuid(stocktakeId),
    ImportedByUserId: p.uuid(importedByUserId),
    ItemsJson: p.ntext(JSON.stringify(payload)),
  });

  return (rs[0] && rs[0][0]) || { ImportedRows: 0 };
}

async function stocktakeCloseYear({ plantId, stocktakeYear, closedByUserId }) {
  if (!isGuid(plantId)) throw new Error("plantId must be GUID");
  if (!isGuid(closedByUserId)) throw new Error("closedByUserId must be GUID");

  await execProc("dbo.spStocktakeCloseYear", {
    PlantId: p.uuid(plantId),
    StocktakeYear: p.int(toYear(stocktakeYear)),
    ClosedByUserId: p.uuid(closedByUserId),
  });
}

async function stocktakeOpenNextYearWithCarryPending({
  plantId,
  fromYear,
  toYear: toYearInput,
  createdByUserId,
}) {
  if (!isGuid(plantId)) throw new Error("plantId must be GUID");
  if (!isGuid(createdByUserId)) throw new Error("createdByUserId must be GUID");

  const safeFromYear = toYear(fromYear);
  const safeToYear = Number.isFinite(Number(toYearInput))
    ? toYear(toYearInput)
    : safeFromYear + 1;

  const r = await execProcRaw(
    "dbo.spStocktake_OpenNextYearWithCarryPending",
    {
      PlantId: p.uuid(plantId),
      FromYear: p.int(safeFromYear),
      ToYear: p.int(safeToYear),
      CreatedByUserId: p.uuid(createdByUserId),
    },
    {
      NewStocktakeId: sql.UniqueIdentifier,
    }
  );

  const outId = r.output && r.output.NewStocktakeId;
  const rsId =
    r.recordsets &&
    r.recordsets[0] &&
    r.recordsets[0][0] &&
    (r.recordsets[0][0].NewStocktakeId || r.recordsets[0][0].newStocktakeId);

  const newStocktakeId = isGuid(outId) ? outId : rsId || null;
  return {
    fromYear: safeFromYear,
    toYear: safeToYear,
    newStocktakeId,
  };
}

function normalizeHeader(v) {
  return String(v || "")
    .trim()
    .toLowerCase()
    .replace(/^\uFEFF/, "")
    .replace(/[\s_.-]+/g, "");
}

function mapImportRow(row) {
  const keys = Object.keys(row || {});
  const index = {};
  keys.forEach((k) => {
    index[normalizeHeader(k)] = k;
  });

  const pick = (aliases) => {
    for (const a of aliases) {
      const k = index[normalizeHeader(a)];
      if (k) return row[k];
    }
    return null;
  };

  const assetNo = String(pick(["AssetNo", "Asset Number", "asset_no"]) || "").trim();
  const statusCode = toStatusCode(pick(["StatusCode", "Status", "Result"]));
  const noteText = toText(pick(["NoteText", "Note", "Remark"]), 1000);
  const countMethod = toCountMethod(pick(["CountMethod", "Method", "Source"]) || "EXCEL");

  if (!assetNo) return null;
  return {
    AssetNo: assetNo,
    StatusCode: statusCode,
    NoteText: noteText,
    CountMethod: countMethod,
  };
}

function parseCsvBuffer(buffer) {
  const text = buffer.toString("utf8");
  const rows = parse(text, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    bom: true,
  });

  return (rows || []).map(mapImportRow).filter(Boolean);
}

function parseXlsxBuffer(buffer) {
  const wb = XLSX.read(buffer, { type: "buffer" });
  const sheetName = wb.SheetNames && wb.SheetNames[0];
  if (!sheetName) return [];

  const sheet = wb.Sheets[sheetName];
  if (!sheet) return [];

  const rows = XLSX.utils.sheet_to_json(sheet, { defval: "" });
  return (rows || []).map(mapImportRow).filter(Boolean);
}

function parseStocktakeImportFile({ originalName, buffer }) {
  if (!buffer || !Buffer.isBuffer(buffer) || !buffer.length) {
    throw new Error("Import file is empty");
  }

  const ext = String(path.extname(originalName || "") || "")
    .trim()
    .toLowerCase();

  if (ext === ".xlsx" || ext === ".xls") {
    return parseXlsxBuffer(buffer);
  }
  if (ext === ".csv" || ext === ".txt" || !ext) {
    return parseCsvBuffer(buffer);
  }

  throw new Error("Unsupported import file type. Use .csv or .xlsx");
}

module.exports = {
  toYear,
  stocktakeGetOrCreate,
  stocktakeGetConfig,
  stocktakeReportSummary,
  stocktakeReportDetail,
  stocktakeReportExport3Tabs,
  stocktakeScan,
  stocktakeAddImage,
  stocktakeImportCountJson,
  stocktakeCloseYear,
  stocktakeOpenNextYearWithCarryPending,
  parseStocktakeImportFile,
};
