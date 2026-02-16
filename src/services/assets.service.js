const { execProc, p } = require("../db/execProc");

function isGuid(s) {
  return /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(
    s
  );
}

function toPositiveInt(v, def = 1, max = 500) {
  const n = Number.parseInt(String(v ?? "").trim(), 10);
  if (!Number.isFinite(n) || n <= 0) return def;
  return Math.min(n, max);
}

function toSearch(v) {
  const s = String(v ?? "").trim();
  return s ? s : null;
}

function toPagedResult(rs, page, pageSize) {
  const rows = rs[0] || [];
  const totalRows = Number((rs[1] && rs[1][0] && rs[1][0].TotalRows) || 0);
  const safeTotal = Number.isFinite(totalRows) ? totalRows : 0;
  const totalPages = safeTotal > 0 ? Math.ceil(safeTotal / pageSize) : 0;

  return {
    rows,
    paging: {
      page,
      pageSize,
      totalRows: safeTotal,
      totalPages,
    },
  };
}

async function assetsList({ page = 1, pageSize = 50, search = null } = {}) {
  const safePage = toPositiveInt(page, 1, 100000);
  const safePageSize = toPositiveInt(pageSize, 50, 500);

  const rs = await execProc("dbo.spAssetsListPaged", {
    Page: p.int(safePage),
    PageSize: p.int(safePageSize),
    Search: p.nvarchar(100, toSearch(search)),
  });

  return toPagedResult(rs, safePage, safePageSize);
}

async function assetsNoImage({ page = 1, pageSize = 50, search = null } = {}) {
  const safePage = toPositiveInt(page, 1, 100000);
  const safePageSize = toPositiveInt(pageSize, 50, 500);

  const rs = await execProc("dbo.spAssetsNoImagePaged", {
    Page: p.int(safePage),
    PageSize: p.int(safePageSize),
    Search: p.nvarchar(100, toSearch(search)),
  });

  return toPagedResult(rs, safePage, safePageSize);
}

async function assetDetail(assetId) {
  if (!isGuid(assetId)) throw new Error("assetId must be GUID");

  const rs = await execProc("dbo.spAssetDetail", {
    AssetId: p.uuid(assetId),
  });

  return {
    asset: (rs[0] && rs[0][0]) || null,
    images: rs[1] || [],
  };
}

async function assetsSapMismatch({ page = 1, pageSize = 50, search = null } = {}) {
  const safePage = toPositiveInt(page, 1, 100000);
  const safePageSize = toPositiveInt(pageSize, 50, 500);

  const rs = await execProc("dbo.spAssetsSapMismatchPaged", {
    Page: p.int(safePage),
    PageSize: p.int(safePageSize),
    Search: p.nvarchar(100, toSearch(search)),
  });

  return toPagedResult(rs, safePage, safePageSize);
}

async function assetAddImage({ assetId, fileUrl, isPrimary = false } = {}) {
  if (!isGuid(String(assetId || ""))) throw new Error("assetId must be GUID");
  const safeFileUrl = String(fileUrl || "").trim();
  if (!safeFileUrl) throw new Error("fileUrl is required");

  const rs = await execProc("dbo.spAssetImageAdd", {
    AssetId: p.uuid(assetId),
    FileUrl: p.nvarchar(1000, safeFileUrl),
    IsPrimary: p.bit(Boolean(isPrimary)),
  });

  return (rs[0] && rs[0][0]) || null;
}

module.exports = {
  assetsList,
  assetsNoImage,
  assetDetail,
  assetsSapMismatch,
  assetAddImage,
};
