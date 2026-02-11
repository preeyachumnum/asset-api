const { execProc, p } = require("../db/execProc");

// เช็ค GUID แบบง่ายๆ กันพังตั้งแต่ต้น
function isGuid(s) {
  return /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(
    s
  );
}

// 1) ดึง list ทั้งหมด -> dbo.spAssetsList
async function assetsList() {
  const rs = await execProc("dbo.spAssetsList");
  return rs[0] || [];
}

// 2) ดึง asset ที่ไม่มีรูป -> dbo.spAssetsNoImage
async function assetsNoImage() {
  const rs = await execProc("dbo.spAssetsNoImage");
  return rs[0] || [];
}

// 3) รายละเอียด 1 ตัว + รูป -> dbo.spAssetDetail(@AssetId)
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

function toPositiveInt(v, def = 1000, max = 20000) {
  const n = Number.parseInt(String(v ?? "").trim(), 10);
  if (!Number.isFinite(n) || n <= 0) return def;
  return Math.min(n, max);
}

// 4) รายการปัญหาข้อมูล SAP ไม่ตรงกับระบบ -> dbo.spAssetsSapMismatch
async function assetsSapMismatch({ limit = 1000, search = null } = {}) {
  const rs = await execProc("dbo.spAssetsSapMismatch", {
    TopRows: p.int(toPositiveInt(limit, 1000, 20000)),
    Search: p.nvarchar(100, search ? String(search).trim() : null),
  });
  return rs[0] || [];
}

module.exports = { assetsList, assetsNoImage, assetDetail, assetsSapMismatch };
