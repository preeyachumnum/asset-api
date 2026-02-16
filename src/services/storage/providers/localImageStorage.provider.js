const fs = require("fs/promises");
const path = require("path");
const crypto = require("crypto");

function getRootDir() {
  const root = String(process.env.ASSET_IMAGE_DIR || "D:\\Work-Mitrpol\\Asset\\images").trim();
  if (!root) throw new Error("Missing ASSET_IMAGE_DIR");
  return root;
}

function getPublicBasePath() {
  const base = String(process.env.ASSET_IMAGE_PUBLIC_BASE || "/files/assets").trim();
  return base.startsWith("/") ? base.replace(/\/+$/, "") : `/${base.replace(/\/+$/, "")}`;
}

function safeNamePart(v, fallback = "file") {
  const s = String(v || "")
    .toLowerCase()
    .replace(/[^a-z0-9._-]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
  return s || fallback;
}

function detectExt(originalName, mimeType) {
  const extFromName = path.extname(String(originalName || "")).trim();
  if (extFromName && extFromName.length <= 10) return extFromName.toLowerCase();

  const map = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
    "image/bmp": ".bmp",
  };
  return map[String(mimeType || "").toLowerCase()] || ".bin";
}

async function saveAssetImage({ assetId, originalName, mimeType, buffer }) {
  if (!buffer || !Buffer.isBuffer(buffer) || buffer.length === 0) {
    throw new Error("Image file is empty");
  }

  const rootDir = getRootDir();
  const now = new Date();
  const yyyy = String(now.getFullYear());
  const mm = String(now.getMonth() + 1).padStart(2, "0");
  const assetPart = safeNamePart(assetId, "asset");
  const randomPart = crypto.randomBytes(4).toString("hex");
  const ext = detectExt(originalName, mimeType);
  const fileName = `${assetPart}-${Date.now()}-${randomPart}${ext}`;

  const relDir = path.join(yyyy, mm);
  const fullDir = path.join(rootDir, relDir);
  const fullPath = path.join(fullDir, fileName);

  await fs.mkdir(fullDir, { recursive: true });
  await fs.writeFile(fullPath, buffer);

  const urlPath = [getPublicBasePath(), yyyy, mm, fileName].join("/").replace(/\/+/g, "/");

  return {
    provider: "local",
    fileUrl: urlPath,
    fullPath,
    async cleanup() {
      try {
        await fs.unlink(fullPath);
      } catch {
        // Ignore cleanup failure.
      }
    },
  };
}

function getStaticMount() {
  return {
    routePath: getPublicBasePath(),
    dirPath: getRootDir(),
  };
}

module.exports = { saveAssetImage, getStaticMount };
