require("dotenv").config();
const express = require("express");
const cors = require("cors");
const multer = require("multer");

const {
  assetsList,
  assetsNoImage,
  assetDetail,
  assetsSapMismatch,
  assetAddImage,
} = require("./services/assets.service");
const {
  loginBegin,
  loginCreateSession,
  validateSession,
  logout,
  switchPlant,
  requireSession,
} = require("./services/auth.service");
const {
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
} = require("./services/stocktake.service");

const { importSapFiles } = require("./services/sapImport.service");
const { startSapImportJob } = require("./jobs/sapImport.job");
const { saveAssetImage, getImageStaticMount } = require("./services/storage/imageStorage.service");

const app = express();
app.use(
  cors({
    origin: true,
    credentials: true,
  })
);
app.use(express.json());

const imageUpload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: Number(process.env.ASSET_IMAGE_MAX_BYTES || 10 * 1024 * 1024),
  },
});
const importUpload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: Number(process.env.STOCKTAKE_IMPORT_MAX_BYTES || 20 * 1024 * 1024),
  },
});

const staticMount = getImageStaticMount();
if (staticMount && staticMount.routePath && staticMount.dirPath) {
  app.use(staticMount.routePath, express.static(staticMount.dirPath));
  console.log(
    `Image static mount: ${staticMount.routePath} -> ${staticMount.dirPath}`
  );
}

// ดึง sessionId จาก header: x-session-id หรือ Authorization: Bearer <id>
function getSessionId(req) {
  const x = req.headers["x-session-id"];
  if (x) return String(x).trim();

  const auth = req.headers["authorization"];
  if (!auth) return null;
  const m = String(auth).match(/^Bearer\s+(.+)$/i);
  return m ? m[1].trim() : null;
}

// เหมือน “ยามหน้าประตู”: ถ้าไม่มีบัตร หรือบัตรใช้ไม่ได้ -> ไม่ให้เข้า
async function authGuard(req, res, next) {
  try {
    const sessionId = getSessionId(req);
    if (!sessionId) return res.status(401).json({ ok: false, message: "Missing session" });

    // ใช้ RequireSession (จะ THROW ถ้าไม่ valid)
    const s = await requireSession(sessionId);

    // เก็บไว้ใช้ต่อใน route
    req.sessionId = sessionId;
    req.session = s;

    next();
  } catch (e) {
    return res.status(401).json({ ok: false, message: e.message || "Unauthorized" });
  }
}

function getSessionContext(req) {
  const session = req.session || {};
  const userId = session.UserId || session.userId || null;
  const plantId = session.PlantId || session.plantId || null;
  if (!userId || !plantId) {
    throw new Error("Invalid session context");
  }
  return { userId, plantId };
}

/* =========================
   AUTH
========================= */

// เอาไว้เช็คว่า email นี้มีจริงไหม และมี plant อะไรบ้าง
app.post("/auth/begin", async (req, res) => {
  try {
    const { email } = req.body || {};
    const r = await loginBegin(String(email || "").trim());
    res.json({ ok: true, ...r });
  } catch (e) {
    res.status(400).json({ ok: false, message: e.message });
  }
});

// ล็อกอิน -> ได้ sessionId
app.post("/auth/login", async (req, res) => {
  try {
    const { email, password, plantId } = req.body || {};

    // ถ้าไม่ส่ง plantId มา เราจะเลือก plant แรกให้เอง (กันงง)
    let pid = plantId;
    if (!pid) {
      const begin = await loginBegin(String(email || "").trim());
      if (!begin.plants.length) return res.status(400).json({ ok: false, message: "No plant access" });
      pid = begin.plants[0].PlantId;
    }

    const sess = await loginCreateSession({
      email: String(email || "").trim(),
      password: String(password || ""),
      plantId: pid,
      clientIp: req.ip,
      userAgent: req.headers["user-agent"] || "",
    });

    res.json({ ok: true, ...sess });
  } catch (e) {
    res.status(401).json({ ok: false, message: e.message });
  }
});

// ดูข้อมูล session ปัจจุบัน
app.get("/auth/me", async (req, res) => {
  try {
    const sessionId = getSessionId(req);
    if (!sessionId) return res.status(401).json({ ok: false, message: "Missing session" });

    const v = await validateSession(sessionId);
    res.json({ ok: true, data: v });
  } catch (e) {
    res.status(400).json({ ok: false, message: e.message });
  }
});

// เปลี่ยน plant
app.post("/auth/switch-plant", authGuard, async (req, res) => {
  try {
    const { plantId } = req.body || {};
    const r = await switchPlant(req.sessionId, String(plantId || "").trim());
    res.json({ ok: true, data: r });
  } catch (e) {
    res.status(400).json({ ok: false, message: e.message });
  }
});

// logout
app.post("/auth/logout", authGuard, async (req, res) => {
  try {
    const { reason } = req.body || {};
    const r = await logout(req.sessionId, reason || "logout");
    res.json({ ok: true, data: r });
  } catch (e) {
    res.status(400).json({ ok: false, message: e.message });
  }
});

/* =========================
   ASSETS (ล็อกอินก่อนถึงเข้าได้)
========================= */

app.get("/assets", authGuard, async (req, res) => {
  try {
    const data = await assetsList({
      page: req.query.page,
      pageSize: req.query.pageSize,
      search: req.query.search,
    });
    res.json({ ok: true, ...data });
  } catch (e) {
    res.status(400).json({ ok: false, message: e.message });
  }
});

app.get("/assets/no-image", authGuard, async (req, res) => {
  try {
    const data = await assetsNoImage({
      page: req.query.page,
      pageSize: req.query.pageSize,
      search: req.query.search,
    });
    res.json({ ok: true, ...data });
  } catch (e) {
    res.status(400).json({ ok: false, message: e.message });
  }
});

app.get("/assets/sap-mismatch", authGuard, async (req, res) => {
  try {
    const data = await assetsSapMismatch({
      page: req.query.page,
      pageSize: req.query.pageSize,
      search: req.query.search,
    });
    res.json({ ok: true, ...data });
  } catch (e) {
    res.status(400).json({ ok: false, message: e.message });
  }
});

app.get("/assets/:assetId", authGuard, async (req, res) => {
  try {
    const r = await assetDetail(req.params.assetId);
    res.json({ ok: true, ...r });
  } catch (e) {
    res.status(400).json({ ok: false, message: e.message });
  }
});

app.post(
  "/assets/:assetId/images",
  authGuard,
  imageUpload.single("image"),
  async (req, res) => {
    try {
      const assetId = String(req.params.assetId || "").trim();
      if (!req.file || !req.file.buffer || !req.file.buffer.length) {
        return res.status(400).json({ ok: false, message: "Missing image file" });
      }

      const isPrimary =
        String(req.body?.isPrimary || "").trim().toLowerCase() === "1" ||
        String(req.body?.isPrimary || "").trim().toLowerCase() === "true";

      const saved = await saveAssetImage({
        assetId,
        originalName: req.file.originalname,
        mimeType: req.file.mimetype,
        buffer: req.file.buffer,
      });

      try {
        const image = await assetAddImage({
          assetId,
          fileUrl: saved.fileUrl,
          isPrimary,
        });

        return res.json({
          ok: true,
          image,
          file: {
            provider: saved.provider || "local",
            fileUrl: saved.fileUrl,
          },
        });
      } catch (dbErr) {
        if (saved && typeof saved.cleanup === "function") {
          await saved.cleanup();
        }
        throw dbErr;
      }
    } catch (e) {
      return res.status(400).json({ ok: false, message: e.message });
    }
  }
);

/* =========================
   SAP IMPORT (ทดสอบแบบกดเอง)
========================= */

// กด import ตอนนี้ (อ่านไฟล์จากโฟลเดอร์ตาม ENV แล้วเรียก SP)
/* =========================
   STOCKTAKE (MODULE 2)
========================= */

app.get("/stocktake/workspace", authGuard, async (req, res) => {
  try {
    const { userId, plantId } = getSessionContext(req);
    const stocktakeYear = toYear(req.query.year);
    const statusCode = String(req.query.statusCode || "").trim() || null;
    const search = String(req.query.search || "").trim() || null;

    const stocktakeId = await stocktakeGetOrCreate({
      plantId,
      stocktakeYear,
      userId,
    });

    const [summaryRs, detailRs, exportRs, config] = await Promise.all([
      stocktakeReportSummary({ plantId, stocktakeYear }),
      stocktakeReportDetail({ plantId, stocktakeYear, statusCode, search }),
      stocktakeReportExport3Tabs({ plantId, stocktakeYear, search }),
      stocktakeGetConfig({ plantId, stocktakeYear }),
    ]);

    const summary = summaryRs[0] || [];
    const details = detailRs[0] || [];
    const pendingItems = (exportRs[2] || []).map((x) => ({
      AssetNo: x.AssetNo,
      AssetName: x.AssetName,
      BookValue: x.BookValue,
      NoteText: x.NoteText,
      CountedAt: x.CountedAt,
    }));

    return res.json({
      ok: true,
      data: {
        stocktakeId,
        plantId,
        stocktakeYear,
        config: config
          ? {
              stocktakeYearConfigId: config.StocktakeYearConfigId,
              isOpen: Boolean(config.IsOpen),
              reportGeneratedAt: config.ReportGeneratedAt || null,
              closedAt: config.ClosedAt || null,
              closedByUserId: config.ClosedByUserId || null,
            }
          : null,
        summary,
        details,
        pendingItems,
      },
    });
  } catch (e) {
    return res.status(400).json({ ok: false, message: e.message });
  }
});

app.post("/stocktake/scan", authGuard, imageUpload.single("image"), async (req, res) => {
  try {
    const { userId, plantId } = getSessionContext(req);
    const stocktakeYear = toYear(req.body?.year);
    const assetId = String(req.body?.assetId || "").trim();
    const statusCode = String(req.body?.statusCode || "").trim();
    const countMethod = String(req.body?.countMethod || "").trim() || "QR";
    const noteText = String(req.body?.noteText || "").trim() || null;

    if (!assetId) return res.status(400).json({ ok: false, message: "assetId is required" });
    if (!statusCode) return res.status(400).json({ ok: false, message: "statusCode is required" });
    if (!req.file || !req.file.buffer || !req.file.buffer.length) {
      return res.status(400).json({ ok: false, message: "Image file is required for stocktake scan" });
    }

    const stocktakeId = await stocktakeGetOrCreate({
      plantId,
      stocktakeYear,
      userId,
    });

    const saved = await saveAssetImage({
      assetId,
      originalName: req.file.originalname,
      mimeType: req.file.mimetype,
      buffer: req.file.buffer,
    });

    try {
      const stocktakeItemId = await stocktakeScan({
        stocktakeId,
        assetId,
        statusCode,
        countedByUserId: userId,
        countMethod,
        noteText,
      });

      const stocktakeItemImageId = await stocktakeAddImage({
        stocktakeItemId,
        fileUrl: saved.fileUrl,
      });

      return res.json({
        ok: true,
        data: {
          stocktakeId,
          stocktakeItemId,
          stocktakeItemImageId,
          fileUrl: saved.fileUrl,
        },
      });
    } catch (dbErr) {
      if (saved && typeof saved.cleanup === "function") {
        await saved.cleanup();
      }
      throw dbErr;
    }
  } catch (e) {
    return res.status(400).json({ ok: false, message: e.message });
  }
});

app.post("/stocktake/import", authGuard, importUpload.single("file"), async (req, res) => {
  try {
    const { userId, plantId } = getSessionContext(req);
    const stocktakeYear = toYear(req.body?.year);
    const overrideMethod = String(req.body?.countMethod || "")
      .trim()
      .toUpperCase();

    if (!req.file || !req.file.buffer || !req.file.buffer.length) {
      return res.status(400).json({ ok: false, message: "Import file is required" });
    }

    const rows = parseStocktakeImportFile({
      originalName: req.file.originalname,
      buffer: req.file.buffer,
    });

    if (!rows.length) {
      return res.status(400).json({ ok: false, message: "No valid rows found in import file" });
    }

    const items = overrideMethod
      ? rows.map((x) => ({ ...x, CountMethod: overrideMethod }))
      : rows;

    const stocktakeId = await stocktakeGetOrCreate({
      plantId,
      stocktakeYear,
      userId,
    });

    const imported = await stocktakeImportCountJson({
      stocktakeId,
      importedByUserId: userId,
      items,
    });

    return res.json({
      ok: true,
      data: {
        stocktakeId,
        parsedRows: items.length,
        importedRows: Number(imported?.ImportedRows || 0),
      },
    });
  } catch (e) {
    return res.status(400).json({ ok: false, message: e.message });
  }
});

app.post("/stocktake/close-year", authGuard, async (req, res) => {
  try {
    const { userId, plantId } = getSessionContext(req);
    const stocktakeYear = toYear(req.body?.year);

    await stocktakeCloseYear({
      plantId,
      stocktakeYear,
      closedByUserId: userId,
    });

    return res.json({
      ok: true,
      data: {
        plantId,
        stocktakeYear,
      },
    });
  } catch (e) {
    return res.status(400).json({ ok: false, message: e.message });
  }
});

app.post("/stocktake/open-next-year", authGuard, async (req, res) => {
  try {
    const { userId, plantId } = getSessionContext(req);
    const fromYear = toYear(req.body?.fromYear || req.body?.year);
    const toYearValue = req.body?.toYear ? toYear(req.body?.toYear) : fromYear + 1;

    const r = await stocktakeOpenNextYearWithCarryPending({
      plantId,
      fromYear,
      toYear: toYearValue,
      createdByUserId: userId,
    });

    return res.json({ ok: true, data: r });
  } catch (e) {
    return res.status(400).json({ ok: false, message: e.message });
  }
});

app.post("/sap/import-now", authGuard, async (req, res) => {
  try {
    const r = await importSapFiles();
    const successCount = Number(r?.summary?.successCount || 0);
    if (successCount <= 0) {
      return res.status(400).json({
        ok: false,
        message: "SAP import failed for all configured files",
        ...r,
      });
    }
    res.json({ ok: true, ...r });
  } catch (e) {
    res.status(400).json({ ok: false, message: e.message });
  }
});

const port = Number(process.env.PORT || 3001);
app.listen(port, () => {
  console.log(`API running on http://localhost:${port}`);
  // เริ่ม job import ตามเวลาที่ตั้งไว้ใน ENV
  startSapImportJob();
});
