require("dotenv").config();
const express = require("express");
const cors = require("cors");

const {
  assetsList,
  assetsNoImage,
  assetDetail,
  assetsSapMismatch,
} = require("./services/assets.service");
const {
  loginBegin,
  loginCreateSession,
  validateSession,
  logout,
  switchPlant,
  requireSession,
} = require("./services/auth.service");

const { importSapFiles } = require("./services/sapImport.service");
const { startSapImportJob } = require("./jobs/sapImport.job");

const app = express();
app.use(
  cors({
    origin: true,
    credentials: true,
  })
);
app.use(express.json());

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

/* =========================
   SAP IMPORT (ทดสอบแบบกดเอง)
========================= */

// กด import ตอนนี้ (อ่านไฟล์จากโฟลเดอร์ตาม ENV แล้วเรียก SP)
app.post("/sap/import-now", authGuard, async (req, res) => {
  try {
    const r = await importSapFiles();
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
