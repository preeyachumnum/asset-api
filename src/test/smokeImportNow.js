require("dotenv").config();

function getBaseUrl() {
  const fromEnv = String(process.env.TEST_BASE_URL || "").trim();
  if (fromEnv) return fromEnv.replace(/\/+$/, "");
  const port = Number(process.env.PORT || 3001);
  return `http://localhost:${port}`;
}

function must(v, name) {
  const s = String(v || "").trim();
  if (!s) throw new Error(`Missing ${name} in env`);
  return s;
}

async function callApi(baseUrl, method, path, { body, sessionId } = {}) {
  const headers = { "content-type": "application/json" };
  if (sessionId) headers["x-session-id"] = sessionId;

  const r = await fetch(`${baseUrl}${path}`, {
    method,
    headers,
    body: body ? JSON.stringify(body) : undefined,
  });

  const text = await r.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = { raw: text };
  }

  return { status: r.status, ok: r.ok, data };
}

async function sleep(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function callApiWithRetry(baseUrl, method, path, options = {}, retries = 5) {
  let lastErr = null;
  for (let i = 0; i < retries; i += 1) {
    try {
      return await callApi(baseUrl, method, path, options);
    } catch (e) {
      lastErr = e;
      await sleep(500);
    }
  }
  if (lastErr && String(lastErr.message || "").includes("fetch failed")) {
    throw new Error(
      `Request failed: cannot reach ${baseUrl}. Start API first with \"npm run dev\"`
    );
  }
  throw lastErr || new Error("Request failed");
}

async function main() {
  const baseUrl = getBaseUrl();
  const email = must(process.env.TEST_EMAIL, "TEST_EMAIL");
  const password = must(process.env.TEST_PASSWORD, "TEST_PASSWORD");
  const plantId = String(process.env.TEST_PLANT_ID || process.env.TEST_PLANTID || "").trim();

  let sessionId = null;
  try {
    console.log("smoke: baseUrl", baseUrl);
    console.log("smoke: sap source", {
      SAP_DROP_DIR: process.env.SAP_DROP_DIR || "",
      SAP_FILES: process.env.SAP_FILES || "",
    });

    console.log("smoke: login");
    const login = await callApiWithRetry(baseUrl, "POST", "/auth/login", {
      body: {
        email,
        password,
        ...(plantId ? { plantId } : {}),
      },
    });

    if (!login.ok || !login.data || login.data.ok !== true || !login.data.sessionId) {
      throw new Error(`login failed: ${JSON.stringify(login)}`);
    }
    sessionId = login.data.sessionId;
    console.log("smoke: session ok", sessionId);

    console.log("smoke: call /sap/import-now");
    const imp = await callApiWithRetry(baseUrl, "POST", "/sap/import-now", {
      sessionId,
      body: {},
    });

    if (!imp.ok || !imp.data || imp.data.ok !== true) {
      throw new Error(`import-now failed: ${JSON.stringify(imp)}`);
    }

    const results = Array.isArray(imp.data.results) ? imp.data.results : [];
    const okCount = results.filter((x) => x && x.ok).length;
    const failCount = results.length - okCount;
    if (okCount <= 0) {
      throw new Error(`import-now finished with 0 success files: ${JSON.stringify(imp.data)}`);
    }
    console.log("smoke: import done", { files: results.length, okCount, failCount });
  } finally {
    if (sessionId) {
      const out = await callApi(baseUrl, "POST", "/auth/logout", {
        sessionId,
        body: { reason: "smoke test import-now" },
      }).catch(() => null);
      if (out) console.log("smoke: logout status", out.status);
    }
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
