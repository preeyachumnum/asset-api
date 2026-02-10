const cron = require("node-cron");
const { importSapFiles } = require("../services/sapImport.service");

function envBool(v, def = false) {
  const s = String(v ?? "").trim().toLowerCase();
  if (!s) return def;
  return s === "true" || s === "1" || s === "yes";
}

let running = false;

function startSapImportJob() {
  const enabled = envBool(process.env.SAP_IMPORT_ENABLED, false);
  if (!enabled) {
    console.log("SAP import job: disabled");
    return null;
  }

  const expr = String(process.env.SAP_CRON || "0 2 * * *").trim();
  const tz = String(process.env.SAP_TIMEZONE || "Asia/Bangkok").trim();

  const task = cron.schedule(
    expr,
    async () => {
      if (running) {
        console.log("SAP import job: skip (still running)");
        return;
      }

      running = true;
      try {
        console.log("SAP import job: start");
        const r = await importSapFiles();
        console.log("SAP import job: done", JSON.stringify(r));
      } catch (e) {
        console.log("SAP import job: error", e.message || String(e));
      } finally {
        running = false;
      }
    },
    { timezone: tz }
  );

  task.start();
  console.log(`SAP import job: scheduled (${expr}) tz=${tz}`);
  return task;
}

module.exports = { startSapImportJob };
