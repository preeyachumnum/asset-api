require("dotenv").config();
const { execProc } = require("../db/execProc");

async function main() {
  const rs = await execProc("dbo.spAssetsList");
  console.log("rows:", (rs[0] && rs[0].length) || 0);
  console.log("first row:", (rs[0] && rs[0][0]) || null);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
