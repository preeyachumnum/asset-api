require("dotenv").config();
const {
  assetsList,
  assetsNoImage,
  assetDetail,
} = require("../services/assets.service");

async function main() {
  const list = await assetsList();
  console.log("assetsList rows:", list.length);

  const noImg = await assetsNoImage();
  console.log("assetsNoImage rows:", noImg.length);

  const first = list[0];
  if (!first) return;

  const detail = await assetDetail(first.AssetId);
  console.log("assetDetail asset:", detail.asset);
  console.log("assetDetail images:", detail.images.length);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
