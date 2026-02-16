const localProvider = require("./providers/localImageStorage.provider");

const providers = {
  local: localProvider,
};

function resolveProvider() {
  const key = String(process.env.ASSET_IMAGE_STORAGE_PROVIDER || "local")
    .trim()
    .toLowerCase();

  const provider = providers[key];
  if (!provider) {
    throw new Error(`Unsupported ASSET_IMAGE_STORAGE_PROVIDER: ${key}`);
  }
  return provider;
}

function getImageStaticMount() {
  const provider = resolveProvider();
  if (typeof provider.getStaticMount !== "function") return null;
  return provider.getStaticMount();
}

async function saveAssetImage(args) {
  const provider = resolveProvider();
  return provider.saveAssetImage(args);
}

module.exports = { saveAssetImage, getImageStaticMount };
