require("dotenv").config();
const {
    loginBegin,
    loginCreateSession,
    validateSession,
    logout,
    switchPlant,
    requireSession,
} = require("../services/auth.service");

async function main() {
    const email = (process.env.TEST_EMAIL || "").trim();
    const password = process.env.TEST_PASSWORD || "";

    const begin = await loginBegin(email);
    console.log("user:", begin.user);
    console.log("plants:", begin.plants);

    const plantId = begin.plants[0].PlantId;

    const sess = await loginCreateSession({
        email,
        password,
        plantId,
        clientIp: "127.0.0.1",
        userAgent: "local-test",
    });
    console.log("session:", sess);

    console.log("validateSession:", await validateSession(sess.sessionId));
    console.log("requireSession:", await requireSession(sess.sessionId));
    console.log("switchPlant:", await switchPlant(sess.sessionId, plantId));
    console.log("logout:", await logout(sess.sessionId, "test logout"));
    console.log("validateSession(after logout):", await validateSession(sess.sessionId));
}

main().catch((e) => {
    console.error(e);
    process.exit(1);
});
