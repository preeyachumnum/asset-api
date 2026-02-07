const bcrypt = require("bcryptjs");
const { execProc, execProcRaw, p, sql } = require("../db/execProc");

function isGuid(s) {
    return /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(
        s
    );
}

// 1) ขอข้อมูล user + plant ที่เข้าได้ (dbo.spAuth_LoginBegin @Email)
async function loginBegin(email) {
    const rs = await execProc("dbo.spAuth_LoginBegin", {
        Email: p.nvarchar(320, email),
    });

    return {
        user: (rs[0] && rs[0][0]) || null,
        plants: rs[1] || [],
    };
}

// 2) ตรวจ password แล้วสร้าง session (dbo.spAuth_CreateSession OUTPUT)
async function loginCreateSession({ email, password, plantId, clientIp = null, userAgent = null }) {
    if (!isGuid(plantId)) throw new Error("plantId must be GUID");

    const { user, plants } = await loginBegin(email);
    if (!user) throw new Error("Email not found");
    if (!user.IsActive) throw new Error("User is inactive");
    if (!user.PasswordHash) throw new Error("User has no PasswordHash");

    const ok = await bcrypt.compare(password, user.PasswordHash);
    if (!ok) throw new Error("Password incorrect");

    const allowed = plants.some((x) => x.PlantId === plantId);
    if (!allowed) throw new Error("User has no access to this plant");

    const r = await execProcRaw(
        "dbo.spAuth_CreateSession",
        {
            UserId: p.uuid(user.UserId),
            PlantId: p.uuid(plantId),
            ClientIp: p.nvarchar(64, clientIp),
            UserAgent: p.nvarchar(512, userAgent),
            ExpiresInMinutes: p.int(480),
        },
        {
            SessionId: sql.UniqueIdentifier,
            ExpiresAt: sql.DateTime2(0),
        }
    );

    return {
        sessionId: r.output.SessionId,
        expiresAt: r.output.ExpiresAt,
        user: { userId: user.UserId, email: user.Email, displayName: user.DisplayName },
    };
}

// 3) เช็ค session ยังใช้ได้ไหม (dbo.spAuth_ValidateSession @SessionId)
async function validateSession(sessionId) {
    if (!isGuid(sessionId)) throw new Error("sessionId must be GUID");

    const rs = await execProc("dbo.spAuth_ValidateSession", {
        SessionId: p.uuid(sessionId),
    });

    return (rs[0] && rs[0][0]) || null;
}

// 4) ออกจากระบบ/ปิด session (dbo.spAuth_Logout @SessionId, @Reason)
async function logout(sessionId, reason = null) {
    if (!isGuid(sessionId)) throw new Error("sessionId must be GUID");

    const rs = await execProc("dbo.spAuth_Logout", {
        SessionId: p.uuid(sessionId),
        Reason: p.nvarchar(200, reason),
    });

    return (rs[0] && rs[0][0]) || null;
}

// 5) เปลี่ยนโรงงานใน session เดิม (dbo.spAuth_SwitchPlant @SessionId, @PlantId)
async function switchPlant(sessionId, plantId) {
    if (!isGuid(sessionId)) throw new Error("sessionId must be GUID");
    if (!isGuid(plantId)) throw new Error("plantId must be GUID");

    const rs = await execProc("dbo.spAuth_SwitchPlant", {
        SessionId: p.uuid(sessionId),
        PlantId: p.uuid(plantId),
    });

    return (rs[0] && rs[0][0]) || null;
}

// 6) บังคับว่า session ต้อง valid ไม่งั้น THROW (dbo.spAuth_RequireSession @SessionId)
async function requireSession(sessionId) {
    if (!isGuid(sessionId)) throw new Error("sessionId must be GUID");

    const rs = await execProc("dbo.spAuth_RequireSession", {
        SessionId: p.uuid(sessionId),
    });

    return (rs[0] && rs[0][0]) || null;
}

module.exports = {
    loginBegin,
    loginCreateSession,
    validateSession,
    logout,
    switchPlant,
    requireSession,
};
