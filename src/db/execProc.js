const { sql, getPool } = require("./mssql");

const p = {
  uuid: (v) => ({ type: sql.UniqueIdentifier, value: v }),
  int: (v) => ({ type: sql.Int, value: v }),
  bit: (v) => ({ type: sql.Bit, value: v }),
  nvarchar: (len, v) => ({ type: sql.NVarChar(len), value: v }),
  ntext: (v) => ({ type: sql.NVarChar(sql.MAX), value: v }),
};

// แบบเดิม: คืนแค่ recordsets (Assets ใช้อันนี้ได้เหมือนเดิม)
async function execProc(procName, inputs = {}) {
  const r = await execProcRaw(procName, inputs);
  return r.recordsets || [];
}

// แบบใหม่: คืน recordsets + output + rowsAffected (Auth/CreateSession ต้องใช้อันนี้)
async function execProcRaw(procName, inputs = {}, outputs = {}) {
  const pool = await getPool();
  const req = pool.request();

  for (const [name, param] of Object.entries(inputs)) {
    req.input(name, param.type, param.value);
  }
  for (const [name, type] of Object.entries(outputs)) {
    req.output(name, type);
  }

  const r = await req.execute(procName);
  return { recordsets: r.recordsets || [], output: r.output || {}, rowsAffected: r.rowsAffected || [] };
}

module.exports = { execProc, execProcRaw, p, sql };
