const express = require('express');
const { google } = require('googleapis');
const path = require('path');
const multer = require('multer');
const { parse: csvParse } = require('csv-parse/sync');
const { Pool } = require('pg');

const app = express();
const PORT = process.env.PORT || 3000;

const SPREADSHEET_ID = '12nkWKQfAfR70MMapo-pa0jT2PQD4SEY8M6Icse9PmSQ';

// ----------------------------------------------------------------------------
// PostgreSQL connection (graceful degradation if DATABASE_URL missing)
// ----------------------------------------------------------------------------
let pgPool = null;
let dbReady = false;

if (process.env.DATABASE_URL) {
  pgPool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DATABASE_URL.includes('localhost') ? false : { rejectUnauthorized: false },
  });
  pgPool.on('error', (err) => console.error('[pg] pool error:', err.message));
} else {
  console.warn('[ads] DATABASE_URL not set — /api/ads/* endpoints disabled');
}

async function initAdsSchema() {
  if (!pgPool) return;
  const sql = `
    CREATE TABLE IF NOT EXISTS card_transactions (
      id                     SERIAL PRIMARY KEY,
      person                 VARCHAR(50),
      card_id                VARCHAR(100),
      card_number            VARCHAR(30),
      transaction_serial     VARCHAR(100) UNIQUE NOT NULL,
      original_serial        VARCHAR(100),
      transaction_amount     NUMERIC(14,2),
      transaction_currency   VARCHAR(10),
      authorized_amount      NUMERIC(14,4),
      authorized_currency    VARCHAR(10),
      authorization_fee      NUMERIC(14,4),
      cross_border_fee       NUMERIC(14,4),
      settlement_amount      NUMERIC(14,4),
      merchant_name          VARCHAR(255),
      type                   VARCHAR(30),
      status                 VARCHAR(30),
      description            TEXT,
      transaction_time       TIMESTAMP,
      uploaded_at            TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_card_tx_person ON card_transactions(person);
    CREATE INDEX IF NOT EXISTS idx_card_tx_time   ON card_transactions(transaction_time);
    CREATE INDEX IF NOT EXISTS idx_card_tx_status ON card_transactions(status);

    CREATE TABLE IF NOT EXISTS wallet_transactions (
      id                     SERIAL PRIMARY KEY,
      transaction_id         VARCHAR(100) UNIQUE NOT NULL,
      account_id             VARCHAR(100),
      account_name           VARCHAR(100),
      account_type           VARCHAR(30),
      transaction_target     VARCHAR(30),
      currency               VARCHAR(10),
      amount                 NUMERIC(14,4),
      balance_before         NUMERIC(14,4),
      balance_after          NUMERIC(14,4),
      business_order_no      VARCHAR(100),
      business_type          VARCHAR(50),
      operation_type         VARCHAR(50),
      direction              VARCHAR(10),
      remarks                VARCHAR(255),
      transaction_time       TIMESTAMP,
      uploaded_at            TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_wallet_tx_direction ON wallet_transactions(direction);
    CREATE INDEX IF NOT EXISTS idx_wallet_tx_btype     ON wallet_transactions(business_type);
    CREATE INDEX IF NOT EXISTS idx_wallet_tx_time      ON wallet_transactions(transaction_time);
  `;
  try {
    await pgPool.query(sql);
    dbReady = true;
    console.log('[ads] card_transactions + wallet_transactions schema ready');
  } catch (err) {
    console.error('[ads] schema init failed:', err.message);
  }
}
initAdsSchema();

// Guard middleware for /api/ads/* — returns 503 if DB not configured/ready
const requireDb = (req, res, next) => {
  if (!pgPool) return res.status(503).json({ error: 'Database not configured (DATABASE_URL missing)' });
  if (!dbReady) return res.status(503).json({ error: 'Database schema not ready — try again shortly' });
  next();
};

// Multer — 5MB CSV upload, memory storage (no disk writes on Railway)
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 5 * 1024 * 1024 },
});

// Service account credentials from environment variable
let auth;
function getAuth() {
  if (auth) return auth;
  const creds = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT || '{}');
  auth = new google.auth.GoogleAuth({
    credentials: creds,
    scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
  });
  return auth;
}

async function getSheetData(sheetName, range) {
  const authClient = await getAuth().getClient();
  const sheets = google.sheets({ version: 'v4', auth: authClient });
  const fullRange = range ? `'${sheetName}'!${range}` : `'${sheetName}'`;
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: fullRange,
  });
  return res.data.values || [];
}

function parseAmount(val) {
  if (!val) return 0;
  const cleaned = String(val).replace(/[₱,\s]/g, '');
  const num = parseFloat(cleaned);
  return isNaN(num) ? 0 : num;
}

function parseRows(rows, headerRow) {
  if (!rows || rows.length === 0) return [];
  const headers = headerRow || rows[0];
  const dataRows = headerRow ? rows : rows.slice(1);
  return dataRows
    .filter(r => r[0] && r[0] !== 'TOTAL' && r[0] !== '')
    .map(r => {
      const obj = {};
      headers.forEach((h, i) => {
        obj[h] = r[i] || '';
      });
      return obj;
    });
}

// Serve static files
app.use(express.static(path.join(__dirname, 'public')));

// API: OPEX data
app.get('/api/opex', async (req, res) => {
  try {
    const rows = await getSheetData('OPEX Log');
    const data = parseRows(rows);
    const result = data.map(r => ({
      date: r['Date'] || '',
      person: r['Employee Name'] || '',
      department: r['Department'] || '',
      website: r['Website'] || '',
      category: r['Expense Category'] || '',
      description: r['Description'] || '',
      txnNo: r['Transaction No.'] || '',
      quantity: parseInt(r['Quantity']) || 1,
      amount: parseAmount(r['Amount']),
      total: parseAmount(r['Total']),
      payment: r['Mode of Payment'] || '',
      status: r['Status'] || '',
      approvedBy: r['Approved By'] || '',
      notes: r['Notes'] || '',
      type: 'OPEX'
    }));
    res.json(result);
  } catch (err) {
    console.error('OPEX error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// API: Pre-operational data
app.get('/api/preop', async (req, res) => {
  try {
    const rows = await getSheetData('Liquidation - Pre-operational Expenses');
    const data = parseRows(rows);
    const result = data.map(r => ({
      date: r['Date'] || '',
      person: r['Employee Name'] || '',
      department: r['Department'] || '',
      website: '',
      category: r['Expense Category'] || '',
      description: r['Description'] || '',
      txnNo: r['Transaction no.'] || '',
      quantity: parseInt(r['Quantity']) || 1,
      amount: parseAmount(r['Amount']),
      total: parseAmount(r['Total']),
      payment: r['Mode of Payment'] || '',
      status: r['Status'] || '',
      approvedBy: r['Approved By'] || '',
      notes: r['Notes'] || '',
      type: 'Pre-Operation'
    }));
    res.json(result);
  } catch (err) {
    console.error('PreOp error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// API: Hard Assets data
app.get('/api/hardassets', async (req, res) => {
  try {
    const rows = await getSheetData('Liquidation - Hard Assets');
    const data = parseRows(rows);
    const result = data.map(r => ({
      date: r['Date'] || '',
      person: r['Employee Name'] || '',
      department: r['Department'] || '',
      website: '',
      category: r['Expense Category'] || '',
      description: r['Description'] || '',
      txnNo: r['Transaction no.'] || '',
      quantity: parseInt(r['Quantity']) || 1,
      amount: parseAmount(r['Amount']),
      total: parseAmount(r['Total']),
      payment: r['Mode of Payment'] || '',
      status: r['Status'] || '',
      approvedBy: r['Approved By'] || '',
      notes: r['Notes'] || '',
      type: 'Hard Assets'
    }));
    res.json(result);
  } catch (err) {
    console.error('Hard Assets error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// API: All combined
app.get('/api/all', async (req, res) => {
  try {
    const [opexRows, preopRows, hardRows] = await Promise.all([
      getSheetData('OPEX Log'),
      getSheetData('Liquidation - Pre-operational Expenses'),
      getSheetData('Liquidation - Hard Assets'),
    ]);

    const parse = (rows, type) => {
      const data = parseRows(rows);
      return data.map(r => ({
        date: r['Date'] || '',
        person: r['Employee Name'] || '',
        department: r['Department'] || '',
        website: r['Website'] || '',
        category: r['Expense Category'] || '',
        description: r['Description'] || '',
        txnNo: r['Transaction no.'] || r['Transaction No.'] || '',
        quantity: parseInt(r['Quantity']) || 1,
        amount: parseAmount(r['Amount']),
        total: parseAmount(r['Total']),
        payment: r['Mode of Payment'] || '',
        status: r['Status'] || '',
        approvedBy: r['Approved By'] || '',
        notes: r['Notes'] || '',
        type
      }));
    };

    const all = [
      ...parse(opexRows, 'OPEX'),
      ...parse(preopRows, 'Pre-Operation'),
      ...parse(hardRows, 'Hard Assets'),
    ];

    res.json(all);
  } catch (err) {
    console.error('All error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// API: Summary stats
app.get('/api/summary', async (req, res) => {
  try {
    const [opexRows, preopRows, hardRows] = await Promise.all([
      getSheetData('OPEX Log'),
      getSheetData('Liquidation - Pre-operational Expenses'),
      getSheetData('Liquidation - Hard Assets'),
    ]);

    const sumTotal = (rows) => {
      const data = parseRows(rows);
      return data.reduce((sum, r) => sum + parseAmount(r['Total']), 0);
    };

    const opexTotal = sumTotal(opexRows);
    const preopTotal = sumTotal(preopRows);
    const hardTotal = sumTotal(hardRows);

    res.json({
      opex: opexTotal,
      preOperation: preopTotal,
      hardAssets: hardTotal,
      total: opexTotal + preopTotal + hardTotal,
      counts: {
        opex: parseRows(opexRows).length,
        preOperation: parseRows(preopRows).length,
        hardAssets: parseRows(hardRows).length,
      }
    });
  } catch (err) {
    console.error('Summary error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Master ledger
app.get('/api/master', async (req, res) => {
  try {
    const rows = await getSheetData('Master Ledger', 'A2:F3');
    if (rows.length >= 2) {
      const labels = rows[0];
      const values = rows[1];
      const result = {};
      labels.forEach((l, i) => {
        result[l] = values[i] || '';
      });
      res.json(result);
    } else {
      res.json({});
    }
  } catch (err) {
    console.error('Master error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// API: Ads Fund (USDT Transfers)
app.get('/api/adsfund', async (req, res) => {
  try {
    const rows = await getSheetData('USDT Transfers', 'A5:N50');
    const headers = rows[0];
    const data = rows.slice(1)
      .filter(r => r[0] && r[0] !== '' && r[0] !== '0')
      .map(r => ({
        no: parseInt(r[0]) || 0,
        date: r[1] || '',
        time: r[2] || '',
        recipient: r[3] || '',
        department: r[4] || '',
        category: r[5] || '',
        usdt: parseAmount(r[6]),
        orderNo: r[7] || '',
        rate: parseAmount(r[8]),
        php: parseAmount(r[9]),
        rateTime: r[10] || '',
        payment: r[11] || '',
        wallet: r[12] || '',
        network: r[13] || '',
      }));

    // Summary by recipient
    const byRecipient = {};
    data.forEach(r => {
      if (r.recipient) {
        if (!byRecipient[r.recipient]) byRecipient[r.recipient] = { usdt: 0, php: 0, count: 0 };
        byRecipient[r.recipient].usdt += r.usdt;
        byRecipient[r.recipient].php += r.php;
        byRecipient[r.recipient].count++;
      }
    });

    const totalUsdt = data.reduce((s, r) => s + r.usdt, 0);
    const totalPhp = data.reduce((s, r) => s + r.php, 0);

    res.json({
      transfers: data,
      totalUsdt,
      totalPhp,
      avgRate: totalUsdt > 0 ? totalPhp / totalUsdt : 0,
      count: data.length,
      byRecipient,
    });
  } catch (err) {
    console.error('Ads Fund error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// API: Full Master Ledger transactions (fund flow)
app.get('/api/ledger', async (req, res) => {
  try {
    const rows = await getSheetData('Master Ledger', 'A8:K200');
    const headers = rows[0];
    const data = rows.slice(1)
      .filter(r => r[0] && r[0] !== '')
      .map(r => ({
        no: parseInt(r[0]) || 0,
        date: r[1] || '',
        type: r[2] || '',
        source: r[3] || '',
        description: r[4] || '',
        person: r[5] || '',
        debit: parseAmount(r[6]),
        credit: parseAmount(r[7]),
        balance: parseAmount(r[8]),
        reference: r[9] || '',
        remarks: r[10] || '',
      }));
    res.json(data);
  } catch (err) {
    console.error('Ledger error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// API: Fund flow summary (remittance by site)
app.get('/api/fundflow', async (req, res) => {
  try {
    const rows = await getSheetData('Master Ledger', 'A8:K200');
    const txns = rows.slice(1).filter(r => r[0] && r[0] !== '');

    let totalRemittance = 0;
    let totalExpenses = 0;
    const bySite = {};
    const bySource = {};
    const daily = {};

    txns.forEach(r => {
      const type = r[2] || '';
      const source = r[3] || '';
      const person = r[5] || '';
      const debit = parseAmount(r[6]);
      const credit = parseAmount(r[7]);
      const date = r[1] || '';

      if (type === 'CREDIT') {
        const amount = credit || debit;
        totalRemittance += amount;
        // Extract site name from person field (e.g., "Neji - COW" -> "COW")
        let site = person;
        if (person.includes(' - ')) {
          site = person.split(' - ').pop().trim();
        }
        // Map to standard names
        const siteMap = {
          'COW': 'Cow88 (COW)',
          'City of Wins': 'Cow88 (COW)',
          'T2B': 'Time to Bet (T2B)',
          'Time 2 Bet': 'Time to Bet (T2B)',
          'RLM': 'Roll Em (RLM)',
          'Rollem': 'Roll Em (RLM)',
          'WFL': 'Win For Life (WFL)',
          'Win For Life': 'Win For Life (WFL)',
        };
        const mappedSite = siteMap[site] || site;
        bySite[mappedSite] = (bySite[mappedSite] || 0) + amount;
      } else if (type === 'DEBIT') {
        const amount = debit || credit;
        totalExpenses += amount;
        bySource[source] = (bySource[source] || 0) + amount;
      }

      // Daily flow
      if (date) {
        if (!daily[date]) daily[date] = { in: 0, out: 0 };
        if (type === 'CREDIT') daily[date].in += (credit || debit);
        if (type === 'DEBIT') daily[date].out += (debit || credit);
      }
    });

    const dailyArray = Object.entries(daily)
      .map(([date, v]) => ({ date, in: v.in, out: v.out, net: v.in - v.out }))
      .sort((a, b) => a.date.localeCompare(b.date));

    res.json({
      totalRemittance,
      totalExpenses,
      cashBalance: totalRemittance - totalExpenses,
      bySite,
      bySource,
      daily: dailyArray,
      transactionCount: txns.length,
    });
  } catch (err) {
    console.error('Fundflow error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================================
// ADS CARD STATEMENT TRACKER
// Facebook ads card transactions imported from Wallester CSV exports.
// Backed by PostgreSQL (card_transactions table). All endpoints guarded
// by requireDb middleware so the server still boots without DATABASE_URL.
// All responses are camelCase to match the frontend (public/ads.html).
// ============================================================================

// Helpers ---------------------------------------------------------------------

const ALLOWED_PERSONS = new Set(['Jason', 'Jomar', 'Mika', 'Ron', 'Shila']);

const toNum = (v) => {
  if (v === undefined || v === null || v === '') return null;
  const n = parseFloat(String(v).replace(/[,\s₱]/g, ''));
  return isNaN(n) ? null : n;
};

// "2026-04-19 05:04:07" (UTC) -> ISO timestamp Postgres can cast
const parseTxTime = (s) => {
  if (!s) return null;
  const trimmed = String(s).trim();
  if (!trimmed) return null;
  // Treat as UTC (CSV column is labeled UTC+0)
  const iso = trimmed.replace(' ', 'T') + 'Z';
  const d = new Date(iso);
  return isNaN(d.getTime()) ? null : d.toISOString();
};

// Extract last 4 digits from a masked card number ("537100******3077" -> "3077")
const extractLast4 = (cardNumber) => {
  if (!cardNumber) return null;
  const s = String(cardNumber).replace(/[^0-9]/g, '');
  if (s.length < 4) return s || null;
  return s.slice(-4);
};

// POST /api/ads/upload --------------------------------------------------------
app.post('/api/ads/upload', requireDb, upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded (field name: "file")' });
    const person = (req.body.person || '').trim();
    if (!ALLOWED_PERSONS.has(person)) {
      return res.status(400).json({ error: `Invalid person "${person}". Must be one of: ${[...ALLOWED_PERSONS].join(', ')}` });
    }

    let records;
    try {
      records = csvParse(req.file.buffer, {
        columns: true,
        skip_empty_lines: true,
        trim: true,
        bom: true,
      });
    } catch (parseErr) {
      return res.status(400).json({ error: `CSV parse failed: ${parseErr.message}` });
    }

    const total = records.length;
    let inserted = 0;
    let skipped = 0;
    const errors = [];

    const insertSql = `
      INSERT INTO card_transactions (
        person, card_id, card_number, transaction_serial, original_serial,
        transaction_amount, transaction_currency,
        authorized_amount, authorized_currency,
        authorization_fee, cross_border_fee, settlement_amount,
        merchant_name, type, status, description, transaction_time
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17
      )
      ON CONFLICT (transaction_serial) DO NOTHING
      RETURNING id
    `;

    const client = await pgPool.connect();
    try {
      for (let i = 0; i < records.length; i++) {
        const r = records[i];
        const serial = (r['Transaction serial number'] || '').trim();
        if (!serial) {
          skipped++;
          continue;
        }
        try {
          const result = await client.query(insertSql, [
            person,
            r['Card ID'] || null,
            r['Card number'] || null,
            serial,
            r['Original transaction serial number'] || null,
            toNum(r['Transaction amount']),
            r['Transaction currency'] || null,
            toNum(r['Authorized amount']),
            r['Authorized currency'] || null,
            toNum(r['Authorization fee']),
            toNum(r['Cross-border transaction fee']),
            toNum(r['Settlement amount']),
            r['Merchant name'] || null,
            r['Type'] || null,
            r['Status'] || null,
            r['Description'] || null,
            parseTxTime(r['Transaction time (UTC+0)']),
          ]);
          if (result.rowCount > 0) inserted++;
          else skipped++;
        } catch (rowErr) {
          skipped++;
          if (errors.length < 5) errors.push({ row: i + 2, error: rowErr.message });
        }
      }
    } finally {
      client.release();
    }

    res.json({ inserted, skipped, total, person, ...(errors.length ? { errors } : {}) });
  } catch (err) {
    console.error('Ads upload error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/ads/summary --------------------------------------------------------
app.get('/api/ads/summary', requireDb, async (req, res) => {
  try {
    // Totals exclude Failed; reversals excluded from totalPhp
    const overallSql = `
      SELECT
        COUNT(*)::int AS "totalTxns",
        COALESCE(SUM(CASE
          WHEN type = 'Authorization' AND status IN ('Authorized','Success')
          THEN transaction_amount ELSE 0 END), 0)::numeric AS "totalPhp",
        COALESCE(SUM(CASE
          WHEN status IN ('Authorized','Success')
          THEN authorized_amount ELSE 0 END), 0)::numeric AS "totalUsd",
        COALESCE(SUM(CASE
          WHEN status IN ('Authorized','Success')
          THEN COALESCE(authorization_fee,0) + COALESCE(cross_border_fee,0)
          ELSE 0 END), 0)::numeric AS "totalFeesUsd",
        COUNT(*) FILTER (WHERE status = 'Fail')::int AS "failedCount",
        COUNT(*) FILTER (WHERE type = 'Reversal')::int AS "reversalCount",
        COUNT(*) FILTER (WHERE status IN ('Authorized','Success'))::int AS "approvedCount",
        COUNT(DISTINCT card_number)::int AS "uniqueCards",
        MIN(transaction_time) AS "minTime",
        MAX(transaction_time) AS "maxTime"
      FROM card_transactions
    `;

    const byPersonSql = `
      SELECT
        person                                                           AS "person",
        COUNT(DISTINCT card_number)::int                                 AS "cards",
        COUNT(*)::int                                                    AS "txns",
        COALESCE(SUM(CASE
          WHEN type = 'Authorization' AND status IN ('Authorized','Success')
          THEN transaction_amount ELSE 0 END), 0)::numeric               AS "totalPhp",
        COALESCE(SUM(CASE
          WHEN status IN ('Authorized','Success')
          THEN authorized_amount ELSE 0 END), 0)::numeric                AS "totalUsd",
        COUNT(*) FILTER (WHERE status = 'Fail')::int                     AS "failed"
      FROM card_transactions
      WHERE person IS NOT NULL
      GROUP BY person
      ORDER BY "totalPhp" DESC
    `;

    const [overall, byPerson] = await Promise.all([
      pgPool.query(overallSql),
      pgPool.query(byPersonSql),
    ]);

    const o = overall.rows[0] || {};
    res.json({
      totalTxns: o.totalTxns || 0,
      totalPhp: Number(o.totalPhp) || 0,
      totalUsd: Number(o.totalUsd) || 0,
      totalFeesUsd: Number(o.totalFeesUsd) || 0,
      failedCount: o.failedCount || 0,
      reversalCount: o.reversalCount || 0,
      approvedCount: o.approvedCount || 0,
      uniqueCards: o.uniqueCards || 0,
      dateRange: { min: o.minTime, max: o.maxTime },
      byPerson: byPerson.rows.map(r => ({
        person: r.person,
        cards: r.cards,
        txns: r.txns,
        totalPhp: Number(r.totalPhp) || 0,
        totalUsd: Number(r.totalUsd) || 0,
        failed: r.failed,
      })),
    });
  } catch (err) {
    console.error('Ads summary error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/ads/transactions ---------------------------------------------------
app.get('/api/ads/transactions', requireDb, async (req, res) => {
  try {
    const { person, search } = req.query;
    const statusFilter = (req.query.status || '').toLowerCase();
    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    const offset = Math.max(parseInt(req.query.offset) || 0, 0);

    const where = [];
    const params = [];
    if (person) { params.push(person); where.push(`person = $${params.length}`); }
    // Map friendly status filter values to DB status/type semantics
    if (statusFilter === 'approved') {
      where.push(`status IN ('Authorized','Success')`);
    } else if (statusFilter === 'failed') {
      where.push(`status = 'Fail'`);
    } else if (statusFilter === 'reversed') {
      where.push(`type = 'Reversal'`);
    } else if (statusFilter === 'pending') {
      where.push(`status NOT IN ('Authorized','Success','Fail')`);
    } else if (statusFilter) {
      // Pass-through for exact DB status value
      params.push(req.query.status);
      where.push(`status = $${params.length}`);
    }
    if (search) {
      params.push('%' + search + '%');
      where.push(`merchant_name ILIKE $${params.length}`);
    }

    const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';
    params.push(limit);
    const limitIdx = params.length;
    params.push(offset);
    const offsetIdx = params.length;

    const sql = `
      SELECT
        id                                                   AS "id",
        person                                               AS "person",
        RIGHT(REGEXP_REPLACE(COALESCE(card_number,''), '[^0-9]', '', 'g'), 4) AS "cardLast4",
        card_number                                          AS "cardNumber",
        merchant_name                                        AS "merchant",
        transaction_amount                                   AS "php",
        authorized_amount                                    AS "usd",
        authorization_fee                                    AS "authorizationFee",
        cross_border_fee                                     AS "crossBorderFee",
        (COALESCE(authorization_fee,0) + COALESCE(cross_border_fee,0)) AS "feesUsd",
        type                                                 AS "type",
        status                                               AS "status",
        transaction_time                                     AS "time",
        description                                          AS "description"
      FROM card_transactions
      ${whereSql}
      ORDER BY transaction_time DESC NULLS LAST
      LIMIT $${limitIdx} OFFSET $${offsetIdx}
    `;

    const countSql = `SELECT COUNT(*)::int AS total FROM card_transactions ${whereSql}`;
    const countParams = params.slice(0, params.length - 2);

    const [rowsRes, countRes] = await Promise.all([
      pgPool.query(sql, params),
      pgPool.query(countSql, countParams),
    ]);

    const rows = rowsRes.rows.map(r => ({
      id: r.id,
      person: r.person,
      cardLast4: r.cardLast4 || null,
      cardNumber: r.cardNumber,
      merchant: r.merchant,
      php: r.php != null ? Number(r.php) : null,
      usd: r.usd != null ? Number(r.usd) : null,
      authorizationFee: r.authorizationFee != null ? Number(r.authorizationFee) : null,
      crossBorderFee: r.crossBorderFee != null ? Number(r.crossBorderFee) : null,
      feesUsd: r.feesUsd != null ? Number(r.feesUsd) : null,
      type: r.type,
      status: r.status,
      time: r.time ? new Date(r.time).toISOString() : null,
      description: r.description,
    }));

    res.json({
      total: countRes.rows[0].total,
      limit,
      offset,
      rows,
    });
  } catch (err) {
    console.error('Ads transactions error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/ads/by-card --------------------------------------------------------
app.get('/api/ads/by-card', requireDb, async (req, res) => {
  try {
    const sql = `
      SELECT
        RIGHT(REGEXP_REPLACE(COALESCE(card_number,''), '[^0-9]', '', 'g'), 4) AS "cardLast4",
        card_number                                                          AS "cardNumber",
        MAX(person)                                                          AS "person",
        COUNT(*)::int                                                        AS "txCount",
        COALESCE(SUM(CASE
          WHEN type = 'Authorization' AND status IN ('Authorized','Success')
          THEN transaction_amount ELSE 0 END), 0)::numeric                   AS "totalPhp",
        COALESCE(SUM(CASE
          WHEN status IN ('Authorized','Success')
          THEN authorized_amount ELSE 0 END), 0)::numeric                    AS "totalUsd",
        MIN(transaction_time)                                                AS "firstUsed",
        MAX(transaction_time)                                                AS "lastUsed"
      FROM card_transactions
      WHERE card_number IS NOT NULL
      GROUP BY card_number
      ORDER BY "totalPhp" DESC
    `;
    const { rows } = await pgPool.query(sql);
    res.json(rows.map(r => ({
      cardLast4: r.cardLast4 || null,
      cardNumber: r.cardNumber,
      person: r.person,
      txCount: r.txCount,
      totalPhp: Number(r.totalPhp) || 0,
      totalUsd: Number(r.totalUsd) || 0,
      firstUsed: r.firstUsed ? new Date(r.firstUsed).toISOString() : null,
      lastUsed: r.lastUsed ? new Date(r.lastUsed).toISOString() : null,
    })));
  } catch (err) {
    console.error('Ads by-card error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/ads/daily ----------------------------------------------------------
app.get('/api/ads/daily', requireDb, async (req, res) => {
  try {
    const sql = `
      SELECT
        TO_CHAR(DATE(transaction_time), 'YYYY-MM-DD') AS "date",
        COALESCE(SUM(transaction_amount), 0)::numeric AS "php",
        COALESCE(SUM(authorized_amount), 0)::numeric  AS "usd",
        COUNT(*)::int                                 AS "count"
      FROM card_transactions
      WHERE type = 'Authorization'
        AND status IN ('Authorized','Success')
        AND transaction_time IS NOT NULL
      GROUP BY DATE(transaction_time)
      ORDER BY DATE(transaction_time) ASC
    `;
    const { rows } = await pgPool.query(sql);
    res.json(rows.map(r => ({
      date: r.date,
      php: Number(r.php) || 0,
      usd: Number(r.usd) || 0,
      count: r.count,
    })));
  } catch (err) {
    console.error('Ads daily error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/ads/hourly ---------------------------------------------------------
app.get('/api/ads/hourly', requireDb, async (req, res) => {
  try {
    const sql = `
      SELECT
        EXTRACT(HOUR FROM transaction_time)::int AS "hour",
        COUNT(*)::int                            AS "count",
        COALESCE(SUM(CASE
          WHEN type = 'Authorization' AND status IN ('Authorized','Success')
          THEN transaction_amount ELSE 0 END), 0)::numeric AS "totalPhp"
      FROM card_transactions
      WHERE transaction_time IS NOT NULL
      GROUP BY EXTRACT(HOUR FROM transaction_time)
      ORDER BY "hour" ASC
    `;
    const { rows } = await pgPool.query(sql);
    // Ensure all 24 hours represented
    const byHour = new Map(rows.map(r => [r.hour, { count: r.count, totalPhp: Number(r.totalPhp) || 0 }]));
    const result = [];
    for (let h = 0; h < 24; h++) {
      const v = byHour.get(h);
      result.push({ hour: h, count: v ? v.count : 0, totalPhp: v ? v.totalPhp : 0 });
    }
    res.json(result);
  } catch (err) {
    console.error('Ads hourly error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/ads/failed-pattern -------------------------------------------------
app.get('/api/ads/failed-pattern', requireDb, async (req, res) => {
  try {
    const sql = `
      SELECT
        person                                                                 AS "person",
        RIGHT(REGEXP_REPLACE(COALESCE(card_number,''), '[^0-9]', '', 'g'), 4)  AS "cardLast4",
        card_number                                                            AS "cardNumber",
        COUNT(*)::int                                                          AS "failedCount",
        MAX(transaction_time)                                                  AS "lastFailure",
        (ARRAY_AGG(merchant_name ORDER BY transaction_time DESC NULLS LAST))[1] AS "merchant"
      FROM card_transactions
      WHERE status = 'Fail'
      GROUP BY person, card_number
      ORDER BY "failedCount" DESC, "lastFailure" DESC
    `;
    const { rows } = await pgPool.query(sql);
    res.json(rows.map(r => ({
      person: r.person,
      cardLast4: r.cardLast4 || null,
      cardNumber: r.cardNumber,
      failedCount: r.failedCount,
      lastFailure: r.lastFailure ? new Date(r.lastFailure).toISOString() : null,
      merchant: r.merchant,
    })));
  } catch (err) {
    console.error('Ads failed-pattern error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/ads/reversal-rate --------------------------------------------------
app.get('/api/ads/reversal-rate', requireDb, async (req, res) => {
  try {
    const sql = `
      SELECT
        person                                              AS "person",
        COUNT(*) FILTER (WHERE type = 'Authorization')::int AS "total",
        COUNT(*) FILTER (WHERE type = 'Reversal')::int      AS "reversed"
      FROM card_transactions
      WHERE person IS NOT NULL
      GROUP BY person
      ORDER BY person ASC
    `;
    const { rows } = await pgPool.query(sql);
    res.json(rows.map(r => ({
      person: r.person,
      total: r.total,
      reversed: r.reversed,
      reversalPct: r.total > 0
        ? Number(((r.reversed / r.total) * 100).toFixed(2))
        : 0,
    })));
  } catch (err) {
    console.error('Ads reversal-rate error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/ads/fx-rate --------------------------------------------------------
app.get('/api/ads/fx-rate', requireDb, async (req, res) => {
  try {
    const trendSql = `
      SELECT
        TO_CHAR(DATE(transaction_time), 'YYYY-MM-DD') AS "date",
        AVG(transaction_amount / NULLIF(authorized_amount, 0))::numeric(10,4) AS "rate"
      FROM card_transactions
      WHERE transaction_amount > 0
        AND authorized_amount > 0
        AND transaction_currency = 'PHP'
        AND authorized_currency = 'USD'
        AND transaction_time IS NOT NULL
      GROUP BY DATE(transaction_time)
      ORDER BY DATE(transaction_time) ASC
    `;
    const overallSql = `
      SELECT
        AVG(transaction_amount / NULLIF(authorized_amount, 0))::numeric(10,4) AS "current"
      FROM card_transactions
      WHERE transaction_amount > 0
        AND authorized_amount > 0
        AND transaction_currency = 'PHP'
        AND authorized_currency = 'USD'
    `;
    const [trend, overall] = await Promise.all([
      pgPool.query(trendSql),
      pgPool.query(overallSql),
    ]);
    res.json({
      current: Number((overall.rows[0] || {}).current) || 0,
      trend: trend.rows.map(r => ({
        date: r.date,
        rate: Number(r.rate) || 0,
      })),
    });
  } catch (err) {
    console.error('Ads fx-rate error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/ads/plan-vs-actual -------------------------------------------------
// Cross-reference: planned = USDT Transfers (Master Ledger) matched by recipient
// name, converted to PHP using the sheet's own rate. Actual = card spend grouped
// by person (Authorizations only, non-Failed).
app.get('/api/ads/plan-vs-actual', requireDb, async (req, res) => {
  try {
    // Planned (from Google Sheet - same pattern as /api/adsfund)
    const sheetRows = await getSheetData('USDT Transfers', 'A5:N50');
    const transfers = sheetRows.slice(1)
      .filter(r => r[0] && r[0] !== '' && r[0] !== '0')
      .map(r => ({
        recipient: (r[3] || '').trim(),
        usdt: parseAmount(r[6]),
        rate: parseAmount(r[8]),
        php: parseAmount(r[9]),
      }));

    // Aggregate plan by first-name match (case-insensitive startsWith)
    const planByPerson = {};
    for (const person of ALLOWED_PERSONS) {
      planByPerson[person] = { plannedPhp: 0, plannedUsdt: 0, rateSum: 0, rateCount: 0 };
    }
    transfers.forEach(t => {
      if (!t.recipient) return;
      const first = t.recipient.split(/\s+/)[0];
      // Match by case-insensitive prefix
      const match = [...ALLOWED_PERSONS].find(p => first.toLowerCase() === p.toLowerCase());
      if (match) {
        planByPerson[match].plannedPhp += t.php;
        planByPerson[match].plannedUsdt += t.usdt;
        if (t.rate > 0) {
          planByPerson[match].rateSum += t.rate;
          planByPerson[match].rateCount += 1;
        }
      }
    });

    // Actual (from PG)
    const actualSql = `
      SELECT
        person                                                    AS "person",
        COALESCE(SUM(CASE
          WHEN type = 'Authorization' AND status IN ('Authorized','Success')
          THEN transaction_amount ELSE 0 END), 0)::numeric        AS "actualPhp"
      FROM card_transactions
      WHERE person IS NOT NULL
      GROUP BY person
    `;
    const { rows } = await pgPool.query(actualSql);
    const actualByPerson = {};
    rows.forEach(r => { actualByPerson[r.person] = Number(r.actualPhp) || 0; });

    const result = [...ALLOWED_PERSONS].map(person => {
      const plan = planByPerson[person];
      const actualPhp = actualByPerson[person] || 0;
      const plannedPhp = plan.plannedPhp;
      const rate = plan.rateCount > 0 ? plan.rateSum / plan.rateCount : 0;
      return {
        person,
        plannedUsdt: plan.plannedUsdt,
        rate,
        plannedPhp,
        actualPhp,
        variance: plannedPhp - actualPhp,
        utilization: plannedPhp > 0
          ? Number(((actualPhp / plannedPhp) * 100).toFixed(2))
          : 0,
      };
    });

    res.json(result);
  } catch (err) {
    console.error('Ads plan-vs-actual error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/ads/transactions/:id -------------------------------------------
app.delete('/api/ads/transactions/:id', requireDb, async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    if (!Number.isFinite(id) || id <= 0) return res.status(400).json({ error: 'Invalid id' });
    const { rowCount } = await pgPool.query(
      'DELETE FROM card_transactions WHERE id = $1',
      [id]
    );
    if (rowCount === 0) return res.status(404).json({ error: 'Transaction not found' });
    res.json({ deleted: true, id });
  } catch (err) {
    console.error('Ads delete error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================================
// MARK WALLET LEDGER
// Mark is the funding wallet that loads all 5 cards. CSV format is a WALLET
// ledger, not a card statement. Tracks USDT deposits in and card loads out.
// Responses are camelCase to match frontend conventions.
// ============================================================================

// POST /api/wallet/upload -----------------------------------------------------
app.post('/api/wallet/upload', requireDb, upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded (field name: "file")' });

    let records;
    try {
      records = csvParse(req.file.buffer, {
        columns: true,
        skip_empty_lines: true,
        trim: true,
        bom: true,
      });
    } catch (parseErr) {
      return res.status(400).json({ error: `CSV parse failed: ${parseErr.message}` });
    }

    const total = records.length;
    let inserted = 0;
    let skipped = 0;
    const errors = [];

    const insertSql = `
      INSERT INTO wallet_transactions (
        transaction_id, account_id, account_name, account_type,
        transaction_target, currency, amount, balance_before, balance_after,
        business_order_no, business_type, operation_type, direction, remarks,
        transaction_time
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15
      )
      ON CONFLICT (transaction_id) DO NOTHING
      RETURNING id
    `;

    const client = await pgPool.connect();
    try {
      for (let i = 0; i < records.length; i++) {
        const r = records[i];
        const txId = (r['Transaction ID'] || '').toString().trim();
        if (!txId) {
          skipped++;
          continue;
        }
        try {
          const result = await client.query(insertSql, [
            txId,
            r['Account ID'] || null,
            r['Account Name'] || null,
            r['Account Type'] || null,
            r['Transaction Target'] || null,
            r['Transaction currency'] || null,
            toNum(r['Transaction amount']),
            toNum(r['Balance before transaction']),
            toNum(r['Balance after transaction']),
            r['Business order number'] || null,
            r['Business type'] || null,
            r['Operation type'] || null,
            r['Direction'] || null,
            r['Remarks'] || null,
            parseTxTime(r['Transaction time (UTC+0)']),
          ]);
          if (result.rowCount > 0) inserted++;
          else skipped++;
        } catch (rowErr) {
          skipped++;
          if (errors.length < 5) errors.push({ row: i + 2, error: rowErr.message });
        }
      }
    } finally {
      client.release();
    }

    res.json({ inserted, skipped, total, ...(errors.length ? { errors } : {}) });
  } catch (err) {
    console.error('Wallet upload error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/wallet/summary -----------------------------------------------------
app.get('/api/wallet/summary', requireDb, async (req, res) => {
  try {
    const overallSql = `
      SELECT
        COALESCE(SUM(CASE
          WHEN direction = 'In' AND business_type = 'On-chain Deposit'
          THEN amount ELSE 0 END), 0)::numeric            AS "totalUsdtIn",
        COALESCE(SUM(CASE
          WHEN direction = 'Out' AND business_type = 'Card Deposit'
          THEN amount ELSE 0 END), 0)::numeric            AS "totalCardOut",
        COUNT(*) FILTER (WHERE direction = 'In' AND business_type = 'On-chain Deposit')::int  AS "depositCount",
        COUNT(*) FILTER (WHERE direction = 'Out' AND business_type = 'Card Deposit')::int     AS "cardLoadCount",
        MIN(transaction_time)                             AS "minTime",
        MAX(transaction_time)                             AS "maxTime"
      FROM wallet_transactions
    `;
    // Most recent balance_after
    const latestBalSql = `
      SELECT balance_after AS "balanceAfter"
      FROM wallet_transactions
      WHERE balance_after IS NOT NULL AND transaction_time IS NOT NULL
      ORDER BY transaction_time DESC
      LIMIT 1
    `;

    const [overall, latest] = await Promise.all([
      pgPool.query(overallSql),
      pgPool.query(latestBalSql),
    ]);

    const o = overall.rows[0] || {};
    const totalUsdtIn = Number(o.totalUsdtIn) || 0;
    const totalCardOut = Number(o.totalCardOut) || 0;
    const currentBalance = latest.rows.length ? Number(latest.rows[0].balanceAfter) || 0 : 0;

    res.json({
      totalUsdtIn,
      totalCardOut,
      netBalance: totalUsdtIn - totalCardOut,
      currentBalance,
      depositCount: o.depositCount || 0,
      cardLoadCount: o.cardLoadCount || 0,
      dateRange: { min: o.minTime, max: o.maxTime },
    });
  } catch (err) {
    console.error('Wallet summary error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/wallet/by-card -----------------------------------------------------
app.get('/api/wallet/by-card', requireDb, async (req, res) => {
  try {
    const sql = `
      SELECT
        transaction_target                                                        AS "cardNumber",
        RIGHT(REGEXP_REPLACE(COALESCE(transaction_target,''), '[^0-9]', '', 'g'), 4) AS "cardLast4",
        COALESCE(SUM(amount), 0)::numeric                                         AS "totalLoadedUsd",
        COUNT(*)::int                                                             AS "loadCount",
        MIN(transaction_time)                                                     AS "firstLoad",
        MAX(transaction_time)                                                     AS "lastLoad"
      FROM wallet_transactions
      WHERE business_type = 'Card Deposit'
        AND transaction_target IS NOT NULL
        AND transaction_target <> ''
      GROUP BY transaction_target
      ORDER BY "totalLoadedUsd" DESC
    `;
    const { rows } = await pgPool.query(sql);
    res.json(rows.map(r => ({
      cardNumber: r.cardNumber,
      cardLast4: r.cardLast4 || null,
      totalLoadedUsd: Number(r.totalLoadedUsd) || 0,
      loadCount: r.loadCount,
      firstLoad: r.firstLoad ? new Date(r.firstLoad).toISOString() : null,
      lastLoad: r.lastLoad ? new Date(r.lastLoad).toISOString() : null,
    })));
  } catch (err) {
    console.error('Wallet by-card error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/wallet/timeline ----------------------------------------------------
app.get('/api/wallet/timeline', requireDb, async (req, res) => {
  try {
    const sql = `
      SELECT
        TO_CHAR(DATE(transaction_time), 'YYYY-MM-DD') AS "date",
        COALESCE(SUM(CASE
          WHEN direction = 'In' AND business_type = 'On-chain Deposit'
          THEN amount ELSE 0 END), 0)::numeric         AS "usdtIn",
        COALESCE(SUM(CASE
          WHEN direction = 'Out' AND business_type = 'Card Deposit'
          THEN amount ELSE 0 END), 0)::numeric         AS "cardOut"
      FROM wallet_transactions
      WHERE transaction_time IS NOT NULL
      GROUP BY DATE(transaction_time)
      ORDER BY DATE(transaction_time) ASC
    `;
    const { rows } = await pgPool.query(sql);
    res.json(rows.map(r => {
      const usdtIn = Number(r.usdtIn) || 0;
      const cardOut = Number(r.cardOut) || 0;
      return {
        date: r.date,
        usdtIn,
        cardOut,
        netFlow: usdtIn - cardOut,
      };
    }));
  } catch (err) {
    console.error('Wallet timeline error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/wallet/reconciliation ---------------------------------------------
// Match wallet card-loads (wallet_transactions Card Deposit) with card spend
// (card_transactions Authorized/Success) by last-4 digits of card number.
app.get('/api/wallet/reconciliation', requireDb, async (req, res) => {
  try {
    const sql = `
      WITH wallet_loads AS (
        SELECT
          transaction_target AS card_number,
          RIGHT(REGEXP_REPLACE(COALESCE(transaction_target,''), '[^0-9]', '', 'g'), 4) AS card_last4,
          SUM(amount)::numeric AS loaded_usd
        FROM wallet_transactions
        WHERE business_type = 'Card Deposit'
          AND transaction_target IS NOT NULL
          AND transaction_target <> ''
        GROUP BY transaction_target
      ),
      card_spend AS (
        SELECT
          RIGHT(REGEXP_REPLACE(COALESCE(card_number,''), '[^0-9]', '', 'g'), 4) AS card_last4,
          MAX(person) AS person,
          COALESCE(SUM(CASE
            WHEN type = 'Authorization' AND status IN ('Authorized','Success')
            THEN authorized_amount ELSE 0 END), 0)::numeric AS spent_usd,
          COALESCE(SUM(CASE
            WHEN type = 'Authorization' AND status IN ('Authorized','Success')
            THEN transaction_amount ELSE 0 END), 0)::numeric AS spent_php,
          -- Avg implied FX rate for this card (PHP per USD)
          AVG(CASE
            WHEN transaction_amount > 0 AND authorized_amount > 0
              AND transaction_currency = 'PHP' AND authorized_currency = 'USD'
            THEN transaction_amount / authorized_amount
          END)::numeric AS avg_rate
        FROM card_transactions
        WHERE card_number IS NOT NULL
        GROUP BY RIGHT(REGEXP_REPLACE(COALESCE(card_number,''), '[^0-9]', '', 'g'), 4)
      )
      SELECT
        wl.card_number                              AS "cardNumber",
        wl.card_last4                               AS "cardLast4",
        cs.person                                   AS "person",
        wl.loaded_usd                               AS "loadedUsd",
        cs.avg_rate                                 AS "avgRate",
        cs.spent_usd                                AS "spentUsd",
        cs.spent_php                                AS "spentPhp"
      FROM wallet_loads wl
      LEFT JOIN card_spend cs ON cs.card_last4 = wl.card_last4
      ORDER BY wl.loaded_usd DESC
    `;
    const { rows } = await pgPool.query(sql);
    res.json(rows.map(r => {
      const loadedUsd = Number(r.loadedUsd) || 0;
      const spentUsd = Number(r.spentUsd) || 0;
      const spentPhp = Number(r.spentPhp) || 0;
      const avgRate = Number(r.avgRate) || 0;
      const loadedPhp = loadedUsd * avgRate;
      return {
        cardNumber: r.cardNumber,
        cardLast4: r.cardLast4 || null,
        person: r.person || null,
        loadedUsd,
        loadedPhp,
        spentUsd,
        spentPhp,
        utilizationPct: loadedUsd > 0
          ? Number(((spentUsd / loadedUsd) * 100).toFixed(2))
          : 0,
        remainingUsd: loadedUsd - spentUsd,
      };
    }));
  } catch (err) {
    console.error('Wallet reconciliation error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/wallet/transactions -----------------------------------------------
app.get('/api/wallet/transactions', requireDb, async (req, res) => {
  try {
    const { direction, type } = req.query;
    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    const offset = Math.max(parseInt(req.query.offset) || 0, 0);

    const where = [];
    const params = [];
    if (direction) { params.push(direction); where.push(`direction = $${params.length}`); }
    if (type)      { params.push(type);      where.push(`business_type = $${params.length}`); }

    const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';
    params.push(limit);
    const limitIdx = params.length;
    params.push(offset);
    const offsetIdx = params.length;

    const sql = `
      SELECT
        id                  AS "id",
        transaction_id      AS "transactionId",
        direction           AS "direction",
        business_type       AS "businessType",
        amount              AS "amount",
        currency            AS "currency",
        transaction_target  AS "target",
        balance_after       AS "balanceAfter",
        transaction_time    AS "time",
        remarks             AS "remarks"
      FROM wallet_transactions
      ${whereSql}
      ORDER BY transaction_time DESC NULLS LAST
      LIMIT $${limitIdx} OFFSET $${offsetIdx}
    `;

    const countSql = `SELECT COUNT(*)::int AS total FROM wallet_transactions ${whereSql}`;
    const countParams = params.slice(0, params.length - 2);

    const [rowsRes, countRes] = await Promise.all([
      pgPool.query(sql, params),
      pgPool.query(countSql, countParams),
    ]);

    const rows = rowsRes.rows.map(r => ({
      id: r.id,
      transactionId: r.transactionId,
      direction: r.direction,
      businessType: r.businessType,
      amount: r.amount != null ? Number(r.amount) : null,
      currency: r.currency,
      target: r.target,
      balanceAfter: r.balanceAfter != null ? Number(r.balanceAfter) : null,
      time: r.time ? new Date(r.time).toISOString() : null,
      remarks: r.remarks,
    }));

    res.json({
      total: countRes.rows[0].total,
      limit,
      offset,
      rows,
    });
  } catch (err) {
    console.error('Wallet transactions error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/wallet/transactions/:id ----------------------------------------
app.delete('/api/wallet/transactions/:id', requireDb, async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    if (!Number.isFinite(id) || id <= 0) return res.status(400).json({ error: 'Invalid id' });
    const { rowCount } = await pgPool.query(
      'DELETE FROM wallet_transactions WHERE id = $1',
      [id]
    );
    if (rowCount === 0) return res.status(404).json({ error: 'Transaction not found' });
    res.json({ deleted: true, id });
  } catch (err) {
    console.error('Wallet delete error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ===========================================================================
// CARD LEDGER — unified per-card timeline (wallet loads IN + card spends OUT)
// ===========================================================================

// GET /api/cards — list all cards (union of card_transactions + wallet_transactions)
app.get('/api/cards', requireDb, async (req, res) => {
  try {
    const { rows } = await pgPool.query(`
      WITH card_side AS (
        SELECT
          card_number,
          RIGHT(REGEXP_REPLACE(card_number, '[^0-9]', '', 'g'), 4) AS card_last4,
          MAX(person) AS person,
          COUNT(*) FILTER (WHERE type = 'Authorization' AND status IN ('Authorized','Success')) AS spend_count,
          COALESCE(SUM(authorized_amount) FILTER (WHERE type = 'Authorization' AND status IN ('Authorized','Success')), 0) AS spent_usd,
          COALESCE(SUM(transaction_amount) FILTER (WHERE type = 'Authorization' AND status IN ('Authorized','Success')), 0) AS spent_php,
          MIN(transaction_time) AS first_tx,
          MAX(transaction_time) AS last_tx
        FROM card_transactions
        WHERE card_number IS NOT NULL
        GROUP BY card_number
      ),
      wallet_side AS (
        SELECT
          transaction_target AS card_number,
          RIGHT(REGEXP_REPLACE(transaction_target, '[^0-9]', '', 'g'), 4) AS card_last4,
          COUNT(*) AS load_count,
          COALESCE(SUM(amount), 0) AS loaded_usd,
          MIN(transaction_time) AS first_load,
          MAX(transaction_time) AS last_load
        FROM wallet_transactions
        WHERE transaction_target IS NOT NULL
          AND transaction_target <> ''
          AND business_type = 'Card Deposit'
          AND direction = 'Out'
        GROUP BY transaction_target
      )
      SELECT
        COALESCE(c.card_number, w.card_number) AS "cardNumber",
        COALESCE(c.card_last4, w.card_last4) AS "cardLast4",
        c.person AS "person",
        COALESCE(w.loaded_usd, 0) AS "loadedUsd",
        COALESCE(w.load_count, 0) AS "loadCount",
        COALESCE(c.spent_usd, 0) AS "spentUsd",
        COALESCE(c.spent_php, 0) AS "spentPhp",
        COALESCE(c.spend_count, 0) AS "spendCount",
        COALESCE(w.loaded_usd, 0) - COALESCE(c.spent_usd, 0) AS "balanceUsd",
        LEAST(c.first_tx, w.first_load) AS "firstActivity",
        GREATEST(c.last_tx, w.last_load) AS "lastActivity"
      FROM card_side c
      FULL OUTER JOIN wallet_side w ON c.card_number = w.card_number
      ORDER BY "spentUsd" DESC, "loadedUsd" DESC
    `);
    res.json(rows);
  } catch (err) {
    console.error('Cards list error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/card-ledger/:cardNumber — unified chronological ledger for one card
// cardNumber is URL-encoded masked format like "537100******3077"
// Alternatively accepts last 4 digits
app.get('/api/card-ledger/:cardNumber', requireDb, async (req, res) => {
  try {
    const raw = decodeURIComponent(req.params.cardNumber || '');
    if (!raw) return res.status(400).json({ error: 'cardNumber required' });

    // If only 4 digits provided, match by last 4
    const isLast4 = /^\d{4}$/.test(raw);
    const matchClause = isLast4
      ? `RIGHT(REGEXP_REPLACE(card_number, '[^0-9]', '', 'g'), 4) = $1`
      : `card_number = $1`;
    const walletMatchClause = isLast4
      ? `RIGHT(REGEXP_REPLACE(transaction_target, '[^0-9]', '', 'g'), 4) = $1`
      : `transaction_target = $1`;

    // Fetch card header info
    const headerResult = await pgPool.query(
      `SELECT
         card_number AS "cardNumber",
         RIGHT(REGEXP_REPLACE(card_number, '[^0-9]', '', 'g'), 4) AS "cardLast4",
         MAX(person) AS "person"
       FROM card_transactions
       WHERE ${matchClause}
       GROUP BY card_number
       LIMIT 1`,
      [raw]
    );

    // Fetch wallet loads for this card (IN events)
    const loadsResult = await pgPool.query(
      `SELECT
         id,
         transaction_id AS "refId",
         transaction_time AS "time",
         amount AS "amountUsd",
         currency,
         business_type AS "businessType",
         remarks,
         balance_after AS "walletBalanceAfter"
       FROM wallet_transactions
       WHERE ${walletMatchClause}
         AND business_type = 'Card Deposit'
         AND direction = 'Out'
       ORDER BY transaction_time ASC`,
      [raw]
    );

    // Fetch card spends (OUT events)
    const spendsResult = await pgPool.query(
      `SELECT
         id,
         transaction_serial AS "refId",
         transaction_time AS "time",
         transaction_amount AS "amountPhp",
         authorized_amount AS "amountUsd",
         authorization_fee AS "authFee",
         cross_border_fee AS "xBorderFee",
         merchant_name AS "merchant",
         type,
         status,
         description
       FROM card_transactions
       WHERE ${matchClause}
       ORDER BY transaction_time ASC`,
      [raw]
    );

    // Merge into unified ledger (IN then OUT for same-timestamp ties)
    const entries = [];
    loadsResult.rows.forEach(r => {
      entries.push({
        id: 'w' + r.id,
        time: r.time,
        direction: 'IN',
        type: r.businessType || 'Card Deposit',
        source: 'wallet',
        amountUsd: Number(r.amountUsd) || 0,
        amountPhp: null,
        description: r.remarks || 'Wallet load',
        status: 'Success',
        refId: r.refId
      });
    });
    spendsResult.rows.forEach(r => {
      const isReversal = (r.type || '').toLowerCase() === 'reversal';
      const isFailed = (r.status || '').toLowerCase() === 'fail';
      // Only count successful Authorizations as OUT; Reversals as IN; Failed as 0
      let direction = 'OUT';
      let effectiveUsd = Number(r.amountUsd) || 0;
      if (isReversal) {
        direction = 'IN';
      } else if (isFailed) {
        direction = 'OUT';
        effectiveUsd = 0; // failed auths don't move money
      }
      const fees = (Number(r.authFee) || 0) + (Number(r.xBorderFee) || 0);
      entries.push({
        id: 'c' + r.id,
        time: r.time,
        direction,
        type: r.type || 'Authorization',
        source: 'card',
        amountUsd: effectiveUsd,
        amountPhp: Number(r.amountPhp) || 0,
        fees,
        merchant: r.merchant,
        status: r.status,
        description: r.description || '',
        refId: r.refId,
        isFailed
      });
    });

    // Sort by time then IN before OUT
    entries.sort((a, b) => {
      const tA = new Date(a.time).getTime();
      const tB = new Date(b.time).getTime();
      if (tA !== tB) return tA - tB;
      if (a.direction === b.direction) return 0;
      return a.direction === 'IN' ? -1 : 1;
    });

    // Compute running balance (USD only — PHP varies by FX rate)
    let balance = 0;
    let totalIn = 0;
    let totalOut = 0;
    let totalFees = 0;
    entries.forEach(e => {
      if (e.direction === 'IN') {
        balance += e.amountUsd;
        totalIn += e.amountUsd;
      } else if (!e.isFailed) {
        balance -= e.amountUsd;
        totalOut += e.amountUsd;
      }
      totalFees += e.fees || 0;
      e.runningBalance = balance;
    });

    const header = headerResult.rows[0] || {};
    // If header missing (wallet-only card), synthesize it
    if (!header.cardNumber && loadsResult.rows.length > 0) {
      const anyLoad = await pgPool.query(
        `SELECT transaction_target AS "cardNumber",
                RIGHT(REGEXP_REPLACE(transaction_target, '[^0-9]', '', 'g'), 4) AS "cardLast4"
         FROM wallet_transactions
         WHERE ${walletMatchClause} AND transaction_target IS NOT NULL
         LIMIT 1`,
        [raw]
      );
      if (anyLoad.rows[0]) Object.assign(header, anyLoad.rows[0], { person: null });
    }

    res.json({
      cardNumber: header.cardNumber || null,
      cardLast4: header.cardLast4 || null,
      person: header.person || null,
      summary: {
        totalIn,
        totalOut,
        totalFees,
        balance,
        loadCount: loadsResult.rows.length,
        spendCount: spendsResult.rows.length,
        failedCount: spendsResult.rows.filter(r => (r.status || '').toLowerCase() === 'fail').length,
        reversalCount: spendsResult.rows.filter(r => (r.type || '').toLowerCase() === 'reversal').length,
        entryCount: entries.length
      },
      entries
    });
  } catch (err) {
    console.error('Card ledger error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`QUAD OPEX Dashboard running on port ${PORT}`);
});
