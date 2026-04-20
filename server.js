const express = require('express');
const { google } = require('googleapis');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

const SPREADSHEET_ID = '12nkWKQfAfR70MMapo-pa0jT2PQD4SEY8M6Icse9PmSQ';

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

app.listen(PORT, () => {
  console.log(`QUAD OPEX Dashboard running on port ${PORT}`);
});
