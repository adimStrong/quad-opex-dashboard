// One-shot diagnostic: break down person='undefined' rows by card last-4
// Usage: railway run node scripts/diag_undefined.js
const { Pool } = require('pg');

(async () => {
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DATABASE_URL && process.env.DATABASE_URL.includes('railway.internal')
      ? false
      : { rejectUnauthorized: false },
  });
  try {
    const totalRes = await pool.query(
      `SELECT COUNT(*)::int AS n FROM card_transactions WHERE person = 'undefined'`
    );
    console.log('Total undefined rows:', totalRes.rows[0].n);

    const byCardRes = await pool.query(`
      SELECT
        RIGHT(REGEXP_REPLACE(COALESCE(card_number,''), '[^0-9]', '', 'g'), 4) AS last4,
        card_number,
        COUNT(*)::int AS rows,
        COALESCE(SUM(CASE WHEN type='Authorization' AND status IN ('Authorized','Success')
                    THEN transaction_amount ELSE 0 END),0)::numeric AS approved_php,
        MIN(transaction_time) AS first_seen,
        MAX(transaction_time) AS last_seen
      FROM card_transactions
      WHERE person = 'undefined'
      GROUP BY last4, card_number
      ORDER BY rows DESC
    `);
    console.log('\nBy card (undefined only):');
    console.table(byCardRes.rows);

    const byPersonRes = await pool.query(`
      SELECT person, COUNT(*)::int AS rows
      FROM card_transactions
      GROUP BY person
      ORDER BY rows DESC
    `);
    console.log('\nAll persons in card_transactions:');
    console.table(byPersonRes.rows);
  } catch (e) {
    console.error('ERROR:', e.message);
  } finally {
    await pool.end();
  }
})();
