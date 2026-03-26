const SHEET_ID = '1wCcDyOVyCXO4WmIpi602ySKfbg3BI7Xd';

const ALLOWED_SHEETS = new Set(['Fixtures', 'Scores']);
const RANGE_PATTERN = /^[A-Z]{1,3}\d{0,5}(:[A-Z]{1,3}\d{0,5})?$/;

function parseCSV(text) {
  if (!text || !text.trim()) return [];
  const rows = [];
  let row = [];
  let field = '';
  let inQuotes = false;
  let i = 0;

  while (i < text.length) {
    const ch = text[i];
    if (inQuotes) {
      if (ch === '"') {
        if (i + 1 < text.length && text[i + 1] === '"') {
          field += '"';
          i += 2;
        } else {
          inQuotes = false;
          i++;
        }
      } else {
        field += ch;
        i++;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
        i++;
      } else if (ch === ',') {
        row.push(field);
        field = '';
        i++;
      } else if (ch === '\r') {
        row.push(field);
        field = '';
        rows.push(row);
        row = [];
        i += (i + 1 < text.length && text[i + 1] === '\n') ? 2 : 1;
      } else if (ch === '\n') {
        row.push(field);
        field = '';
        rows.push(row);
        row = [];
        i++;
      } else {
        field += ch;
        i++;
      }
    }
  }
  if (field || row.length > 0) {
    row.push(field);
    rows.push(row);
  }
  return rows;
}

export async function onRequest(context) {
  const url = new URL(context.request.url);
  const sheet = url.searchParams.get('sheet');
  const range = url.searchParams.get('range');

  if (!sheet || !ALLOWED_SHEETS.has(sheet)) {
    return new Response(JSON.stringify({ error: 'Invalid sheet' }), {
      status: 400,
      headers: { 'Content-Type': 'application/json' },
    });
  }
  if (!range || !RANGE_PATTERN.test(range)) {
    return new Response(JSON.stringify({ error: 'Invalid range' }), {
      status: 400,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  const gvizUrl = `https://docs.google.com/spreadsheets/d/${SHEET_ID}/gviz/tq?tqx=out:csv&sheet=${encodeURIComponent(sheet)}&range=${encodeURIComponent(range)}`;

  try {
    const resp = await fetch(gvizUrl);
    if (!resp.ok) {
      return new Response(JSON.stringify({ error: `Google returned ${resp.status}` }), {
        status: 502,
        headers: { 'Content-Type': 'application/json' },
      });
    }
    const csv = await resp.text();
    const values = parseCSV(csv);

    return new Response(JSON.stringify({ values }), {
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'public, max-age=30',
        'Access-Control-Allow-Origin': '*',
      },
    });
  } catch (err) {
    return new Response(JSON.stringify({ error: 'Proxy fetch failed' }), {
      status: 502,
      headers: { 'Content-Type': 'application/json' },
    });
  }
}
