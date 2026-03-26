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

async function handleSheets(url) {
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
      },
    });
  } catch (err) {
    return new Response(JSON.stringify({ error: 'Proxy fetch failed' }), {
      status: 502,
      headers: { 'Content-Type': 'application/json' },
    });
  }
}

const PIXELLOT_API = 'https://supersportschools.watch.pixellot.tv/api/event/list';
const PIXELLOT_PROJECT = '606dace04cf99f438737e283';
const TOURNAMENT_KEYWORD = 'KINGSMEAD';

async function handleStreams() {
  try {
    const events = [];
    for (const status of ['live', 'archived', 'upcoming']) {
      const resp = await fetch(PIXELLOT_API, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-project-id': PIXELLOT_PROJECT,
        },
        body: JSON.stringify({
          filters: { status },
          limit: 100,
          offset: 0,
        }),
      });
      if (!resp.ok) continue;
      const data = await resp.json();
      const entries = data?.content?.entries || [];
      for (const e of entries) {
        const title = (e.title || '').toUpperCase();
        if (!title.includes(TOURNAMENT_KEYWORD) || !title.includes('HOCKEY')) continue;
        const home = e.eventTeams?.homeTeam?.name || '';
        const away = e.eventTeams?.awayTeam?.name || '';
        events.push({
          id: e._id,
          home,
          away,
          status: e.status || status,
          date: e.event_date || 0,
          url: `https://live.supersportschools.com/events/${e._id}/`,
        });
      }
    }
    return new Response(JSON.stringify({ events }), {
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'public, max-age=300',
      },
    });
  } catch (err) {
    return new Response(JSON.stringify({ error: 'Stream fetch failed', events: [] }), {
      status: 502,
      headers: { 'Content-Type': 'application/json' },
    });
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/api/sheets') {
      return handleSheets(url);
    }
    if (url.pathname === '/api/streams') {
      return handleStreams();
    }
    return env.ASSETS.fetch(request);
  },
};
