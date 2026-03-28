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
const PIXELLOT_EVENT_API = 'https://supersportschools.watch.pixellot.tv/api/event/get_by_id/id/';
const PIXELLOT_PROJECT = '606dace04cf99f438737e283';
const TOURNAMENT_SUBCATEGORY = '69c2ed3f2a337d76720fc58d'; // Kingsmead Courage Festival 2026

// Day 1 streams that drop off the list API but are still accessible by ID
const PINNED_EVENT_IDS = [
  '69c48952a89f43606ac88979', // Monument vs DF Akademie
  '69c48c8aa89f43606ac88bf7', // Epworth vs Monument
  '69c488bca89f43606ac888e9', // DSG vs St Dominic's
  '69c48ae3f7d08325083438a3', // Volkskool vs Ermelo
  '69c48bc6f7d083250834396d', // St Mary's vs Kingsmead
  '69c48a35a89f43606ac88a47', // Hugenote vs Maris Stella
  '69c48dbca89f43606ac88ce1', // Parktown vs Middleburg
];

async function handleStreams() {
  try {
    const events = [];
    for (const status of ['live', 'archived', 'upcoming']) {
      // Paginate through all results (API caps at 20 per page)
      let offset = 0;
      const maxPages = 5; // safety limit
      for (let page = 0; page < maxPages; page++) {
        let resp;
        try {
          resp = await fetch(PIXELLOT_API, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'x-project-id': PIXELLOT_PROJECT,
            },
            body: JSON.stringify({
              filters: { 'identities.id': TOURNAMENT_SUBCATEGORY, status },
              limit: 20,
              offset,
            }),
          });
        } catch (fetchErr) {
          break; // network error, stop paginating this status
        }
        if (!resp.ok) break;
        const data = await resp.json();
        const total = data?.content?.entryCount || 0;
        const entries = data?.content?.entries || [];
        for (const e of entries) {
          const title = (e.title || '').toUpperCase();
          if (!title.includes('HOCKEY')) continue;
          const homeTeam = e.eventTeams?.homeTeam || {};
          const awayTeam = e.eventTeams?.awayTeam || {};
          events.push({
            id: e._id,
            home: homeTeam.name || '',
            away: awayTeam.name || '',
            homeId: homeTeam.teamId || homeTeam.id || '',
            awayId: awayTeam.teamId || awayTeam.id || '',
            homeLogo: homeTeam.logo || '',
            awayLogo: awayTeam.logo || '',
            status: e.status || status,
            date: e.event_date || 0,
            url: `https://live.supersportschools.com/events/${e._id}/`,
          });
        }
        offset += entries.length;
        if (offset >= total || entries.length === 0) break; // got all pages
      }
    }

    // Fetch pinned events that may have dropped off the list API
    const seenIds = new Set(events.map(e => e.id));
    for (const eid of PINNED_EVENT_IDS) {
      if (seenIds.has(eid)) continue;
      try {
        const resp = await fetch(PIXELLOT_EVENT_API + eid, {
          headers: { 'x-project-id': PIXELLOT_PROJECT },
        });
        if (!resp.ok) continue;
        const data = await resp.json();
        const e = data?.content;
        if (!e) continue;
        const title = (e.title || '').toUpperCase();
        if (!title.includes('HOCKEY')) continue;
        events.push({
          id: e._id || e.event_id || eid,
          home: e.eventTeams?.homeTeam?.name || '',
          away: e.eventTeams?.awayTeam?.name || '',
          status: e.status || 'archived',
          date: e.event_date || 0,
          url: `https://live.supersportschools.com/events/${eid}/`,
        });
      } catch (err) { /* skip failed individual fetches */ }
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
