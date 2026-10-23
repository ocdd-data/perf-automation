/**
 * Regional Operational Data updater (Apps Script, bound to "Regional Operational Data").
 *
 * On the 2nd of each month "kpi tracking/regional_od.py" posts Regional_Operational_Data_<Mon>_<YYYY>.csv to
 * Slack next to its Excel: one row per value, region / vehicle / metric / value (vehicle BIKE, CAR or TUKTUK,
 * blank for SG, HK and NY), tagged to the right (F1:G2):
 *   report       | Regional Ops
 *   month_start  | 2026-09-01
 *
 * To fill a month: File > Import > Upload > "Insert new sheet(s)". The script reads the tags first and only
 * goes ahead for a Regional Ops file; anything else is refused and nothing changes. Every tab has the months
 * across row 1 (a date per column, from column B):
 *   - the month's column is found by its date; if it isn't there and is the month after the last one, a
 *     column is added after the last on all ten tabs (date, width and formatting from the month before)
 *   - SG, HK, NY, TH, KH, HCMC, HAN: the file's values are written by metric name, under the vehicle's
 *     heading (BIKE / CAR / TUKTUK in column A) on tabs that have them. A row whose previous month is a
 *     formula (HCMC's USD GMV, from the VND rows lower down) keeps the formula, so the value goes to the
 *     metric's other row. Rows the file doesn't cover (spend, costs, P&L) are left for the people who fill
 *     them; in a new column only the formulas the previous month already has are copied (e.g. TOTAL)
 *   - VN SUM, TH(USD), HK(USD): every formula row is dragged into the month, from the nearest month to the
 *     left that has it; cells that already hold something are left alone
 * Every row is found and checked before anything is written. Re-importing a month overwrites its values. On
 * success the imported tab is deleted; on an error it stays, with the reason in red at F4, and the sheet is
 * untouched.
 *
 * Setup: Extensions > Apps Script, paste this file, save, reload the sheet, Regional Ops > Enable auto-update.
 */

const REPORT = 'Regional Ops';
const VALUE_TABS = ['SG', 'HK', 'NY', 'TH', 'KH', 'HCMC', 'HAN'];  // also the file's region names
const FORMULA_TABS = ['VN SUM', 'TH(USD)', 'HK(USD)'];
const VEHICLES = ['BIKE', 'CAR', 'TUKTUK'];
const DATE_ROW = 1;   // month dates, from column B
const FIRST_COL = 2;
const LABEL_COL = 1;

const REPORT_KEY = 'report';
const MONTH_KEY = 'month_start';
const TAG_SCAN_COLS = 30;
const FORMULA_LOOKBACK = 12;  // months to the left searched for a formula tab's formula

// ---------- menu and triggers ----------

function onOpen() {
  SpreadsheetApp.getUi().createMenu(REPORT)
    .addItem('Update now', 'updateNow')
    .addItem('Remove the last month…', 'removeLastMonth')
    .addItem('Enable auto-update', 'enableAutoUpdate')
    .addToUi();
}

function updateNow() { run_(true); }

// Installable onChange: runs on File > Import and on edits, and returns at once unless a tagged file has
// been imported.
function onChangeHandler(e) { run_(false); }

function enableAutoUpdate() {
  const ss = SpreadsheetApp.getActive();
  ScriptApp.getProjectTriggers()
    .filter(t => t.getHandlerFunction() === 'onChangeHandler')
    .forEach(t => ScriptApp.deleteTrigger(t));
  ScriptApp.newTrigger('onChangeHandler').forSpreadsheet(ss).onChange().create();
  ss.toast(`Auto-update is on: import a ${REPORT} file as new sheets and the month fills itself.`, REPORT, 8);
}

// ---------- run ----------

function run_(manual) {
  const ss = SpreadsheetApp.getActive();
  const lock = LockService.getDocumentLock();
  if (!lock.tryLock(manual ? 30000 : 1000)) return;
  let imp = null;
  try {
    const found = findImports_(ss);
    if (!found.length) {
      if (manual) throw new Error(`No tagged file found. Import the ${REPORT} CSV with File > Import > Upload > "Insert new sheet(s)" (the Excel file is not the one).`);
      return;
    }
    imp = found[0];
    // A failed import stays with its error noted; rerun it from the menu, not on every later edit.
    if (!manual && String(errorCell_(imp).getValue()).indexOf('Not filled') === 0) return;
    if (found.length > 1)
      throw new Error(`${found.length} imported files are in the sheet (${found.map(f => f.sheet.getName()).join(', ')}). Delete the ones you don't need, then use ${REPORT} > Update now.`);
    if (imp.report !== REPORT)
      throw new Error(`This is a ${imp.report} file, but this spreadsheet takes ${REPORT}. Nothing was changed; delete the imported tab.`);

    const tz = ss.getSpreadsheetTimeZone() || Session.getScriptTimeZone() || 'Asia/Singapore';
    const month = monthKey_(imp.monthValue, tz);
    const src = readSource_(imp);

    // Everything is checked before anything is written.
    const plans = VALUE_TABS.map(t => planValueTab_(ss, t, src[t], month, tz))
      .concat(FORMULA_TABS.map(t => planTab_(ss, t, month, tz)));
    plans.forEach(p => { if (p.isNew) addMonth_(p); });
    const notes = [];
    plans.forEach(p => (p.writes ? fillValueTab_(p, notes) : fillFormulaTab_(p)));
    SpreadsheetApp.flush();

    ss.deleteSheet(imp.sheet);
    const added = plans.some(p => p.isNew);
    const msg = `Filled ${month}${added ? ' (column added)' : ''}.` + (notes.length ? ' Check: ' + notes.join('; ') : '');
    if (manual) SpreadsheetApp.getUi().alert(msg);
    else ss.toast(msg, REPORT, 15);
  } catch (err) {
    const msg = 'Not filled: ' + err.message;
    if (imp) errorCell_(imp).setValue(msg).setFontColor('#c5221f').setFontWeight('bold');
    if (manual) SpreadsheetApp.getUi().alert(msg);
    else ss.toast(msg, REPORT, 30);
  } finally {
    lock.releaseLock();
  }
}

// ---------- imported file ----------

// Tabs whose row 1 holds a "report" tag: each is one imported file.
function findImports_(ss) {
  const found = [];
  ss.getSheets().forEach(sh => {
    const width = Math.min(sh.getLastColumn(), TAG_SCAN_COLS);
    if (width < 2) return;
    const rows = sh.getRange(1, 1, 2, width).getValues();
    const col = rows[0].findIndex(v => String(v).trim() === REPORT_KEY);
    if (col < 0 || col + 1 >= width) return;
    found.push({ sheet: sh, tagCol: col + 1, report: String(rows[0][col + 1]).trim(), monthValue: rows[1][col + 1] });
  });
  return found;
}

function errorCell_(imp) { return imp.sheet.getRange(4, imp.tagCol); }

function monthKey_(v, tz) {
  const s = v instanceof Date ? Utilities.formatDate(new Date(v.getTime() + 12 * 3600 * 1000), tz, 'yyyy-MM-dd') : String(v).trim();
  if (!/^\d{4}-\d{2}(-\d{2})?$/.test(s))
    throw new Error(`The file's ${MONTH_KEY} tag should be a date like 2026-09-01, got "${v}".`);
  return s.slice(0, 7);
}

// A CSV import keeps numbers longer than 15 digits (e.g. 0.6551269023284992) as text; written as text they
// lose the cell's number format, so turn anything that reads as a number back into one.
function toNumber_(v) {
  return typeof v === 'string' && /^\s*-?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?\s*$/.test(v) ? Number(v) : v;
}

// region -> [{vehicle, metric, value}], from columns A:D of the imported tab (the tags sit further right).
function readSource_(imp) {
  const sh = imp.sheet;
  const head = sh.getRange(1, 1, 1, 4).getValues()[0].map(norm_);
  if (head.join('|') !== 'region|vehicle|metric|value')
    throw new Error(`The imported file's columns should be region, vehicle, metric, value, not ${head.join(', ')}.`);
  const src = {};
  sh.getRange(2, 1, Math.max(sh.getLastRow() - 1, 1), 4).getValues().forEach(([region, vehicle, metric, v]) => {
    region = String(region).trim();
    if (!region) return;
    (src[region] = src[region] || []).push({ vehicle: String(vehicle).trim().toUpperCase(), metric: String(metric).trim(), value: toNumber_(v) });
  });
  return src;
}

// ---------- month columns ----------

function norm_(s) { return String(s).replace(/\s+/g, ' ').trim().toLowerCase(); }

function nextMonth_(key) {
  let [y, m] = key.split('-').map(Number);
  if (++m > 12) { m = 1; y++; }
  return `${y}-${String(m).padStart(2, '0')}`;
}

// 'YYYY-MM' -> column, from the dates in row 1.
function monthCols_(sh, tz) {
  const width = sh.getLastColumn();
  const dates = width >= FIRST_COL ? sh.getRange(DATE_ROW, FIRST_COL, 1, width - FIRST_COL + 1).getValues()[0] : [];
  const m = {};
  let lastKey = null, lastCol = null;
  dates.forEach((v, i) => {
    if (!(v instanceof Date)) return;
    const k = Utilities.formatDate(new Date(v.getTime() + 12 * 3600 * 1000), tz, 'yyyy-MM');
    m[k] = FIRST_COL + i;
    if (!lastKey || k > lastKey) { lastKey = k; lastCol = FIRST_COL + i; }
  });
  return { m, lastKey, lastCol };
}

function tab_(ss, name) {
  const sh = ss.getSheetByName(name);
  if (!sh) throw new Error(`No tab named "${name}". Has it been renamed?`);
  return sh;
}

// The month's column (or where it will be added) and the month before it, whose formatting and formulas a
// new column takes.
function planTab_(ss, name, month, tz) {
  const sh = tab_(ss, name);
  const mc = monthCols_(sh, tz);
  if (!mc.lastKey) throw new Error(`${name}: no month dates found in row ${DATE_ROW}.`);
  let col = mc.m[month] || null;
  if (!col && month !== nextMonth_(mc.lastKey))
    throw new Error(`${name} has no ${month} column, and it isn't the month after the last one (${mc.lastKey}). Fill the months in order.`);
  const isNew = !col;
  if (isNew) col = mc.lastCol + 1;
  if (col <= FIRST_COL) throw new Error(`${name}: ${month} is the first month; there is no month before it to copy from.`);
  return { sh, name, month, tz, col, isNew, prevCol: col - 1 };
}

// Where each of the region's values goes. Vehicle tabs: the rows under each BIKE / CAR / TUKTUK heading in
// column A, down to the next heading; a vehicle can have several headings (HCMC: one block in USD, one in
// VND). Each metric goes to its first row in every block of its vehicle, except rows that are formulas, and
// must land somewhere.
function planValueTab_(ss, name, values, month, tz) {
  if (!values || !values.length) throw new Error(`The file has no ${name} values (its query may have failed). Fill ${name} by hand and import again without it, or rerun the job.`);
  const p = planTab_(ss, name, month, tz);
  const sh = p.sh;
  const lastRow = sh.getLastRow();
  const labels = sh.getRange(1, LABEL_COL, lastRow, 1).getValues().map(r => norm_(r[0]));
  const prevF = sh.getRange(1, p.prevCol, lastRow, 1).getFormulas().map(r => r[0]);
  const ownF = p.isNew ? labels.map(() => '') : sh.getRange(1, p.col, lastRow, 1).getFormulas().map(r => r[0]);

  const blocks = [];  // {vehicle, from, to} rows, 1-based
  labels.forEach((l, i) => {
    const v = VEHICLES.find(x => x.toLowerCase() === l);
    if (v) blocks.push({ vehicle: v, from: i + 2, to: lastRow });
  });
  blocks.forEach((b, i) => { if (blocks[i + 1]) b.to = blocks[i + 1].from - 2; });
  if (!blocks.length) blocks.push({ vehicle: '', from: DATE_ROW + 1, to: lastRow });

  const writes = [], skipped = [];
  values.forEach(({ vehicle, metric, value }) => {
    const mine = blocks.filter(b => b.vehicle === vehicle);
    if (!mine.length) throw new Error(`${name} has no ${vehicle || 'single'} block for the file's ${vehicle} values. Has a heading in column A been renamed?`);
    let landed = 0, formulaOnly = 0;
    mine.forEach(b => {
      for (let r = b.from; r <= b.to; r++) {
        if (labels[r - 1] !== norm_(metric)) continue;
        if (prevF[r - 1] || ownF[r - 1]) { formulaOnly++; break; }
        writes.push({ row: r, value, metric, vehicle });
        landed++;
        break;
      }
    });
    if (!landed)
      throw new Error(formulaOnly
        ? `${name}: every "${metric}"${vehicle ? ' row under ' + vehicle : ' row'} is a formula, so its value has nowhere to go.`
        : `${name} has no "${metric}" row${vehicle ? ' under ' + vehicle : ''}. Has a row been renamed?`);
    if (value === '' || value === null) skipped.push(`${name}${vehicle ? ' ' + vehicle : ''} ${metric}`);
  });
  p.writes = writes;
  p.skipped = skipped;
  return p;
}

// Adds the month's column after the last: date, width and formatting from the month before.
function addMonth_(p) {
  const { sh, prevCol, col, month, tz } = p;
  sh.insertColumnAfter(prevCol);
  sh.setColumnWidth(col, sh.getColumnWidth(prevCol));
  const height = Math.max(sh.getLastRow(), 1);
  sh.getRange(1, prevCol, height, 1)
    .copyTo(sh.getRange(1, col, height, 1), SpreadsheetApp.CopyPasteType.PASTE_FORMAT, false);
  // Midnight on the 1st in the sheet's own time zone, like the dates typed in before.
  sh.getRange(DATE_ROW, col).setValue(Utilities.parseDate(`${month}-01`, tz, 'yyyy-MM-dd'));
}

// Value tabs: the file's values; in a new column also the formulas the month before has (TOTAL, the USD rows).
function fillValueTab_(p, notes) {
  const { sh, col, prevCol, isNew, writes, skipped } = p;
  if (isNew) {
    const height = sh.getLastRow() - DATE_ROW;
    const f = sh.getRange(DATE_ROW + 1, prevCol, height, 1).getFormulasR1C1();
    f.forEach((r, i) => { if (r[0]) sh.getRange(DATE_ROW + 1 + i, col).setFormulaR1C1(r[0]); });
  }
  writes.forEach(w => { if (w.value !== '' && w.value !== null) sh.getRange(w.row, col).setValue(w.value); });
  if (skipped.length) notes.push(`empty in the file, left as they were: ${skipped.join(', ')}`);
}

// Formula tabs: every formula row dragged in from the nearest month to the left that has it; cells that
// already hold something are left alone.
function fillFormulaTab_(p) {
  const { sh, col } = p;
  const height = sh.getLastRow() - DATE_ROW;
  if (height < 1) return;
  const own = sh.getRange(DATE_ROW + 1, col, height, 1).getValues().map(r => r[0]);
  const ownF = sh.getRange(DATE_ROW + 1, col, height, 1).getFormulas().map(r => r[0]);
  const history = [];
  for (let c = col - 1; c >= FIRST_COL && history.length < FORMULA_LOOKBACK; c--)
    history.push(sh.getRange(DATE_ROW + 1, c, height, 1).getFormulasR1C1().map(r => r[0]));
  for (let i = 0; i < height; i++) {
    if (ownF[i] || own[i] !== '') continue;
    const f = history.map(h => h[i]).find(x => x);
    if (f) sh.getRange(DATE_ROW + 1 + i, col).setFormulaR1C1(f);
  }
}

// ---------- undo ----------

// Deletes the last month's column from every tab that has it as its last month (all ten after an import).
function removeLastMonth() {
  const ui = SpreadsheetApp.getUi();
  const ss = SpreadsheetApp.getActive();
  const tz = ss.getSpreadsheetTimeZone() || Session.getScriptTimeZone() || 'Asia/Singapore';
  const tabs = VALUE_TABS.concat(FORMULA_TABS).map(n => tab_(ss, n));
  const cols = tabs.map(sh => monthCols_(sh, tz));
  const last = cols.map(c => c.lastKey).filter(Boolean).sort().pop();
  if (!last) { ui.alert('No months found.'); return; }
  const ok = ui.alert(`Remove ${last}?`,
    `Deletes the ${last} column from every tab whose last month it is, including anything typed into it (spend, costs, P&L).`,
    ui.ButtonSet.YES_NO);
  if (ok !== ui.Button.YES) return;
  const done = [];
  tabs.forEach((sh, i) => {
    if (cols[i].lastKey !== last) return;
    sh.deleteColumn(cols[i].lastCol);
    done.push(sh.getName());
  });
  ui.alert(`Removed ${last} from ${done.join(', ')}.`);
}
