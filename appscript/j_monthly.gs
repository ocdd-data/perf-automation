/**
 * Monthly report (J) updater (Apps Script, bound to "Monthly report updated summary format").
 *
 * On the 1st of each month monthly/all_j.py posts Monthly_Report_J_<Mon>_<YYYY>.xlsx to Slack: one tab per
 * country (SG, KH, VN, TH, HK, NY), each a month header (A1, e.g. Sep_2026) and 15 values (A2:A16) in the
 * report's row order. The SG tab is tagged by utils/report_tag.py (C1:D2): report = J Monthly,
 * month_start = the month's first day.
 *
 * To fill a month: File > Import > Upload > "Insert new sheet(s)". The script reads the tags first and only
 * goes ahead for a J Monthly file; anything else is refused and nothing changes. Then, for e.g. Oct 2026:
 *   1. duplicates last month's tab ("Sep 26") as "Oct 26", first in the tab list
 *   2. in every country block, moves last month's numbers (column D/J) out and this month's into their
 *      place: the previous-month column takes Sep's values (exact values; MKT $ / Ride included, formulas
 *      such as live exchange rates frozen at their value), and the current-month column is cleared
 *   3. renames the two month headers (September, October)
 *   4. writes the file's 15 values into the current-month column; MKT $ / Ride is left empty to fill in
 *   5. adds the New York block next to Hong Kong if the tab doesn't have one yet (Thailand's block as the
 *      template; Target left blank to fill in, vs Target shows once it is)
 * M-o-M %, Target and vs Target are left as they are. Every block's country name and row labels are
 * checked before anything is written. Importing a month whose tab already exists refills its current-month
 * column only. On success the imported tabs are deleted; on an error they stay, with the reason in red at
 * C4 of the imported SG tab, and the report is untouched.
 *
 * Setup: Extensions > Apps Script, paste this file, save, reload the sheet, J Monthly > Enable auto-update.
 */

const REPORT = 'J Monthly';
const REPORT_KEY = 'report';
const MONTH_KEY = 'month_start';
const TAG_SCAN_COLS = 30;
const MAIN_TAB = 'SG';                       // the tagged imported tab
const SOURCES = ['SG', 'KH', 'VN', 'TH', 'HK', 'NY'];
const VALUES = 15;                           // values per country in the file
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const MONTH_NAMES = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
  'October', 'November', 'December'];
const LABEL_COL = 3;                         // row labels in column C, shared by the blocks side by side

// The 16 rows of every block: the file's 15 values in this order, then MKT $ / Ride (not in the file).
const LABELS = ['Total Completed Rides', 'Average Daily Rides', 'ETA', 'Cater Rate', 'MAU - Passenger',
  'Total Completed Passenger', 'New activations', 'Resurrection', 'Churn', 'Net inflow/(outflow)',
  'Total Completed Driver', 'New activations', 'Resurrection', 'Churn', 'Net inflow/(outflow)', 'MKT $ / Ride'];

// Per block: the file tab, the country name in its title row, the title row, and its previous- and
// current-month columns. The header row is title + 1, the 16 rows start at title + 2. New York is added
// from Thailand's block when a tab doesn't have it.
const BLOCKS = [
  { src: 'SG', country: 'Singapore', title: 4, prev: 'D', cur: 'E' },
  { src: 'KH', country: 'Cambodia', title: 4, prev: 'J', cur: 'K' },
  { src: 'VN', country: 'Vietnam', title: 23, prev: 'D', cur: 'E' },
  { src: 'TH', country: 'Thailand', title: 23, prev: 'J', cur: 'K' },
  { src: 'HK', country: 'Hong Kong', title: 42, prev: 'D', cur: 'E' },
  { src: 'NY', country: 'New York', title: 42, prev: 'J', cur: 'K', template: 'TH' },
];

// ---------- menu and triggers ----------

function onOpen() {
  SpreadsheetApp.getUi().createMenu(REPORT)
    .addItem('Update now', 'updateNow')
    .addItem('Fill a specific month…', 'fillSpecificMonth')
    .addItem("Remove a month's tab…", 'removeMonth')
    .addItem('Enable auto-update', 'enableAutoUpdate')
    .addToUi();
}

function updateNow() { run_(true, null); }

function fillSpecificMonth() {
  const month = askMonth_('Fill a specific month', "Month, e.g. 2026-09. If its tab exists, its current-month column is refilled.");
  if (month) run_(true, month);
}

// Installable onChange: runs on File > Import and on edits, and returns at once unless a tagged file has
// been imported.
function onChangeHandler(e) { run_(false, null); }

function enableAutoUpdate() {
  const ss = SpreadsheetApp.getActive();
  ScriptApp.getProjectTriggers()
    .filter(t => t.getHandlerFunction() === 'onChangeHandler')
    .forEach(t => ScriptApp.deleteTrigger(t));
  ScriptApp.newTrigger('onChangeHandler').forSpreadsheet(ss).onChange().create();
  ss.toast(`Auto-update is on: import a ${REPORT} file as new sheets and the month's tab is made.`, REPORT, 8);
}

function askMonth_(title, text) {
  const ui = SpreadsheetApp.getUi();
  const r = ui.prompt(title, text, ui.ButtonSet.OK_CANCEL);
  if (r.getSelectedButton() !== ui.Button.OK) return null;
  const month = r.getResponseText().trim();
  if (!/^\d{4}-\d{2}$/.test(month)) { ui.alert(`"${month}" is not a month like 2026-09.`); return null; }
  return month;
}

// ---------- run ----------

function run_(manual, monthOverride) {
  const ss = SpreadsheetApp.getActive();
  const lock = LockService.getDocumentLock();
  if (!lock.tryLock(manual ? 30000 : 1000)) return;
  let imp = null;
  try {
    const found = findImports_(ss);
    if (!found.length) {
      if (manual) throw new Error(`No tagged file found. Import the ${REPORT} file with File > Import > Upload > "Insert new sheet(s)". Files posted before the report/month_start tags were added can't be used.`);
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
    const month = monthOverride || monthKey_(imp.monthValue, tz);
    const sources = readSources_(ss, imp, month);
    const name = tabName_(month), prevName = tabName_(prevMonth_(month));

    // Everything is checked before anything is written.
    let sh = ss.getSheetByName(name);
    const isNew = !sh;
    if (isNew) {
      const from = ss.getSheetByName(prevName);
      if (!from) throw new Error(`There is no "${prevName}" tab to copy for ${name}. Make the months in order.`);
      checkBlocks_(from);
      sh = from.copyTo(ss).setName(name);
      ss.setActiveSheet(sh);
      ss.moveActiveSheet(1);
      addNewYork_(sh);
      shiftMonths_(sh);
    } else {
      checkBlocks_(sh);
      addNewYork_(sh);
    }
    setHeaders_(sh, month);
    BLOCKS.forEach(b => sh.getRange(b.title + 2, colIndex_(b.cur), VALUES, 1).setValues(sources[b.src].map(v => [v])));
    SpreadsheetApp.flush();

    imp.sheets.forEach(s => ss.deleteSheet(s));
    const msg = isNew ? `Made the "${name}" tab from "${prevName}" and filled ${name.slice(0, 3)}.` : `Refilled the current month on "${name}".`;
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

function toNumber_(v) {
  return typeof v === 'string' && /^\s*-?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?\s*$/.test(v) ? Number(v) : v;
}

// country tab -> its 15 values. Tabs share the tagged tab's name suffix (e.g. "SG 2" with "KH 2").
function readSources_(ss, imp, month) {
  const name = imp.sheet.getName();
  const suffix = name.indexOf(MAIN_TAB) === 0 ? name.slice(MAIN_TAB.length) : '';
  const [y, m] = month.split('-').map(Number);
  const expect = `${MONTHS[m - 1]}_${y}`;
  const out = {};
  imp.sheets = [];
  SOURCES.forEach(t => {
    const sh = t === MAIN_TAB ? imp.sheet : ss.getSheetByName(t + suffix);
    if (!sh) throw new Error(`The file's "${t}" tab wasn't imported.`);
    imp.sheets.push(sh);
    const col = sh.getRange(1, 1, VALUES + 1, 1).getValues().map(r => toNumber_(r[0]));
    const head = String(col[0]).trim();
    if (head !== expect) throw new Error(`The "${sh.getName()}" tab is for ${head}, not ${expect}.`);
    const values = col.slice(1);
    const bad = values.findIndex(v => typeof v !== 'number');
    if (bad >= 0) throw new Error(`The "${sh.getName()}" tab's value ${bad + 1} (${LABELS[bad]}) isn't a number: "${values[bad]}".`);
    out[t] = values;
  });
  return out;
}

// ---------- the report ----------

function colIndex_(s) { return s.split('').reduce((n, ch) => n * 26 + ch.charCodeAt(0) - 64, 0); }
function norm_(s) { return String(s).replace(/\s+/g, ' ').trim().toLowerCase(); }
function tabName_(key) { const [y, m] = key.split('-').map(Number); return `${MONTHS[m - 1]} ${String(y).slice(2)}`; }
function prevMonth_(key) {
  let [y, m] = key.split('-').map(Number);
  if (--m < 1) { m = 12; y--; }
  return `${y}-${String(m).padStart(2, '0')}`;
}

// Every block's country name and its 16 row labels (column C). New York may not exist yet.
function checkBlocks_(sh) {
  const labels = sh.getRange(1, LABEL_COL, 60, 1).getValues().map(r => norm_(r[0]));
  BLOCKS.forEach(b => {
    const title = String(sh.getRange(b.title, colIndex_(b.prev)).getValue()).trim();
    if (b.template && !title) return;
    if (norm_(title) !== norm_(b.country))
      throw new Error(`"${sh.getName()}" ${b.prev}${b.title} should be ${b.country} but is "${title}". Has the layout changed?`);
    LABELS.forEach((label, i) => {
      const row = b.title + 2 + i;
      if (labels[row - 1] !== norm_(label))
        throw new Error(`"${sh.getName()}" row ${row} should be "${label}" (${b.country}) but is "${labels[row - 1]}". Has a row been added or removed?`);
    });
  });
}

// Previous-month column takes last month's values (exact; MKT $ / Ride included, formulas frozen at their
// value); the current-month column is cleared, ready for the file's values.
function shiftMonths_(sh) {
  BLOCKS.forEach(b => {
    const from = sh.getRange(b.title + 2, colIndex_(b.cur), LABELS.length, 1);
    from.copyTo(sh.getRange(b.title + 2, colIndex_(b.prev), LABELS.length, 1), SpreadsheetApp.CopyPasteType.PASTE_VALUES, false);
    from.clearContent();
  });
}

function setHeaders_(sh, month) {
  const m = Number(month.split('-')[1]);
  BLOCKS.forEach(b => {
    sh.getRange(b.title + 1, colIndex_(b.prev)).setValue(MONTH_NAMES[(m + 10) % 12]);
    sh.getRange(b.title + 1, colIndex_(b.cur)).setValue(MONTH_NAMES[m - 1]);
  });
}

// Adds the New York block next to Hong Kong if this tab has none: Thailand's block's formatting, the title,
// headers, M-o-M % formulas, and vs Target formulas that stay blank until a Target is entered.
function addNewYork_(sh) {
  BLOCKS.filter(b => b.template).forEach(b => {
    const left = colIndex_(b.prev);
    if (String(sh.getRange(b.title, left).getValue()).trim()) return;
    const t = BLOCKS.find(x => x.src === b.template);
    const height = LABELS.length + 2, width = 5;
    sh.getRange(t.title, colIndex_(t.prev), height, width)
      .copyTo(sh.getRange(b.title, left, height, width), SpreadsheetApp.CopyPasteType.PASTE_FORMAT, false);
    const titleRange = sh.getRange(b.title, left, 1, width);
    if (!sh.getRange(b.title, left).isPartOfMerge()) titleRange.merge();
    sh.getRange(b.title, left).setValue(b.country);
    sh.getRange(b.title + 1, left + 2, 1, 3).setValues([['M-o-M %', 'Target', 'vs Target']]);
    const P = b.prev, C = b.cur, M = String.fromCharCode(colIndex_(C) + 64 + 2);
    const formulas = LABELS.map((_, i) => {
      const r = b.title + 2 + i;
      return [`=(${C}${r}/${P}${r})-1`, '', `=IF(${M}${r}="","",(${C}${r}/${M}${r})-1)`];
    });
    sh.getRange(b.title + 2, left + 2, LABELS.length, 3).setValues(formulas);
  });
}

// ---------- undo ----------

function removeMonth() {
  const ui = SpreadsheetApp.getUi();
  const month = askMonth_("Remove a month's tab", 'Month, e.g. 2026-09.');
  if (!month) return;
  const ss = SpreadsheetApp.getActive();
  const sh = ss.getSheetByName(tabName_(month));
  if (!sh) { ui.alert(`There is no "${tabName_(month)}" tab.`); return; }
  const ok = ui.alert(`Delete the "${sh.getName()}" tab?`, 'Everything on it goes, including anything typed in by hand.', ui.ButtonSet.YES_NO);
  if (ok !== ui.Button.YES) return;
  ss.deleteSheet(sh);
  ui.alert(`Deleted "${tabName_(month)}".`);
}
