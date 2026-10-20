/**
 * HK weekly perf sheet updater (Apps Script, bound to "HK FI - Weekly Performance Projections").
 *
 * Each Monday weekly/hk.py posts HK_Weekly_<week>.csv to Slack: metric names in column A, the week's values
 * in column B. utils/report_tag.py adds two tags to the right of the table (D1:E2):
 *   report      | HK Weekly
 *   week_start  | 2026-10-12
 *
 * To fill a week: File > Import > Upload > "Insert new sheet(s)" (the CSV comes in as one tab named after
 * the file). The script reads the tags first and only goes ahead if the report is THIS_REPORT below. Any
 * other file (another report's, or one without tags) is refused and nothing in the sheet changes. For a
 * matching file, on the Projections tab, in the tagged week:
 *   - inserts the actual column next to the week's projection column (a week that already has an actual
 *     column is overwritten, not duplicated)
 *   - copies the formatting, and the formula rows (% growth, USD, promo per ride, ...) from earlier actual
 *     columns, and writes the numbers into the value rows
 * On success the imported tab is deleted. On an error it stays, with the reason in red under the tags, and
 * the Projections tab is untouched: it's checked before anything is written.
 *
 * Setup, once: Extensions > Apps Script, paste this file, save, reload the sheet, then
 * HK Weekly > Enable auto-update.
 */

const THIS_REPORT = 'HK Weekly';

const REPORT_KEY = 'report';
const WEEK_KEY = 'week_start';

// Row labels checked before writing (source metric -> text the target's label cell must contain), so a row
// added or removed in the tab stops the run instead of shifting every number below it.
const HK_ANCHORS = {
  completed_trips: 'Completed Trips',
  completed_trips_phv: 'Completed Trips (PHV)',
  completed_trips_taxi: 'Completed Trips (Taxi)',
  platform_fee_per_completed_ride_taxi: 'Platform Fee per completed ride',
};

// Per report:
//   mainTab     name of the tagged imported tab (a CSV import names its tab after the file, so '')
//   companions  other tabs the same file brings in, by name
//   sources     per imported tab: metric names run down column A from row 2; firstMetric/minMetrics check
//               it's the expected table
//   targets     per perf tab: source and column to read (header text, or 1-based number), kind (pair/single),
//               the header row holding the week dates, first metric row, metric count, label column, anchors
const REPORTS = {
  'HK Weekly': {
    mainTab: '',
    companions: {},
    sources: {
      main: { firstMetric: 'completed_trips', minMetrics: 81 },
    },
    targets: [
      { tab: 'Projections', source: 'main', column: 2, kind: 'pair', dateRow: 4, firstRow: 5, metrics: 81, labelCol: 1, anchors: HK_ANCHORS },
    ],
  },
};

const FORMULA_LOOKBACK = 12;  // actual columns searched back for a formula row's formula
const TAG_SCAN_COLS = 30;     // columns of row 1 searched for the report tag

// ---------- menu and triggers ----------

function onOpen() {
  SpreadsheetApp.getUi().createMenu(THIS_REPORT)
    .addItem('Update now', 'updateNow')
    .addItem('Fill a specific week…', 'fillSpecificWeek')
    .addItem("Remove a week's actuals…", 'removeWeek')
    .addItem('Enable auto-update', 'enableAutoUpdate')
    .addToUi();
}

function updateNow() { run_(true, null); }

function fillSpecificWeek() {
  const week = askWeek_('Fill a specific week', "Week start (the Monday), e.g. 2026-10-12. That week's actual column is overwritten.");
  if (week) run_(true, week);
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
  ss.toast(`Auto-update is on: import a ${THIS_REPORT} file as new sheets and the week fills itself.`, THIS_REPORT, 8);
}

function askWeek_(title, text) {
  const ui = SpreadsheetApp.getUi();
  const r = ui.prompt(title, text, ui.ButtonSet.OK_CANCEL);
  if (r.getSelectedButton() !== ui.Button.OK) return null;
  const week = r.getResponseText().trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(week)) { ui.alert(`"${week}" is not a date like 2026-10-12.`); return null; }
  return week;
}

// ---------- run ----------

function run_(manual, weekOverride) {
  const ss = SpreadsheetApp.getActive();
  const lock = LockService.getDocumentLock();
  if (!lock.tryLock(manual ? 30000 : 1000)) return;
  let imp = null;
  try {
    const found = findImports_(ss);
    if (!found.length) {
      if (manual) throw new Error(`No tagged file found. Import the ${THIS_REPORT} file with File > Import > Upload > "Insert new sheet(s)". Files posted before the report/week_start tags were added can't be used.`);
      return;
    }
    imp = found[0];
    // A failed import stays with its error noted; rerun it from the menu, not on every later edit.
    if (!manual && String(errorCell_(imp).getValue()).indexOf('Not filled') === 0) return;
    if (found.length > 1)
      throw new Error(`${found.length} imported files are in the sheet (${found.map(f => f.sheet.getName()).join(', ')}). Delete the ones you don't need, then use ${THIS_REPORT} > Update now.`);

    // The check that matters most: is this file for this spreadsheet?
    if (imp.report !== THIS_REPORT)
      throw new Error(`This is a ${imp.report} file, but this spreadsheet takes ${THIS_REPORT}. Nothing was changed; delete the imported tab.`);
    const cfg = REPORTS[THIS_REPORT];
    if (!cfg) throw new Error(`No layout is set up for ${THIS_REPORT} in the script yet.`);

    const tz = ss.getSpreadsheetTimeZone() || Session.getScriptTimeZone() || 'Asia/Singapore';
    const week = weekOverride || weekKey_(imp.weekValue, tz);
    const sources = readSources_(ss, cfg, imp);

    // Plan every tab before writing anything, so a problem stops the run with the sheet untouched.
    const plans = cfg.targets.map(t => plan_(ss, t, sources[t.source], week, tz));
    const notes = [];
    plans.forEach(p => notes.push(...(p.t.kind === 'pair' ? applyPair_(p) : applySingle_(p))));
    SpreadsheetApp.flush();

    imp.sheets.forEach(sh => ss.deleteSheet(sh));
    const msg = `Filled week of ${week}.` + (notes.length ? ' Check: ' + notes.join('; ') : '');
    if (manual) SpreadsheetApp.getUi().alert(msg);
    else ss.toast(msg, THIS_REPORT, 15);
  } catch (err) {
    const msg = 'Not filled: ' + err.message;
    if (imp) errorCell_(imp).setValue(msg).setFontColor('#c5221f').setFontWeight('bold');
    if (manual) SpreadsheetApp.getUi().alert(msg);
    else ss.toast(msg, THIS_REPORT, 30);
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
    found.push({
      sheet: sh,
      tagCol: col + 1,  // 1-based column of the tag labels
      report: String(rows[0][col + 1]).trim(),
      weekLabel: String(rows[1][col]).trim(),
      weekValue: rows[1][col + 1],
    });
  });
  return found;
}

function errorCell_(imp) { return imp.sheet.getRange(4, imp.tagCol); }

function weekKey_(v, tz) {
  const week = v instanceof Date ? key_(v, tz) : String(v).trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(week))
    throw new Error(`The file's ${WEEK_KEY} tag should be a date like 2026-10-12, got "${v}".`);
  return week;
}

// The tagged tab plus its companions, each parsed into { metrics, header, rows }. Companion tabs share the
// tagged tab's name suffix, e.g. "Weekly Report 2" pairs with "Payment Method 2".
function readSources_(ss, cfg, imp) {
  const name = imp.sheet.getName();
  const suffix = cfg.mainTab && name.indexOf(cfg.mainTab) === 0 ? name.slice(cfg.mainTab.length) : '';
  const tabs = { main: imp.sheet };
  imp.sheets = [imp.sheet];
  Object.keys(cfg.companions || {}).forEach(key => {
    const sh = ss.getSheetByName(cfg.companions[key] + suffix);
    if (!sh) throw new Error(`The file's "${cfg.companions[key]}" tab wasn't imported.`);
    tabs[key] = sh;
    imp.sheets.push(sh);
  });

  const sources = {};
  Object.keys(cfg.sources).forEach(key => {
    const sh = tabs[key];
    const width = key === 'main' ? imp.tagCol - 1 : sh.getLastColumn();  // stop short of the tags
    const vals = sh.getRange(1, 1, Math.max(sh.getLastRow(), 2), width).getValues();
    const metrics = [];
    for (let i = 1; i < vals.length && vals[i][0] !== ''; i++) metrics.push(String(vals[i][0]).trim());
    const want = cfg.sources[key];
    if (metrics[0] !== want.firstMetric || metrics.length < want.minMetrics)
      throw new Error(`The "${sh.getName()}" tab should start with ${want.firstMetric} and have at least ${want.minMetrics} metrics; it has ${metrics[0]} / ${metrics.length}.`);
    sources[key] = { metrics, header: vals[0].map(v => String(v).trim()), rows: vals.slice(1, 1 + metrics.length) };
  });
  return sources;
}

function sourceColumn_(src, t) {
  const idx = typeof t.column === 'number' ? t.column - 1 : src.header.indexOf(t.column);
  if (idx < 0 || idx >= src.header.length) throw new Error(`The imported file has no "${t.column}" column.`);
  return src.rows.slice(0, t.metrics).map(r => toNumber_(r[idx]));
}

// A CSV import keeps numbers longer than 15 digits (e.g. 0.24399450926561428) as text; written as text they
// lose the target cell's number format, so turn anything that reads as a number back into one.
function toNumber_(v) {
  return typeof v === 'string' && /^\s*-?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?\s*$/.test(v) ? Number(v) : v;
}

// ---------- dates ----------

// Header dates are midnight in the sheet's zone; reading them at noon keeps the day right if tz differs.
function key_(d, tz) { return Utilities.formatDate(new Date(d.getTime() + 12 * 3600 * 1000), tz, 'yyyy-MM-dd'); }

// date key -> columns carrying that date in the header row; a pair week with actuals has two
function dateCols_(sh, row, tz) {
  const vals = sh.getRange(row, 1, 1, sh.getLastColumn()).getValues()[0];
  const m = {};
  vals.forEach((v, i) => {
    if (v instanceof Date) (m[key_(v, tz)] = m[key_(v, tz)] || []).push(i + 1);
  });
  return m;
}

// ---------- perf tabs ----------

function plan_(ss, t, src, week, tz) {
  const sh = ss.getSheetByName(t.tab);
  if (!sh) throw new Error(`Tab "${t.tab}" not found.`);
  const m = dateCols_(sh, t.dateRow, tz);
  const cols = m[week];
  if (!cols) throw new Error(`${t.tab}: no column dated ${week} in row ${t.dateRow}.`);

  const labels = sh.getRange(t.firstRow, t.labelCol, t.metrics, 1).getValues().map(r => String(r[0]).trim().toLowerCase());
  Object.keys(t.anchors || {}).forEach(metric => {
    const i = src.metrics.indexOf(metric);
    if (i < 0 || i >= t.metrics) return;
    if (labels[i].indexOf(t.anchors[metric].toLowerCase()) < 0)
      throw new Error(`${t.tab}: row ${t.firstRow + i} should be "${t.anchors[metric]}" but is "${labels[i]}". Has a row been added or removed?`);
  });

  const values = sourceColumn_(src, t);
  if (t.kind === 'single') return { sh, t, values, col: cols[cols.length - 1] };

  if (cols.length > 2) throw new Error(`${t.tab}: ${cols.length} columns dated ${week}; expected projection + actual.`);
  const prevActuals = Object.keys(m).filter(k => k < week && m[k].length === 2).sort().reverse()
    .slice(0, FORMULA_LOOKBACK).map(k => m[k][1]);
  if (!prevActuals.length) throw new Error(`${t.tab}: no earlier actual column to copy formulas from.`);
  return { sh, t, values, projCol: cols[0], actualCol: cols[1] || null, prevActuals };
}

function applyPair_(p) {
  const { sh, t, prevActuals, values } = p;
  const prev = prevActuals[0];
  const top = t.dateRow - 1;  // the days-in-week row above the dates
  const height = t.firstRow + t.metrics - top;

  let col = p.actualCol;
  if (!col) {
    sh.insertColumnAfter(p.projCol);
    col = p.projCol + 1;
    sh.setColumnWidth(col, sh.getColumnWidth(prev));
  }
  sh.getRange(top, prev, height, 1)
    .copyTo(sh.getRange(top, col, height, 1), SpreadsheetApp.CopyPasteType.PASTE_FORMAT, false);
  sh.getRange(top, col).setValue(sh.getRange(top, prev).getValue());
  sh.getRange(t.dateRow, col).setValue(sh.getRange(t.dateRow, p.projCol).getValue());

  // Value rows take the file's numbers; formula rows (blank in the file) take the most recent actual
  // column's formula for that row, in R1C1 so references shift exactly as a manual copy-paste would.
  const history = prevActuals.map(c => ({
    formulas: sh.getRange(t.firstRow, c, t.metrics, 1).getFormulasR1C1().map(r => r[0]),
    values: c === prev ? sh.getRange(t.firstRow, c, t.metrics, 1).getValues().map(r => r[0]) : null,
  }));
  const formulaRows = [], typedRows = [];
  const out = values.map((v, i) => {
    if (v !== '' && v !== null) return [v];
    const src = history.find(h => h.formulas[i]);
    if (src) formulaRows.push([i, src.formulas[i]]);
    else if (history[0].values[i] !== '') typedRows.push(t.firstRow + i);
    return [''];
  });
  sh.getRange(t.firstRow, col, t.metrics, 1).setValues(out);
  formulaRows.forEach(([i, f]) => sh.getRange(t.firstRow + i, col).setFormulaR1C1(f));
  // Number formats (%, #,##0, ...) from the previous actual column, set again after writing so nothing
  // written can leave a row in the wrong format.
  sh.getRange(t.firstRow, col, t.metrics, 1)
    .setNumberFormats(sh.getRange(t.firstRow, prev, t.metrics, 1).getNumberFormats());

  return typedRows.length
    ? [`${t.tab} rows ${typedRows.join(',')} left blank (last week had typed numbers, no formula to copy)`]
    : [];
}

function applySingle_(p) {
  p.sh.getRange(p.t.firstRow, p.col, p.t.metrics, 1).setValues(p.values.map(v => [v === '' ? 0 : v]));
  return [];
}

// ---------- undo ----------

// Takes a week's actuals back out: deletes its actual column on pair tabs, clears its numbers on single tabs.
function removeWeek() {
  const ui = SpreadsheetApp.getUi();
  const cfg = REPORTS[THIS_REPORT];
  if (!cfg) { ui.alert(`No layout is set up for ${THIS_REPORT} in the script yet.`); return; }
  const week = askWeek_("Remove a week's actuals", 'Week start (the Monday), e.g. 2026-10-12.');
  if (!week) return;

  const ss = SpreadsheetApp.getActive();
  const tz = ss.getSpreadsheetTimeZone() || Session.getScriptTimeZone() || 'Asia/Singapore';
  const hits = cfg.targets.map(t => {
    const sh = ss.getSheetByName(t.tab);
    const cols = sh ? dateCols_(sh, t.dateRow, tz)[week] || [] : [];
    return { t, sh, cols };
  }).filter(o => (o.t.kind === 'pair' ? o.cols.length === 2 : o.cols.length > 0));
  if (!hits.length) { ui.alert(`Nothing to remove: no actuals found for ${week}.`); return; }

  const pairs = hits.filter(o => o.t.kind === 'pair').map(o => o.t.tab);
  const ok = ui.alert(`Remove actuals for ${week}?`,
    `Deletes the actual column on ${pairs.join(', ') || 'no tab'} and clears that week's numbers on the other tabs.`,
    ui.ButtonSet.YES_NO);
  if (ok !== ui.Button.YES) return;
  hits.forEach(o => {
    if (o.t.kind === 'pair') o.sh.deleteColumn(o.cols[1]);
    else o.sh.getRange(o.t.firstRow, o.cols[o.cols.length - 1], o.t.metrics, 1).clearContent();
  });
  ui.alert(`Removed actuals for ${week}.`);
}
