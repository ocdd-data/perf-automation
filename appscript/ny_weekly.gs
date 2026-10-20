/**
 * NY weekly perf sheet updater (Apps Script, bound to "NY - Weekly Performance 2026").
 *
 * Each Monday weekly/ny.py posts NY_Weekly_<week>.xlsx to Slack. Its "Weekly Report" tab is tagged by
 * utils/report_tag.py (F1:G2): report = NY Weekly, week_start = the week's Monday.
 *
 * To fill a week: File > Import > Upload > "Insert new sheet(s)". The script reads the tags first and only
 * goes ahead for a NY Weekly file; anything else is refused and nothing changes. Then, on the 2026 Weekly
 * tab:
 *   - if the week has no column yet (it is the week after the last one), the column is added: date, days,
 *     the month label in row 2 (extended, or a new month started in the colour used five months earlier)
 *     and the year label in row 1
 *   - every row label is checked against the file's, so a row added or removed in either stops the run
 *   - value rows get the file's numbers, in the file's number format ($, %, ...)
 *   - formula rows (% Growth, % Resurrect, % Churn) get the formula from the latest earlier week
 *   - rows the file leaves empty (e.g. Retained Driver Pct, which arrives later) are left as they are,
 *     so a number typed in by hand is never wiped
 *   - the month box is redrawn from the row 3 dates: a black right border, rows 2-89, on each month's
 *     last week
 * The week's formatting (alignment, fonts, fill, section lines, number formats) is copied from the latest
 * filled week, then the month border is drawn. Re-importing a week overwrites it. On success the imported tabs are deleted; on an error they stay, with the reason in red at
 * F4 of the imported tab, and the 2026 Weekly tab is untouched.
 *
 * Setup: Extensions > Apps Script, paste this file, save, reload the sheet, NY Weekly > Enable auto-update.
 * When a "2027 Weekly" tab is started, change TAB below.
 */

const REPORT = 'NY Weekly';
const TAB = '2026 Weekly';
const YEAR_ROW = 1;     // "2026", merged across the week columns
const MONTH_ROW = 2;    // month names, merged across each month's weeks
const DATE_ROW = 3;     // week start dates
const DAYS_ROW = 4;     // 7
const FIRST_ROW = 5;    // first metric row (Bookings, Overall)
const METRICS = 85;     // rows 5-89, Bookings .. Total NY Sales Tax
const LAST_ROW = FIRST_ROW + METRICS - 1;
const LABEL_COL = 3;    // metric labels in column C
const COLOUR_CYCLE = 5; // a new month's label takes the colour of the label this many months earlier

// The imported "Weekly Report" tab (ny.py build_workbook): labels in C, values in D, from row 4.
const SRC_FIRST_ROW = 4;
const SRC_LABEL_COL = 3;
const SRC_VALUE_COL = 4;
const COMPANION_TAB = 'Payment Method';  // also imported; not used by this sheet, removed with the import

const REPORT_KEY = 'report';
const WEEK_KEY = 'week_start';
const TAG_SCAN_COLS = 30;
const FORMULA_LOOKBACK = 12;  // earlier week columns searched for a formula row's formula

function onOpen() {
  SpreadsheetApp.getUi().createMenu(REPORT)
    .addItem('Update now', 'updateNow')
    .addItem('Fill a specific week…', 'fillSpecificWeek')
    .addItem("Clear a week's numbers…", 'clearWeek')
    .addItem('Enable auto-update', 'enableAutoUpdate')
    .addToUi();
}

function updateNow() { run_(true, null); }

function fillSpecificWeek() {
  const week = askWeek_('Fill a specific week', "Week start (the Monday), e.g. 2026-10-12. That week's column is overwritten.");
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
  ss.toast(`Auto-update is on: import a ${REPORT} file as new sheets and the week fills itself.`, REPORT, 8);
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
      if (manual) throw new Error(`No tagged file found. Import the ${REPORT} file with File > Import > Upload > "Insert new sheet(s)". Files posted before the report/week_start tags were added can't be used.`);
      return;
    }
    imp = found[0];
    // A failed import stays with its error noted; rerun it from the menu, not on every later edit.
    if (!manual && String(errorCell_(imp).getValue()).indexOf('Not filled') === 0) return;
    if (found.length > 1)
      throw new Error(`${found.length} imported files are in the sheet (${found.map(f => f.sheet.getName()).join(', ')}). Delete the ones you don't need, then use ${REPORT} > Update now.`);
    if (imp.report !== REPORT)
      throw new Error(`This is a ${imp.report} file, but this spreadsheet takes ${REPORT}. Nothing was changed; delete the imported tab.`);

    const tz = ss.getSpreadsheetTimeZone() || Session.getScriptTimeZone() || 'America/New_York';
    const week = weekOverride || weekKey_(imp.weekValue, tz);
    const src = readSource_(imp.sheet);

    const sh = ss.getSheetByName(TAB);
    if (!sh) throw new Error(`Tab "${TAB}" not found.`);
    // Everything is checked before anything is written.
    checkLabels_(sh, src);
    let m = dateCols_(sh, tz);
    const isNew = !m[week];
    if (isNew) checkCanAdd_(m, week);
    if (!Object.keys(m).some(k => k < week)) throw new Error(`${TAB}: no earlier week to copy formulas from.`);

    // Formatting comes from the latest week that has numbers in it: a week column set up ahead of time but
    // never filled lacks the alignment etc. that filled weeks have.
    const styleCol = latestFilled_(sh, m, week);
    if (!styleCol) throw new Error(`${TAB}: no earlier filled week to copy the formatting from.`);

    if (isNew) { addWeek_(sh, m, week, tz, styleCol); m = dateCols_(sh, tz); }
    const col = m[week][0];
    const earlier = Object.keys(m).filter(k => k < week).sort().reverse().slice(0, FORMULA_LOOKBACK).map(k => m[k][0]);
    const empty = fillColumn_(sh, col, earlier, src, styleCol);
    drawMonthBoxes_(sh, m, week);
    SpreadsheetApp.flush();

    removeImport_(ss, imp);
    const msg = `Filled week of ${week}${isNew ? ' (column added)' : ''}.` + (empty.length ? ` No value in the file for: ${empty.join(', ')}.` : '');
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
    found.push({ sheet: sh, tagCol: col + 1, report: String(rows[0][col + 1]).trim(), weekValue: rows[1][col + 1] });
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

function readSource_(sh) {
  const labels = sh.getRange(SRC_FIRST_ROW, SRC_LABEL_COL, METRICS, 1).getValues().map(r => String(r[0]).trim());
  const valueRange = sh.getRange(SRC_FIRST_ROW, SRC_VALUE_COL, METRICS, 1);
  return {
    labels,
    values: valueRange.getValues().map(r => r[0]),
    formats: valueRange.getNumberFormats().map(r => r[0]),
  };
}

// The imported tab, plus the Payment Method tab that came with it (same name suffix, e.g. " 2").
function removeImport_(ss, imp) {
  const name = imp.sheet.getName();
  const suffix = name.indexOf('Weekly Report') === 0 ? name.slice('Weekly Report'.length) : '';
  const companion = ss.getSheetByName(COMPANION_TAB + suffix);
  ss.deleteSheet(imp.sheet);
  if (companion) ss.deleteSheet(companion);
}

// ---------- dates ----------

// Header dates are midnight in the sheet's zone; reading them at noon keeps the day right if tz differs.
function key_(d, tz) { return Utilities.formatDate(new Date(d.getTime() + 12 * 3600 * 1000), tz, 'yyyy-MM-dd'); }

function addDays_(key, days) {
  const [y, mo, d] = key.split('-').map(Number);
  return Utilities.formatDate(new Date(Date.UTC(y, mo - 1, d + days)), 'UTC', 'yyyy-MM-dd');
}

// date key -> [column] for every week column dated in row 3
function dateCols_(sh, tz) {
  const vals = sh.getRange(DATE_ROW, 1, 1, sh.getLastColumn()).getValues()[0];
  const m = {};
  vals.forEach((v, i) => {
    if (v instanceof Date) (m[key_(v, tz)] = m[key_(v, tz)] || []).push(i + 1);
  });
  return m;
}

// ---------- 2026 Weekly ----------

function checkLabels_(sh, src) {
  const labels = sh.getRange(FIRST_ROW, LABEL_COL, METRICS, 1).getValues().map(r => String(r[0]).trim());
  labels.forEach((label, i) => {
    if (label.toLowerCase() !== src.labels[i].toLowerCase())
      throw new Error(`${TAB} row ${FIRST_ROW + i} is "${label}" but the file has "${src.labels[i]}" there. Has a row been added or removed?`);
  });
}

// A missing week can only be added straight after the last one, and only within the tab's year.
function checkCanAdd_(m, week) {
  const keys = Object.keys(m).sort();
  const last = keys[keys.length - 1];
  if (week < last) throw new Error(`${TAB} has no column dated ${week}, and it is before the last week (${last}).`);
  if (week !== addDays_(last, 7))
    throw new Error(`${TAB} ends at the week of ${last}; ${week} isn't the next week. Fill the weeks in order.`);
  if (week.slice(0, 4) !== keys[0].slice(0, 4))
    throw new Error(`${week} is in a new year. Start a "${week.slice(0, 4)} Weekly" tab and change TAB in the script.`);
}

// The column of the latest week before `week` whose first metric row has a value, or null.
function latestFilled_(sh, m, week) {
  const firstRow = sh.getRange(FIRST_ROW, 1, 1, sh.getLastColumn()).getValues()[0];
  const key = Object.keys(m).filter(k => k < week).sort().reverse().find(k => firstRow[m[k][0] - 1] !== '');
  return key ? m[key][0] : null;
}

// Adds the week's column after the last week: formatting from the latest filled week, date, days, and the
// year and month labels.
function addWeek_(sh, m, week, tz, styleCol) {
  const keys = Object.keys(m).sort();
  const lastKey = keys[keys.length - 1];
  const last = m[lastKey][0];
  const col = last + 1;
  sh.insertColumnAfter(last);
  sh.setColumnWidth(col, sh.getColumnWidth(last));

  const body = LAST_ROW - DATE_ROW + 1;
  sh.getRange(DATE_ROW, styleCol, body, 1)
    .copyTo(sh.getRange(DATE_ROW, col, body, 1), SpreadsheetApp.CopyPasteType.PASTE_FORMAT, false);
  sh.getRange(DATE_ROW, col).setValue(Utilities.parseDate(week, tz, 'yyyy-MM-dd'));
  sh.getRange(DAYS_ROW, col).setValue(sh.getRange(DAYS_ROW, last).getValue());

  extendMerge_(sh, YEAR_ROW, last, col);
  if (week.slice(0, 7) === lastKey.slice(0, 7)) {
    extendMerge_(sh, MONTH_ROW, last, col);
  } else {
    // New month: a label like the last month's, in the colour used COLOUR_CYCLE months earlier.
    const labels = monthLabels_(sh, last);
    const prev = labels[labels.length - 1];
    const source = labels[labels.length - COLOUR_CYCLE];
    // Style set field by field: copying from a merged label cell can be refused by Sheets.
    sh.getRange(MONTH_ROW, col)
      .setValue(Utilities.formatDate(Utilities.parseDate(week, tz, 'yyyy-MM-dd'), tz, 'MMMM'))
      .setFontFamily(prev.getFontFamily()).setFontSize(prev.getFontSize())
      .setFontWeight(prev.getFontWeight()).setFontStyle(prev.getFontStyle())
      .setBackground(prev.getBackground())
      .setHorizontalAlignment(prev.getHorizontalAlignment()).setVerticalAlignment(prev.getVerticalAlignment())
      .setFontColor(source ? source.getFontColor() : '#000000');
  }
}

// Widens the merged block in `row` that ends at column `last` so it also covers `col`.
function extendMerge_(sh, row, last, col) {
  const cell = sh.getRange(row, last);
  const merged = cell.isPartOfMerge() ? cell.getMergedRanges()[0] : cell;
  const start = merged.getColumn();
  merged.breakApart();
  sh.getRange(row, start, 1, col - start + 1).merge();
}

// The month labels in row 2 up to column `upTo`, left to right (the first cell of each merged block).
function monthLabels_(sh, upTo) {
  const vals = sh.getRange(MONTH_ROW, 1, 1, upTo).getValues()[0];
  const out = [];
  vals.forEach((v, i) => { if (String(v).trim() && i + 1 > LABEL_COL) out.push(sh.getRange(MONTH_ROW, i + 1)); });
  return out;
}

// The black month-end border (right side, rows 2-89) on the last week of the week's month and of the month
// before; any other week of those months has none.
function drawMonthBoxes_(sh, m, week) {
  const byMonth = {};
  Object.keys(m).forEach(k => (byMonth[k.slice(0, 7)] = byMonth[k.slice(0, 7)] || []).push(m[k][0]));
  const months = Object.keys(byMonth).sort();
  const i = months.indexOf(week.slice(0, 7));
  [months[i - 1], months[i]].filter(Boolean).forEach(month => {
    const cols = byMonth[month].sort((a, b) => a - b);
    const end = cols[cols.length - 1];
    cols.forEach(c => sh.getRange(MONTH_ROW, c, LAST_ROW - MONTH_ROW + 1, 1)
      .setBorder(null, null, null, c === end, null, null, '#000000', SpreadsheetApp.BorderStyle.SOLID));
  });
}

// Writes one week's column. Returns the labels of rows the file had no value for (left untouched).
function fillColumn_(sh, col, earlier, src, styleCol) {
  // All formatting (alignment, fonts, fill, section lines, number formats) as in the latest filled week;
  // drawMonthBoxes_ then sets the month border.
  const body = LAST_ROW - DATE_ROW + 1;
  if (col !== styleCol)
    sh.getRange(DATE_ROW, styleCol, body, 1)
      .copyTo(sh.getRange(DATE_ROW, col, body, 1), SpreadsheetApp.CopyPasteType.PASTE_FORMAT, false);
  const prevFormats = sh.getRange(FIRST_ROW, styleCol, METRICS, 1).getNumberFormats().map(r => r[0]);
  const history = earlier.map(c => sh.getRange(FIRST_ROW, c, METRICS, 1).getFormulasR1C1().map(r => r[0]));
  const empty = [];
  const formats = [];
  let run = null;  // consecutive value rows, written together
  const flush = () => {
    if (!run) return;
    sh.getRange(FIRST_ROW + run.start, col, run.values.length, 1).setValues(run.values.map(v => [v]));
    run = null;
  };
  src.values.forEach((v, i) => {
    if (v !== '' && v !== null) {
      if (!run) run = { start: i, values: [] };
      run.values.push(v);
      // The earlier week's number format; where that is General (the money rows, typed in as text like
      // "$31.54" in earlier weeks), the file's own format ($#,##0.00) so the number still reads as money.
      const p = prevFormats[i];
      formats.push([p && p !== 'General' ? p : (src.formats[i] || 'General')]);
      return;
    }
    flush();
    formats.push([prevFormats[i] || 'General']);
    const f = history.map(h => h[i]).find(x => x);
    if (f) sh.getRange(FIRST_ROW + i, col).setFormulaR1C1(f);
    else if (src.labels[i]) empty.push(`${src.labels[i]} (row ${FIRST_ROW + i})`);
  });
  flush();
  sh.getRange(FIRST_ROW, col, METRICS, 1).setNumberFormats(formats);
  return empty;
}

// ---------- undo ----------

function clearWeek() {
  const ui = SpreadsheetApp.getUi();
  const week = askWeek_("Clear a week's numbers", 'Week start (the Monday), e.g. 2026-10-12.');
  if (!week) return;
  const ss = SpreadsheetApp.getActive();
  const tz = ss.getSpreadsheetTimeZone() || Session.getScriptTimeZone() || 'America/New_York';
  const sh = ss.getSheetByName(TAB);
  const cols = dateCols_(sh, tz)[week];
  if (!cols) { ui.alert(`${TAB} has no column dated ${week}.`); return; }
  const ok = ui.alert(`Clear ${week}?`, `Clears rows ${FIRST_ROW}-${LAST_ROW} of that week's column on ${TAB}. The column, its date and the month box stay.`, ui.ButtonSet.YES_NO);
  if (ok !== ui.Button.YES) return;
  sh.getRange(FIRST_ROW, cols[0], METRICS, 1).clearContent();
  ui.alert(`Cleared ${week}.`);
}
