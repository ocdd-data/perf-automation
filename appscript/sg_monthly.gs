/**
 * SG monthly perf sheet updater (Apps Script, bound to "SG - Perf Sheet").
 *
 * On the 1st of each month monthly/sg.py posts SG_<Mon>_<YYYY>.csv to Slack: metric names in column A, the
 * month's values in column B. utils/report_tag.py adds two tags to the right of the table (D1:E2):
 *   report       | SG Monthly
 *   month_start  | 2026-09-01
 *
 * To fill a month: File > Import > Upload > "Insert new sheet(s)" (the CSV comes in as one tab named after
 * the file). The script reads the tags first and only goes ahead for a SG Monthly file; anything else is
 * refused and nothing changes. Then, on the "Performance Sheet" tab:
 *   - every mapped row's label is checked first, so a row added or removed stops the run untouched
 *   - the month's column is found from the month names in row 3 (years follow the run of months); if the
 *     month doesn't exist yet and is the month after the last one, a column is added before Growth: days
 *     (row 2), month name (row 3), and the year label in row 1 extended over it, or in a January a new
 *     year started
 *   - values are written by metric name (ROWS below); metrics the file leaves empty (downloads, which are
 *     typed in later) leave the cell as it is, and so does Market Daily Trips (row 107, typed in)
 *   - the Month row (118) gets the month's first day
 *   - formula rows (resurrect > 12 month, resurrected / churned %, TADA daily trips and proportion,
 *     downtime ratio, unique demand, daily bookings / completed) take the formula from the previous month
 *   - formatting is copied from the latest filled month; number formats from the most common format of
 *     the last six filled months, so one mis-formatted cell isn't carried forward
 *   - the Growth column is pointed at the latest filled month vs the month before
 * Re-importing a month overwrites it. On success the imported tab is deleted; on an error it stays, with
 * the reason in red at D4, and the perf tab is untouched.
 *
 * Setup: Extensions > Apps Script, paste this file, save, reload the sheet, SG Monthly > Enable auto-update.
 */

const REPORT = 'SG Monthly';
// Found by how its name starts: the tab's name ends in a space, which is easy to lose. ("Performance",
// the older tab, doesn't match: it has no " Sheet".)
const TAB_PREFIX = 'Performance Sheet';
const YEAR_ROW = 1;    // year labels
const DAYS_ROW = 2;    // days in the month
const MONTH_ROW = 3;   // month names (first three letters are matched), and "Growth"
const FIRST_ROW = 4;   // first metric row
const LAST_ROW = 120;  // last row with a formula (Daily Completed)
const MONTH_DATE_ROW = 118;  // "Month": the month's first day
const LABEL_COL = 2;   // row labels in column B
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

const REPORT_KEY = 'report';
const MONTH_KEY = 'month_start';
const TAG_SCAN_COLS = 30;
const FORMULA_LOOKBACK = 12;  // earlier month columns searched for a formula row's formula
const FORMAT_MONTHS = 6;      // recent filled months whose most common number format each row takes

// [sheet row, metric name in the file, the row's label]. Labels are checked before anything is written.
// Rows 4-120 not listed are formula rows, typed-in rows (Market Daily Trips), the Month row, or blank.
const ROWS = [
  [4, "rides", "Rides"],
  [5, "demand", "Demand"],
  [6, "match_rate", "Match Rate"],
  [7, "completion_rate", "Completion Rate"],
  [8, "daily_rides", "Daily Average Completed Rides"],
  [9, "uncompleted", "Uncompleted"],
  [10, "cater_rate", "Cater Rate"],
  [11, "first_try_cater_rate", "First Try Cater Rate"],
  [12, "retry_initiation_rate", "Retry Initiation Rate"],
  [13, "retry_success_rate", "Retry Success Rate"],
  [14, "daily_median_eta", "Daily Average Median ETA (minutes)"],
  [15, "median_time_to_match_sec", "Median Time to Match (seconds)"],
  [16, "median_time_to_expire_sec", "Median Time to Expire (seconds)"],
  [17, "rides_phv", "Rides"],
  [18, "demand_phv", "Matched Rides"],
  [19, "completed_riders_phv", "PHV Unique Completed Drivers"],
  [20, "phv_approved_drivers", "PHV Total Approved Drivers"],
  [21, "rides_taxi", "Rides"],
  [22, "demand_taxi", "Matched Rides"],
  [23, "completed_riders_taxi", "Taxi Unique Completed Drivers"],
  [24, "taxi_approved_drivers", "Taxi Total Approved Drivers"],
  [25, "driver_mau", "MAU Driver"],
  [26, "completed_driver", "Monthly Completed Driver"],
  [27, "total_approved", "Total Approved Driver"],
  [28, "driver_online_daily", "Daily Average Driver Online"],
  [29, "pinged_drivers_daily", "Daily Average drivers who receive at least 1 ping"],
  [30, "completed_driver_daily", "Daily Average Driver Complete"],
  [31, "online_mau", "Online : MAU"],
  [32, "completed_online", "Online vs completed %"],
  [33, "online_no_complete", "Driver online but did not complete"],
  [34, "ride_per_driver", "Average trips/completed driver"],
  [35, "driver_downloads", "Driver Downloads"],
  [36, "driver_sign_up", "Driver Sign-ups"],
  [37, "driver_sign_up_daily", "Daily Sign ups"],
  [38, "driver_ft_all_time", "Driver First-ride (All time)"],
  [39, "driver_ft_same_month", "First Time Driver (Same Month Sign up)"],
  [40, "driver_sign_up_activation_rate", "Same Month Activation Rate (FT/Signup)"],
  [41, "driver_approved_activation_rate", "Same Month Activation Rate (FT/Approved)"],
  [42, "driver_approved", "Driver Approved"],
  [43, "driver_same_month_approved", "Driver Same Month Approved"],
  [44, "driver_average_online_hours", "Average driver online hours"],
  [45, "driver_average_utilisation_hours", "Average driver utilisation hour"],
  [46, "ping_per_driver_daily", "Average number of ping per driver (Daily Average)"],
  [47, "driver_waiting_before_cancel", "Average waiting time before cancellation (minutes)"],
  [48, "driver_cancellation_rate", "Cancellation rate by drivers"],
  [49, "drivers_ft_unique", "First Time / Unique Completed Drivers"],
  [50, "drivers_repeated", "Repeated Drivers"],
  [51, "resurrect_2_month_driver", "Resurrect 2 Month"],
  [52, "resurrect_3_4_month_driver", "Resurrect 3/4 Month"],
  [53, "resurrect_5_12_month_driver", "Resurrect 5/12 Month"],
  [55, "driver_resurrected_all", "Total Resurrected"],
  [56, "drivers_repeated/unique_complete", "Repeated Drivers / Unique Completed Drivers"],
  [57, "resurrect_2_month_driver/unique_complete", "Resurrect 2 Month / Unique Completed Drivers"],
  [58, "resurrect_3_4_month_driver/unique_complete", "Resurrect 3/4 Month / Unique Completed Drivers"],
  [59, "resurrect_5_12_month_driver/unique_complete", "Resurrect 5/12 Month / Unique Completed Drivers"],
  [62, "driver_churned", "Churned (did not take trip this month)"],
  [64, "driver_inflow", "Inflow"],
  [65, "rider_mau", "MAU Rider"],
  [66, "rider_mau_demand", "MAUR:Demand"],
  [67, "rider_mau_rides", "MAUR:Ride"],
  [68, "r_d_ratio", "MAUR:MAUD Ratio"],
  [69, "rider_downloads", "Rider Downloads"],
  [70, "rider_signup", "Rider Sign-ups"],
  [71, "rider_signup_daily", "Daily Sign-up"],
  [72, "rider_ft_all_time", "First Time Riders (All Time)"],
  [73, "rider_ft_same_month", "First Time Riders (Same month sign up)"],
  [74, "rider_same_month_activation", "Same Month Activation Rate (FT/Signup)"],
  [75, "rider_unique_open_monthly", "Unique users opening our app"],
  [76, "rider_unique_search_monthly", "Unique users making a search"],
  [77, "rider_unique_book_monthly", "Unique users making a booking"],
  [78, "rider_unique_complete_monthly", "Unique users completing a trip"],
  [79, "rider_unique_open_daily", "Unique users opening our app(Daily Average)"],
  [80, "rider_unique_search_daily", "Unique users making a search(Daily Average)"],
  [81, "rider_unique_book_daily", "Unique users making a booking(Daily Average)"],
  [82, "rider_unique_complete_daily", "Unique users completing a trip(Daily Average)"],
  [83, "book_search_ratio_daily", "Book:Search Ratio"],
  [84, "booking_per_user", "Average number of bookings a unique user makes"],
  [85, "complete_per_user", "Average number of trips a unique user makes"],
  [86, "duplicate_ratio", "Duplicate Ratio"],
  [87, "rider_waiting_before_cancel", "Average waiting time before cancellation (minutes)"],
  [88, "rider_cancellation_rate", "Cancellation rate"],
  [89, "riders_ft_unique", "First Time / Unique Completed Riders"],
  [90, "riders_repeated", "Repeated Riders"],
  [91, "resurrect_2_month", "Resurrect 2 Month"],
  [92, "resurrect_3_4_month", "Resurrect 3/4 Month"],
  [93, "resurrect_5_12_month", "Resurrect 5/12 Month"],
  [95, "rider_resurrected_all", "Total Resurrected Riders"],
  [96, "rider_repeated/unique_complete", "Repeated Riders / Unique Completed Riders"],
  [97, "resurrect_2_month_rider/unique_complete", "Resurrect 2 Month / Unique Completed Riders"],
  [98, "resurrect_3_4_month_rider/unique_complete", "Resurrect 3/4 Month / Unique Completed Riders"],
  [99, "resurrect_5_12_month_rider/unique_complete", "Resurrect 5/12 Month / Unique Completed Riders"],
  [102, "rider_churned", "Churned (did not take trip this month)"],
  [104, "rider_inflow", "Inflow"],
];

// ---------- menu and triggers ----------

function onOpen() {
  SpreadsheetApp.getUi().createMenu(REPORT)
    .addItem('Update now', 'updateNow')
    .addItem('Fill a specific month…', 'fillSpecificMonth')
    .addItem('Remove a month…', 'removeMonth')
    .addItem('Enable auto-update', 'enableAutoUpdate')
    .addToUi();
}

function updateNow() { run_(true, null); }

function fillSpecificMonth() {
  const month = askMonth_('Fill a specific month', "Month, e.g. 2026-09. That month's column is overwritten.");
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
  ss.toast(`Auto-update is on: import a ${REPORT} file as new sheets and the month fills itself.`, REPORT, 8);
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
    const src = readSource_(imp, month);

    // Everything is checked before anything is written.
    const p = plan_(ss, src, month);
    const col = p.isNew ? addMonth_(p) : p.col;
    const notes = fillMonth_(p, col);
    updateGrowth_(p.sh, null);
    SpreadsheetApp.flush();

    ss.deleteSheet(imp.sheet);
    const msg = `Filled ${month}${p.isNew ? ' (column added)' : ''}.` + (notes.length ? ' Check: ' + notes.join('; ') : '');
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

// A CSV import keeps numbers longer than 15 digits (e.g. 0.8632859984208833) as text; written as text they
// lose the cell's number format, so turn anything that reads as a number back into one.
function toNumber_(v) {
  return typeof v === 'string' && /^\s*-?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?\s*$/.test(v) ? Number(v) : v;
}

// metric name -> value, from columns A:B of the imported tab (the tags sit further right).
function readSource_(imp, month) {
  const sh = imp.sheet;
  // The header above the values says which month the file is, e.g. Sep_2026: it must agree with the tag.
  const [y, m] = month.split('-').map(Number);
  const expect = `${MONTHS[m - 1]}_${y}`;
  const head = String(sh.getRange(1, 2).getValue()).trim();
  if (head && head !== expect) throw new Error(`The imported file is for ${head}, not ${expect}.`);
  const values = {};
  sh.getRange(2, 1, Math.max(sh.getLastRow() - 1, 1), 2).getValues()
    .forEach(([metric, v]) => { if (String(metric).trim()) values[String(metric).trim()] = toNumber_(v); });
  return values;
}

// ---------- month columns ----------

function nextMonth_(key) {
  let [y, m] = key.split('-').map(Number);
  if (++m > 12) { m = 1; y++; }
  return `${y}-${String(m).padStart(2, '0')}`;
}

function colLetter_(c) { let s = ''; for (; c > 0; c = Math.floor((c - 1) / 26)) s = String.fromCharCode(65 + (c - 1) % 26) + s; return s; }
function colIndex_(s) { return s.split('').reduce((n, ch) => n * 26 + ch.charCodeAt(0) - 64, 0); }

// 'YYYY-MM' -> column. Month names come from row 3 (first three letters, so "June" counts). The year starts
// from the first year label in row 1 and steps up each time the months wrap round (Dec -> Jan); row 1 has
// no label over some years' first months, so it can't be read column by column.
function monthCols_(sh) {
  const width = sh.getLastColumn();
  const names = sh.getRange(MONTH_ROW, 1, 1, width).getValues()[0];
  const labels = sh.getRange(YEAR_ROW, 1, 1, width).getValues()[0];
  const first = sh.getRange(FIRST_ROW, 1, 1, width).getValues()[0];
  const m = {}, filled = {};
  let year = null, prevMonth = 99, lastKey = null, lastCol = null, growthCol = null;
  names.forEach((v, i) => {
    const name = String(v).trim();
    if (name === 'Growth') growthCol = i + 1;
    const mi = MONTHS.indexOf(name.slice(0, 3));
    if (mi < 0 || typeof v !== 'string') return;
    if (year === null) {
      // The first year label at or right of this column; if the months wrap round before reaching it, the
      // label is for a later year than this column's.
      const j = labels.findIndex((x, k) => k >= i && typeof x === 'number' && x > 2000);
      if (j < 0) return;
      let wraps = 0, p = mi;
      for (let k = i + 1; k <= j; k++) {
        const q = MONTHS.indexOf(String(names[k]).trim().slice(0, 3));
        if (q < 0 || typeof names[k] !== 'string') continue;
        if (q <= p) wraps++;
        p = q;
      }
      year = labels[j] - wraps;
    } else if (mi <= prevMonth) {
      year++;
    }
    prevMonth = mi;
    const k = `${year}-${String(mi + 1).padStart(2, '0')}`;
    m[k] = i + 1;
    if (first[i] !== '') filled[k] = true;
    lastKey = k; lastCol = i + 1;
  });
  return { m, filled, lastKey, lastCol, growthCol, labels };
}

function latestFilled_(mc, before) {
  const k = Object.keys(mc.filled).filter(x => !before || x < before).sort().pop();
  return k ? mc.m[k] : null;
}

// ---------- the perf tab ----------

function perfTab_(ss) {
  const sh = ss.getSheets().find(x => x.getName().indexOf(TAB_PREFIX) === 0);
  if (!sh) throw new Error(`No tab whose name starts "${TAB_PREFIX}".`);
  return sh;
}

function norm_(s) { return String(s).replace(/\s+/g, ' ').trim().toLowerCase(); }

function plan_(ss, src, month) {
  const sh = perfTab_(ss);
  const labels = sh.getRange(1, LABEL_COL, LAST_ROW, 1).getValues().map(r => r[0]);
  ROWS.forEach(([row, , label]) => {
    if (norm_(labels[row - 1]) !== norm_(label))
      throw new Error(`${sh.getName()} row ${row} should be "${label}" but is "${labels[row - 1]}". Has a row been added or removed?`);
  });
  const mc = monthCols_(sh);
  const col = mc.m[month] || null;
  if (!col && month !== nextMonth_(mc.lastKey))
    throw new Error(`${sh.getName()} has no ${month} column, and it isn't the month after the last one (${mc.lastKey}). Fill the months in order.`);
  const styleCol = latestFilled_(mc, month);
  if (!styleCol) throw new Error(`${sh.getName()}: no earlier filled month to copy formatting and formulas from.`);
  return { sh, src, month, col, isNew: !col, mc, styleCol };
}

// Adds the month's column after the last month (before Growth): formatting from the latest filled month,
// days, month name, and in a January the new year label.
function addMonth_(p) {
  const { sh, mc, styleCol, month } = p;
  const last = mc.lastCol;
  const col = last + 1;
  sh.insertColumnAfter(last);
  sh.setColumnWidth(col, sh.getColumnWidth(styleCol));
  const height = LAST_ROW - DAYS_ROW + 1;
  sh.getRange(DAYS_ROW, styleCol, height, 1)
    .copyTo(sh.getRange(DAYS_ROW, col, height, 1), SpreadsheetApp.CopyPasteType.PASTE_FORMAT, false);
  const [y, m] = month.split('-').map(Number);
  sh.getRange(DAYS_ROW, col).setValue(new Date(y, m, 0).getDate());
  sh.getRange(MONTH_ROW, col).setValue(MONTHS[m - 1]);

  // Year label: the year is merged across its months (and Growth, after the last one). Inserting inside
  // that block may already have widened it; otherwise widen it here. In a January, split it: the old
  // year keeps its months, the new year starts at this column (and keeps Growth if it covered it).
  const cell = sh.getRange(YEAR_ROW, col);
  const lastCell = sh.getRange(YEAR_ROW, last);
  const block = cell.isPartOfMerge() ? cell.getMergedRanges()[0] : lastCell.isPartOfMerge() ? lastCell.getMergedRanges()[0] : null;
  const start = block ? block.getColumn() : last, end = block ? block.getLastColumn() : last;
  if (m !== 1) {
    if (!cell.isPartOfMerge() && block) {
      block.breakApart();
      sh.getRange(YEAR_ROW, start, 1, Math.max(col, end) - start + 1).merge();
    }
  } else {
    if (block) {
      block.breakApart();
      if (last > start) sh.getRange(YEAR_ROW, start, 1, last - start + 1).merge();
      if (end > col) sh.getRange(YEAR_ROW, col, 1, end - col + 1).merge();
    }
    // A label like the last year's, set field by field (copying from a merged cell can be refused).
    const at = mc.labels.lastIndexOf(y - 1);
    const prev = at >= 0 ? sh.getRange(YEAR_ROW, at + 1) : null;
    cell.setValue(y);
    if (prev) cell.setFontFamily(prev.getFontFamily()).setFontSize(prev.getFontSize())
      .setFontWeight(prev.getFontWeight()).setFontStyle(prev.getFontStyle()).setFontColor(prev.getFontColor())
      .setBackground(prev.getBackground()).setNumberFormat(prev.getNumberFormat())
      .setHorizontalAlignment(prev.getHorizontalAlignment()).setVerticalAlignment(prev.getVerticalAlignment());
  }
  return col;
}

// Writes the month: mapped rows from the file (left as they are where the file is empty), formula rows from
// the previous month, formatting from the latest filled month. Returns notes for the message.
function fillMonth_(p, col) {
  const { sh, src, styleCol, isNew, mc } = p;
  const height = LAST_ROW - FIRST_ROW + 1;
  if (!isNew && col !== styleCol)
    sh.getRange(FIRST_ROW, styleCol, height, 1)
      .copyTo(sh.getRange(FIRST_ROW, col, height, 1), SpreadsheetApp.CopyPasteType.PASTE_FORMAT, false);

  const metricAt = {};
  ROWS.forEach(([row, metric]) => (metricAt[row] = metric));
  const history = [];
  for (let c = col - 1; c >= 1 && history.length < FORMULA_LOOKBACK; c--)
    history.push(sh.getRange(FIRST_ROW, c, height, 1).getFormulasR1C1().map(r => r[0]));

  const missing = [];
  let run = null;  // consecutive value rows, written together
  const flush = () => {
    if (!run) return;
    sh.getRange(run.start, col, run.values.length, 1).setValues(run.values.map(v => [v]));
    run = null;
  };
  for (let row = FIRST_ROW; row <= LAST_ROW; row++) {
    const metric = metricAt[row];
    if (metric) {
      const v = src[metric];
      if (v === undefined) missing.push(metric);
      if (v !== undefined && v !== '' && v !== null) {
        if (!run) run = { start: row, values: [] };
        run.values.push(v);
        continue;
      }
      flush();
      continue;
    }
    flush();
    const f = history.map(h => h[row - FIRST_ROW]).find(x => x);
    if (f) sh.getRange(row, col).setFormulaR1C1(f);
  }
  flush();
  // Midnight on the 1st in the sheet's own time zone, like the dates typed in before (a Date built in the
  // script's time zone can land on the previous day).
  const tz = SpreadsheetApp.getActive().getSpreadsheetTimeZone() || Session.getScriptTimeZone();
  sh.getRange(MONTH_DATE_ROW, col).setValue(Utilities.parseDate(`${p.month}-01`, tz, 'yyyy-MM-dd'));
  // Number formats: each row takes the format most of the recent filled months use, so one mis-formatted
  // cell in the latest month isn't carried forward.
  const recent = Object.keys(mc.filled).filter(k => k < p.month).sort().reverse()
    .slice(0, FORMAT_MONTHS).map(k => mc.m[k]);  // all left of `col`, so an added column doesn't shift them
  const fmts = recent.map(c => sh.getRange(FIRST_ROW, c, height, 1).getNumberFormats().map(r => r[0]));
  sh.getRange(FIRST_ROW, col, height, 1).setNumberFormats(fmts[0].map((_, i) => [mostCommon_(fmts.map(f => f[i]))]));
  return missing.length ? [`not in the file: ${missing.join(', ')}`] : [];
}

// The most frequent value; on a tie, the one that comes first (the most recent month).
function mostCommon_(list) {
  const n = {};
  list.forEach(x => (n[x] = (n[x] || 0) + 1));
  return list.reduce((best, x) => (n[x] > n[best] ? x : best), list[0]);
}

// Points the Growth column at the latest filled month vs the month before. `before` is the Growth formulas
// as they were before a column was deleted (their references are #REF! after).
function updateGrowth_(sh, before) {
  const mc = monthCols_(sh);
  if (!mc.growthCol) return;
  const latest = latestFilled_(mc, null);
  if (!latest) return;
  const to = [colLetter_(latest), colLetter_(latest - 1)];
  const range = sh.getRange(FIRST_ROW, mc.growthCol, LAST_ROW - FIRST_ROW + 1, 1);
  const formulas = before || range.getFormulas().map(r => r[0]);
  const ref = /(\$?)([A-Z]{1,3})(\$?\d+)(?![\d(])/g;
  formulas.forEach((f, i) => {
    if (!f) return;
    const letters = [...new Set([...f.matchAll(ref)].map(x => x[2]))];
    if (letters.length !== 2) return;
    const [hi, lo] = letters.sort((a, b) => colIndex_(b) - colIndex_(a));
    const map = { [hi]: to[0], [lo]: to[1] };
    const nf = f.replace(ref, (all, a, c, d) => (c in map ? a + map[c] + d : all));
    if (before || nf !== f) sh.getRange(FIRST_ROW + i, mc.growthCol).setFormula(nf);
  });
}

// ---------- undo ----------

// Takes a month back out. If it is the last month column it is deleted (Growth then points at the month
// before); otherwise its mapped rows are cleared and the formulas kept.
function removeMonth() {
  const ui = SpreadsheetApp.getUi();
  const month = askMonth_('Remove a month', 'Month, e.g. 2026-09.');
  if (!month) return;
  const sh = perfTab_(SpreadsheetApp.getActive());
  const mc = monthCols_(sh);
  const col = mc.m[month];
  if (!col || !mc.filled[month]) { ui.alert(`Nothing to remove: ${sh.getName()} has no numbers for ${month}.`); return; }
  const last = col === mc.lastCol;
  const ok = ui.alert(`Remove ${month}?`,
    last ? `Deletes the ${month} column. Typed-in rows such as downloads go with it.`
         : `Clears ${month}'s numbers (formulas stay). Typed-in rows such as downloads go with it.`,
    ui.ButtonSet.YES_NO);
  if (ok !== ui.Button.YES) return;
  const height = LAST_ROW - FIRST_ROW + 1;
  if (last) {
    const before = mc.growthCol ? sh.getRange(FIRST_ROW, mc.growthCol, height, 1).getFormulas().map(r => r[0]) : null;
    sh.deleteColumn(col);
    updateGrowth_(sh, before);
  } else {
    const mapped = {};
    ROWS.forEach(([row]) => (mapped[row] = true));
    const range = sh.getRange(FIRST_ROW, col, height, 1);
    const values = range.getValues(), formulas = range.getFormulas();
    range.setValues(values.map((v, i) => [mapped[FIRST_ROW + i] ? '' : (formulas[i][0] || v[0])]));
    updateGrowth_(sh, null);
  }
  ui.alert(`Removed ${month}.`);
}
