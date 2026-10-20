"""Tags a weekly output file with the report it is and the week it covers.

The perf sheets' Apps Scripts (appscript/<cc>_weekly.gs) read these tags from an imported file before they
touch the sheet: a file whose report doesn't match the sheet is refused, and the week says which column
to fill. Each tag is a label cell with its value to its right, in rows 1-2, one empty column clear of the
data, so the table itself is unchanged for anyone reading the file.
"""
import csv

REPORT_KEY = "report"
WEEK_KEY = "week_start"


def tag_worksheet(ws, col, report, week_start):
  """Tag an xlsxwriter worksheet at rows 1-2, starting at 0-based column `col`."""
  ws.write_string(0, col, REPORT_KEY)
  ws.write_string(0, col + 1, report)
  ws.write_string(1, col, WEEK_KEY)
  ws.write_string(1, col + 1, week_start)


def tag_csv(path, report, week_start):
  """Tag a CSV written by DataFrame.to_csv: rows 1-2 get the tags, one empty column after the data."""
  with open(path, newline="", encoding="utf-8") as f:
    rows = list(csv.reader(f))
  width = max(len(r) for r in rows)
  for row, (key, value) in zip(rows, ((REPORT_KEY, report), (WEEK_KEY, week_start))):
    row.extend([""] * (width - len(row)) + ["", key, value])
  with open(path, "w", newline="", encoding="utf-8") as f:
    csv.writer(f).writerows(rows)
