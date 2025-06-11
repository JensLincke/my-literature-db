#!/usr/bin/env mongosh

const pad = (str, len) => (str || "").toString().padEnd(len, " ");
const ops = db.currentOp({active: true}).inprog;

const headers = ["OPID", "TIME", "OP", "NAMESPACE", "COMMAND", "PLAN", "CLIENT"];
const widths = [8, 6, 10, 30, 12, 10, 20];

print(headers.map((h, i) => pad(h, widths[i])).join(" | "));
print("-".repeat(widths.reduce((a, b) => a + b + 3, -3)));

ops.forEach(op => {
  const row = [
    op.opid,
    (op.secs_running || 0) + "s",
    op.op || "none",
    op.ns || "",
    op.command?.find || op.command?.aggregate || "",
    op.planSummary || "",
    op.client || ""
  ];
  print(row.map((cell, i) => pad(cell, widths[i])).join(" | "));
});
