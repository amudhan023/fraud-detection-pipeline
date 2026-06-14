"use client";

import { format } from "date-fns";
import type { FraudEvent } from "@/lib/api";
import { AnomalyBadge, ClassificationBadge, ScoreBadge } from "./RiskBadge";

interface Props {
  events: FraudEvent[];
  selectedId: string | null;
  onSelect: (event: FraudEvent) => void;
}

export function FraudEventsTable({ events, selectedId, onSelect }: Props) {
  return (
    <div
      className="rounded-xl border overflow-hidden"
      style={{ borderColor: "var(--border)" }}
    >
      <div
        className="overflow-x-auto"
        style={{ background: "var(--surface)" }}
      >
        <table className="w-full text-sm">
          <thead>
            <tr
              className="text-xs uppercase tracking-wider border-b"
              style={{
                background: "var(--surface-2)",
                borderColor: "var(--border)",
                color: "var(--text-muted)",
              }}
            >
              <th className="text-left px-4 py-3">Detected</th>
              <th className="text-left px-4 py-3">Anomaly</th>
              <th className="text-left px-4 py-3">Account</th>
              <th className="text-left px-4 py-3">Merchant</th>
              <th className="text-right px-4 py-3">Amount</th>
              <th className="text-center px-4 py-3">Rule Score</th>
              <th className="text-center px-4 py-3">AI Score</th>
              <th className="text-left px-4 py-3">Classification</th>
              <th className="text-center px-4 py-3">Action Taken</th>
            </tr>
          </thead>
          <tbody>
            {events.length === 0 && (
              <tr>
                <td
                  colSpan={9}
                  className="text-center py-12"
                  style={{ color: "var(--text-muted)" }}
                >
                  No fraud events detected yet.
                </td>
              </tr>
            )}
            {events.map((ev) => {
              const isSelected = ev.id === selectedId;
              return (
                <tr
                  key={ev.id}
                  onClick={() => onSelect(ev)}
                  className="border-b cursor-pointer transition-colors"
                  style={{
                    borderColor: "var(--border)",
                    background: isSelected ? "var(--surface-2)" : undefined,
                  }}
                  onMouseEnter={(e) => {
                    if (!isSelected)
                      (e.currentTarget as HTMLElement).style.background =
                        "rgba(255,255,255,0.03)";
                  }}
                  onMouseLeave={(e) => {
                    if (!isSelected)
                      (e.currentTarget as HTMLElement).style.background = "";
                  }}
                >
                  <td className="px-4 py-3 font-mono text-xs whitespace-nowrap" style={{ color: "var(--text-muted)" }}>
                    {format(new Date(ev.detected_at), "MMM d HH:mm:ss")}
                  </td>
                  <td className="px-4 py-3">
                    <AnomalyBadge code={ev.reason_code} />
                  </td>
                  <td className="px-4 py-3">
                    <div className="font-medium">{ev.owner_name ?? "—"}</div>
                    <div className="text-xs" style={{ color: "var(--text-muted)" }}>
                      {ev.tier} · {ev.account_country}
                    </div>
                  </td>
                  <td className="px-4 py-3">
                    <div className="font-medium">{ev.merchant_name ?? "—"}</div>
                    <div className="text-xs" style={{ color: "var(--text-muted)" }}>
                      {ev.merchant_category} · {ev.merchant_country}
                    </div>
                  </td>
                  <td className="px-4 py-3 text-right font-mono">
                    {ev.amount != null
                      ? `$${ev.amount.toLocaleString("en-US", { minimumFractionDigits: 2 })}`
                      : "—"}
                  </td>
                  <td className="px-4 py-3 text-center">
                    <ScoreBadge
                      score={
                        ev.rule_risk_score != null
                          ? Math.round(ev.rule_risk_score * 100)
                          : null
                      }
                    />
                  </td>
                  <td className="px-4 py-3 text-center">
                    <ScoreBadge score={ev.ai_analysis?.ai_risk_score ?? null} />
                  </td>
                  <td className="px-4 py-3">
                    <ClassificationBadge
                      classification={ev.ai_analysis?.classification ?? null}
                    />
                  </td>
                  <td className="px-4 py-3 text-center">
                    {ev.last_action ? (
                      <ActionChip action={ev.last_action} />
                    ) : (
                      <span className="text-xs" style={{ color: "var(--text-muted)" }}>
                        None
                      </span>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function ActionChip({ action }: { action: string }) {
  const colors: Record<string, string> = {
    APPROVE: "text-green-400",
    DENY: "text-red-400",
    ESCALATE: "text-yellow-400",
    BLOCK_ACCOUNT: "text-red-500 font-semibold",
  };
  return (
    <span className={`text-xs font-mono ${colors[action] ?? "text-slate-400"}`}>
      {action}
    </span>
  );
}
